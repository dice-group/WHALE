import argparse
import logging
import os
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from smush_rewrite import build_mapping, dedup_file_sort_u, rewrite_nt


def iter_source_datasets(cell_line_dir: Path) -> list[Path]:
    datasets: list[Path] = []
    for path in sorted(cell_line_dir.glob("*.nt")):
        if path.name.endswith("_derived.nt"):
            continue
        datasets.append(path)
    return datasets


def find_derived_datasets(source_dataset: Path) -> list[Path]:
    pattern = f"{source_dataset.stem}__*_derived.nt"
    return sorted(source_dataset.parent.glob(pattern))


def write_mapping(mapping: dict[str, str], output_path: Path) -> None:
    with output_path.open("w", encoding="utf-8") as fout:
        for old_uri, canonical_uri in sorted(mapping.items()):
            if old_uri == canonical_uri:
                continue
            fout.write(f"{old_uri}\t{canonical_uri}\n")


def rewrite_and_dedup(input_path: Path, output_path: Path, mapping: dict[str, str], parallel: int, sort_mem: str) -> None:
    rewrite_nt(str(input_path), str(output_path), mapping)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    os.replace(output_path, tmp_path)
    dedup_file_sort_u(str(tmp_path), str(output_path), parallel=parallel, mem=sort_mem)
    tmp_path.unlink()


def concatenate_files(input_paths: list[Path], output_path: Path) -> None:
    with output_path.open("w", encoding="utf-8") as fout:
        for input_path in input_paths:
            with input_path.open("r", encoding="utf-8", errors="replace") as fin:
                for line in fin:
                    fout.write(line)


def build_output_name(source_dataset: Path, curated_dataset: Path) -> str:
    return f"{source_dataset.stem}__with_{curated_dataset.stem}_smushed_merged.nt"


def merge_dataset(
    source_dataset: Path,
    curated_dataset: Path,
    output_dir: Path,
    mapping: dict[str, str],
    parallel: int,
    sort_mem: str,
) -> tuple[Path, int, list[Path]]:
    derived_datasets = find_derived_datasets(source_dataset)
    merged_output = output_dir / build_output_name(source_dataset, curated_dataset)

    with tempfile.TemporaryDirectory(prefix=f"{source_dataset.stem}_smush_", dir=str(output_dir)) as tmpdir_name:
        tmpdir = Path(tmpdir_name)
        rewritten_inputs: list[Path] = []

        curated_rewritten = tmpdir / f"{curated_dataset.stem}.rewritten.nt"
        rewrite_and_dedup(curated_dataset, curated_rewritten, mapping, parallel=parallel, sort_mem=sort_mem)
        rewritten_inputs.append(curated_rewritten)

        source_rewritten = tmpdir / f"{source_dataset.stem}.rewritten.nt"
        rewrite_and_dedup(source_dataset, source_rewritten, mapping, parallel=parallel, sort_mem=sort_mem)
        rewritten_inputs.append(source_rewritten)

        for derived_dataset in derived_datasets:
            derived_rewritten = tmpdir / f"{derived_dataset.stem}.rewritten.nt"
            rewrite_and_dedup(derived_dataset, derived_rewritten, mapping, parallel=parallel, sort_mem=sort_mem)
            rewritten_inputs.append(derived_rewritten)

        concatenated = tmpdir / "merged.concat.nt"
        concatenate_files(rewritten_inputs, concatenated)
        dedup_file_sort_u(str(concatenated), str(merged_output), parallel=parallel, mem=sort_mem)

    return merged_output, len(rewritten_inputs), derived_datasets


def batch_merge(
    links_path: Path,
    curated_dataset: Path,
    cell_line_dir: Path,
    output_dir: Path,
    conf: float,
    parallel: int,
    sort_mem: str,
    mapping_out: Path | None,
) -> list[tuple[Path, int, list[Path]]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    mapping = build_mapping(str(links_path), conf)
    if mapping_out is not None:
        write_mapping(mapping, mapping_out)

    results: list[tuple[Path, int, list[Path]]] = []
    for source_dataset in iter_source_datasets(cell_line_dir):
        logging.info("Merging %s", source_dataset.name)
        merged_output, merged_inputs, derived_datasets = merge_dataset(
            source_dataset,
            curated_dataset,
            output_dir,
            mapping,
            parallel=parallel,
            sort_mem=sort_mem,
        )
        if derived_datasets:
            logging.info(
                "Wrote %s from %s rewritten inputs after appending %s derived file(s)",
                merged_output,
                merged_inputs,
                len(derived_datasets),
            )
        else:
            logging.info("Wrote %s from %s rewritten inputs", merged_output, merged_inputs)
        results.append((merged_output, merged_inputs, derived_datasets))

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "For each Cell_line_genomic_data_nt source dataset, rewrite it together with a curated dataset using sameAs links, "
            "append any matching derived datasets, and write a deduplicated merged .nt file."
        )
    )
    parser.add_argument("--links", required=True, help="TSV links file: <u1>\\t<u2>\\tconfidence")
    parser.add_argument("--curated", required=True, help="Curated .nt dataset to merge with each source dataset")
    parser.add_argument("--cell-line-dir", required=True, help="Directory containing Cell_line_genomic_data_nt .nt files")
    parser.add_argument("--out-dir", required=True, help="Directory for merged outputs")
    parser.add_argument("--conf", type=float, default=0.7, help="Confidence threshold")
    parser.add_argument("--parallel", type=int, default=8, help="Parallelism passed to sort")
    parser.add_argument("--sort-mem", default="50%", help="Memory budget passed to sort -S")
    parser.add_argument("--mapping-out", default=None, help="Optional old_uri<tab>canonical_uri mapping file")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity",
    )
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s [%(levelname)s] %(message)s")

    results = batch_merge(
        links_path=Path(args.links),
        curated_dataset=Path(args.curated),
        cell_line_dir=Path(args.cell_line_dir),
        output_dir=Path(args.out_dir),
        conf=args.conf,
        parallel=args.parallel,
        sort_mem=args.sort_mem,
        mapping_out=Path(args.mapping_out) if args.mapping_out else None,
    )
    logging.info("Finished %s merged dataset files", len(results))


if __name__ == "__main__":
    main()
