#!/usr/bin/env python3
import argparse
import logging
import os
import subprocess
import tempfile
from pathlib import Path

from smush_rewrite import dedup_file_sort_u, rewrite_nt


def load_mapping(mapping_path: Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    with mapping_path.open("r", encoding="utf-8", errors="replace") as fin:
        for line in fin:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            old_uri, canonical_uri = parts[0], parts[1]
            if old_uri and canonical_uri:
                mapping[old_uri] = canonical_uri
    return mapping


def count_lines(path: Path) -> int:
    with path.open("r", encoding="utf-8", errors="replace") as fin:
        return sum(1 for _ in fin)


def build_rewritten_test(
    source_test: Path,
    mapping: dict[str, str],
    output_path: Path,
    parallel: int,
    sort_mem: str,
) -> None:
    tmp_unsorted = output_path.with_suffix(output_path.suffix + ".tmp")
    rewrite_nt(str(source_test), str(tmp_unsorted), mapping)
    dedup_file_sort_u(str(tmp_unsorted), str(output_path), parallel=parallel, mem=sort_mem)
    tmp_unsorted.unlink()


def build_train_split(
    merged_dataset: Path,
    rewritten_test: Path,
    output_path: Path,
) -> None:
    env = dict(os.environ)
    env["LC_ALL"] = "C"
    with output_path.open("w", encoding="utf-8") as fout:
        subprocess.run(
            ["comm", "-23", str(merged_dataset), str(rewritten_test)],
            stdout=fout,
            check=True,
            env=env,
        )


def hardlink_or_copy(src: Path, dst: Path) -> None:
    if dst.exists():
        dst.unlink()
    try:
        os.link(src, dst)
    except OSError:
        subprocess.run(["cp", str(src), str(dst)], check=True)


def iter_merged_datasets(merged_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in merged_dir.glob("*_smushed_merged.nt")
        if path.is_file()
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Create per-dataset train/test split directories for smushed merged KGs. "
            "test.txt is the rewritten held-out set; train.txt is merged KG minus test.txt."
        )
    )
    parser.add_argument("--mapping", required=True, help="old_uri<tab>canonical_uri mapping TSV")
    parser.add_argument("--test", required=True, help="Original held-out test triples file")
    parser.add_argument("--merged-dir", required=True, help="Directory containing *_smushed_merged.nt files")
    parser.add_argument("--out-root", required=True, help="Directory where per-KG split directories will be created")
    parser.add_argument("--shared-test-out", default=None, help="Optional path for the rewritten shared test file")
    parser.add_argument("--parallel", type=int, default=8, help="Parallelism passed to sort")
    parser.add_argument("--sort-mem", default="50%", help="Memory budget passed to sort -S")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity",
    )
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s [%(levelname)s] %(message)s")

    mapping_path = Path(args.mapping)
    source_test = Path(args.test)
    merged_dir = Path(args.merged_dir)
    out_root = Path(args.out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    merged_datasets = iter_merged_datasets(merged_dir)
    if not merged_datasets:
        raise FileNotFoundError(f"No *_smushed_merged.nt files found in {merged_dir}")

    shared_test_out = Path(args.shared_test_out) if args.shared_test_out else out_root / "shared_test_smushed.txt"

    mapping = load_mapping(mapping_path)
    logging.info("Loaded %s remapped URIs from %s", len(mapping), mapping_path)

    with tempfile.TemporaryDirectory(prefix="package_smushed_splits_", dir=str(out_root)) as tmpdir_name:
        tmpdir = Path(tmpdir_name)
        rewritten_test = tmpdir / "test.rewritten.sorted.txt"
        build_rewritten_test(
            source_test=source_test,
            mapping=mapping,
            output_path=rewritten_test,
            parallel=args.parallel,
            sort_mem=args.sort_mem,
        )
        hardlink_or_copy(rewritten_test, shared_test_out)
        test_line_count = count_lines(shared_test_out)
        logging.info("Prepared rewritten test split with %s triples at %s", test_line_count, shared_test_out)

        for merged_dataset in merged_datasets:
            package_dir = out_root / merged_dataset.stem
            package_dir.mkdir(parents=True, exist_ok=True)

            test_out = package_dir / "test.txt"
            train_out = package_dir / "train.txt"

            hardlink_or_copy(shared_test_out, test_out)
            build_train_split(
                merged_dataset=merged_dataset,
                rewritten_test=shared_test_out,
                output_path=train_out,
            )

            logging.info(
                "Packaged %s -> %s (test=%s triples)",
                merged_dataset.name,
                package_dir,
                test_line_count,
            )

    logging.info("Finished packaging %s merged datasets", len(merged_datasets))


if __name__ == "__main__":
    main()
