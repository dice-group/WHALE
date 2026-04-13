import argparse
import logging
import os
import subprocess
import tempfile
from typing import Iterable

IN_PROTEIN = "<http://whale.data.dice-research.org/ontology/inProtein>"
IN_CELL_LINE = "<http://whale.data.dice-research.org/ontology/inCellLine>"
HAS_PROTEIN = "<http://whale.data.dice-research.org/ontology/hasProtein>"
PROTEIN_OBSERVATION_MARKER = "resource#protein_expr_"


def iter_nt_files(input_dir: str) -> list[str]:
    files: list[str] = []
    for entry in os.scandir(input_dir):
        if entry.is_file() and entry.name.endswith(".nt"):
            files.append(entry.path)
    files.sort()
    return files


def sort_unique(path_in: str, path_out: str, parallel: int = 8, mem: str = "50%") -> None:
    env = dict(os.environ)
    env["LC_ALL"] = "C"
    cmd = ["sort", "-S", mem, f"--parallel={parallel}", "-u", path_in]
    with open(path_out, "w", encoding="utf-8") as fout:
        subprocess.run(cmd, stdout=fout, check=True, env=env)


def extract_relevant_facts(input_files: Iterable[str], facts_path: str, existing_path: str) -> dict[str, int]:
    counts = {
        "files": 0,
        "in_protein": 0,
        "in_cell_line": 0,
        "existing_has_protein": 0,
    }

    with open(facts_path, "w", encoding="utf-8") as facts_out, open(existing_path, "w", encoding="utf-8") as existing_out:
        for input_file in input_files:
            counts["files"] += 1
            logging.info("Scanning %s", input_file)
            with open(input_file, "r", encoding="utf-8", errors="replace") as fin:
                for line in fin:
                    if "ontology/hasProtein" in line:
                        parts = line.split()
                        if len(parts) >= 4 and parts[1] == HAS_PROTEIN and parts[0].startswith("<") and parts[2].startswith("<"):
                            existing_out.write(f"{parts[0]} {HAS_PROTEIN} {parts[2]} .\n")
                            counts["existing_has_protein"] += 1
                        continue

                    if PROTEIN_OBSERVATION_MARKER not in line:
                        continue

                    if "ontology/inProtein" in line:
                        parts = line.split()
                        if len(parts) >= 4 and parts[1] == IN_PROTEIN and parts[0].startswith("<") and parts[2].startswith("<"):
                            facts_out.write(f"{parts[0]}\tP\t{parts[2]}\n")
                            counts["in_protein"] += 1
                    elif "ontology/inCellLine" in line:
                        parts = line.split()
                        if len(parts) >= 4 and parts[1] == IN_CELL_LINE and parts[0].startswith("<") and parts[2].startswith("<"):
                            facts_out.write(f"{parts[0]}\tC\t{parts[2]}\n")
                            counts["in_cell_line"] += 1

    return counts


def flush_subject(cell_lines: set[str], proteins: set[str], out_stream) -> tuple[int, int]:
    if not cell_lines or not proteins:
        return 0, 0

    triples = 0
    for cell_line in sorted(cell_lines):
        for protein in sorted(proteins):
            out_stream.write(f"{cell_line} {HAS_PROTEIN} {protein} .\n")
            triples += 1

    return 1, triples


def build_candidate_triples(facts_sorted_path: str, candidates_path: str) -> tuple[int, int]:
    matched_subjects = 0
    triple_count = 0
    current_subject = None
    cell_lines: set[str] = set()
    proteins: set[str] = set()

    with open(facts_sorted_path, "r", encoding="utf-8") as fin, open(candidates_path, "w", encoding="utf-8") as fout:
        for line in fin:
            subject, kind, obj = line.rstrip("\n").split("\t")
            if subject != current_subject:
                if current_subject is not None:
                    subject_count, derived_count = flush_subject(cell_lines, proteins, fout)
                    matched_subjects += subject_count
                    triple_count += derived_count
                current_subject = subject
                cell_lines = set()
                proteins = set()

            if kind == "C":
                cell_lines.add(obj)
            elif kind == "P":
                proteins.add(obj)

        if current_subject is not None:
            subject_count, derived_count = flush_subject(cell_lines, proteins, fout)
            matched_subjects += subject_count
            triple_count += derived_count

    return matched_subjects, triple_count


def subtract_sorted_lines(candidates_path: str, existing_path: str, output_path: str) -> tuple[int, int]:
    def next_line(handle):
        return handle.readline()

    written = 0
    skipped_existing = 0

    with open(candidates_path, "r", encoding="utf-8") as cand, open(existing_path, "r", encoding="utf-8") as existing, open(output_path, "w", encoding="utf-8") as fout:
        cand_line = next_line(cand)
        existing_line = next_line(existing)

        while cand_line:
            if not existing_line or cand_line < existing_line:
                fout.write(cand_line)
                written += 1
                cand_line = next_line(cand)
            elif cand_line == existing_line:
                skipped_existing += 1
                cand_line = next_line(cand)
                existing_line = next_line(existing)
            else:
                existing_line = next_line(existing)

    return written, skipped_existing


def derive_cell_line_has_protein(input_dir: str, output_file: str, parallel: int = 8, sort_mem: str = "50%") -> None:
    input_files = iter_nt_files(input_dir)
    if not input_files:
        raise FileNotFoundError(f"No .nt files found in {input_dir}")

    output_dir = os.path.dirname(os.path.abspath(output_file)) or os.getcwd()
    os.makedirs(output_dir, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="has_protein_", dir=output_dir) as tmpdir:
        facts_path = os.path.join(tmpdir, "facts.tsv")
        facts_sorted_path = os.path.join(tmpdir, "facts.sorted.tsv")
        existing_path = os.path.join(tmpdir, "existing.nt")
        existing_sorted_path = os.path.join(tmpdir, "existing.sorted.nt")
        candidates_path = os.path.join(tmpdir, "candidates.nt")
        candidates_sorted_path = os.path.join(tmpdir, "candidates.sorted.nt")

        counts = extract_relevant_facts(input_files, facts_path, existing_path)
        logging.info(
            "Found %s inProtein triples, %s inCellLine triples, %s existing hasProtein triples across %s files",
            counts["in_protein"],
            counts["in_cell_line"],
            counts["existing_has_protein"],
            counts["files"],
        )

        logging.info("Sorting extracted facts")
        sort_unique(facts_path, facts_sorted_path, parallel=parallel, mem=sort_mem)

        logging.info("Building derived hasProtein candidates")
        matched_subjects, candidate_count = build_candidate_triples(facts_sorted_path, candidates_path)
        logging.info("Built %s candidate triples from %s matching subjects", candidate_count, matched_subjects)

        logging.info("Deduplicating candidate triples")
        sort_unique(candidates_path, candidates_sorted_path, parallel=parallel, mem=sort_mem)

        logging.info("Deduplicating existing hasProtein triples")
        sort_unique(existing_path, existing_sorted_path, parallel=parallel, mem=sort_mem)

        logging.info("Removing triples that already exist")
        written, skipped_existing = subtract_sorted_lines(candidates_sorted_path, existing_sorted_path, output_file)

    logging.info("Wrote %s new hasProtein triples to %s", written, output_file)
    if skipped_existing:
        logging.info("Skipped %s triples that already existed", skipped_existing)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Derive <cell_line> <hasProtein> <protein> triples from observation subjects that contain both inCellLine and inProtein across a directory of N-Triples files."
    )
    parser.add_argument("--input-dir", required=True, help="Directory containing .nt files to scan.")
    parser.add_argument(
        "--output",
        default=None,
        help="Output .nt file for new derived triples only. Defaults to <input-dir>/cell_line_hasProtein_derived.nt",
    )
    parser.add_argument("--parallel", type=int, default=8, help="Parallelism passed to sort.")
    parser.add_argument("--sort-mem", default="50%", help="Memory budget passed to sort -S.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s [%(levelname)s] %(message)s")

    output_file = args.output or os.path.join(args.input_dir, "cell_line_hasProtein_derived.nt")
    derive_cell_line_has_protein(args.input_dir, output_file, parallel=args.parallel, sort_mem=args.sort_mem)


if __name__ == "__main__":
    main()
