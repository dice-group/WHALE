import argparse
import logging
import os
import subprocess
import tempfile
from collections import defaultdict

ONTOLOGY_PREFIX = "<http://whale.data.dice-research.org/ontology/"
IN_CELL_LINE = "inCellLine"


def iter_nt_files(input_dir: str) -> list[str]:
    files: list[str] = []
    for entry in os.scandir(input_dir):
        if not entry.is_file() or not entry.name.endswith(".nt"):
            continue
        if entry.name.endswith("_derived.nt"):
            continue
        files.append(entry.path)
    files.sort()
    return files


def sort_unique(path_in: str, path_out: str, parallel: int = 8, mem: str = "50%") -> None:
    env = dict(os.environ)
    env["LC_ALL"] = "C"
    cmd = ["sort", "-S", mem, f"--parallel={parallel}", "-u", path_in]
    with open(path_out, "w", encoding="utf-8") as fout:
        subprocess.run(cmd, stdout=fout, check=True, env=env)


def parse_in_triple(line: str) -> tuple[str, str, str] | None:
    parts = line.split()
    if len(parts) != 4 or parts[3] != ".":
        return None

    subject, predicate, obj, _ = parts
    if not (subject.startswith("<") and predicate.startswith(ONTOLOGY_PREFIX) and predicate.endswith(">") and obj.startswith("<")):
        return None

    predicate_local = predicate[len(ONTOLOGY_PREFIX) : -1]
    if not predicate_local.startswith("in"):
        return None

    return subject, predicate_local, obj


def extract_relevant_facts(input_file: str, facts_path: str) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)

    with open(facts_path, "w", encoding="utf-8") as facts_out, open(input_file, "r", encoding="utf-8", errors="replace") as fin:
        for line in fin:
            parsed = parse_in_triple(line)
            if parsed is None:
                continue

            subject, predicate_local, obj = parsed
            facts_out.write(f"{subject}\t{predicate_local}\t{obj}\n")
            counts[predicate_local] += 1

    return dict(counts)


def flush_subject(
    cell_lines: set[str],
    related: dict[str, set[str]],
    candidate_handles: dict[str, object],
    stats: dict[str, dict[str, int]],
) -> None:
    if not cell_lines or not related:
        return

    for in_predicate, objects in sorted(related.items()):
        if not objects:
            continue

        has_predicate = f"has{in_predicate[2:]}"
        handle = candidate_handles.get(has_predicate)
        if handle is None:
            handle = open(stats[has_predicate]["candidate_path"], "w", encoding="utf-8")
            candidate_handles[has_predicate] = handle
        triples_written = 0

        predicate_uri = f"{ONTOLOGY_PREFIX}{has_predicate}>"
        for cell_line in sorted(cell_lines):
            for obj in sorted(objects):
                handle.write(f"{cell_line} {predicate_uri} {obj} .\n")
                triples_written += 1

        stats[has_predicate]["matched_subjects"] += 1
        stats[has_predicate]["candidate_triples"] += triples_written


def build_candidate_triples(facts_sorted_path: str, candidates_dir: str) -> dict[str, dict[str, int]]:
    stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "candidate_path": "",
            "matched_subjects": 0,
            "candidate_triples": 0,
        }
    )
    candidate_handles: dict[str, object] = {}
    current_subject = None
    cell_lines: set[str] = set()
    related: dict[str, set[str]] = defaultdict(set)

    try:
        with open(facts_sorted_path, "r", encoding="utf-8") as fin:
            for line in fin:
                subject, predicate_local, obj = line.rstrip("\n").split("\t")
                if subject != current_subject:
                    if current_subject is not None:
                        flush_subject(cell_lines, related, candidate_handles, stats)
                    current_subject = subject
                    cell_lines = set()
                    related = defaultdict(set)

                if predicate_local == IN_CELL_LINE:
                    cell_lines.add(obj)
                else:
                    has_predicate = f"has{predicate_local[2:]}"
                    if not stats[has_predicate]["candidate_path"]:
                        stats[has_predicate]["candidate_path"] = os.path.join(candidates_dir, f"{has_predicate}.nt")
                    related[predicate_local].add(obj)

            if current_subject is not None:
                flush_subject(cell_lines, related, candidate_handles, stats)
    finally:
        for handle in candidate_handles.values():
            handle.close()

    return dict(stats)


def derive_from_file(input_file: str, output_dir: str, parallel: int = 8, sort_mem: str = "50%") -> list[tuple[str, int]]:
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    results: list[tuple[str, int]] = []

    with tempfile.TemporaryDirectory(prefix=f"{base_name}_derive_", dir=output_dir) as tmpdir:
        facts_path = os.path.join(tmpdir, "facts.tsv")
        facts_sorted_path = os.path.join(tmpdir, "facts.sorted.tsv")
        candidates_dir = os.path.join(tmpdir, "candidates")
        os.makedirs(candidates_dir, exist_ok=True)

        counts = extract_relevant_facts(input_file, facts_path)
        if not counts:
            logging.info("Skipping %s: no in* object triples found", input_file)
            return results

        logging.info("Scanned %s: %s", input_file, ", ".join(f"{pred}={count}" for pred, count in sorted(counts.items())))

        sort_unique(facts_path, facts_sorted_path, parallel=parallel, mem=sort_mem)
        candidate_stats = build_candidate_triples(facts_sorted_path, candidates_dir)

        if not candidate_stats:
            logging.info("Skipping %s: no subjects with both inCellLine and another in* predicate", input_file)
            return results

        for has_predicate, stats in sorted(candidate_stats.items()):
            candidate_path = stats["candidate_path"]
            candidate_triples = stats["candidate_triples"]
            matched_subjects = stats["matched_subjects"]

            if not candidate_path or candidate_triples == 0:
                continue

            output_path = os.path.join(output_dir, f"{base_name}__{has_predicate}_derived.nt")
            logging.info(
                "Writing %s from %s matching subjects in %s",
                output_path,
                matched_subjects,
                input_file,
            )
            sort_unique(candidate_path, output_path, parallel=parallel, mem=sort_mem)
            line_count = count_lines(output_path)
            results.append((output_path, line_count))

    return results


def count_lines(path: str) -> int:
    with open(path, "r", encoding="utf-8") as fin:
        return sum(1 for _ in fin)


def derive_cell_line_has_relations(input_dir: str, output_dir: str, parallel: int = 8, sort_mem: str = "50%") -> list[tuple[str, int]]:
    input_files = iter_nt_files(input_dir)
    if not input_files:
        raise FileNotFoundError(f"No .nt files found in {input_dir}")

    os.makedirs(output_dir, exist_ok=True)
    all_results: list[tuple[str, int]] = []
    for input_file in input_files:
        all_results.extend(derive_from_file(input_file, output_dir, parallel=parallel, sort_mem=sort_mem))

    return all_results


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "For each source .nt file, derive <cell_line> <hasX> <object> triples from subjects that contain "
            "both inCellLine and another in* predicate, and write one output file per source dataset and derived predicate."
        )
    )
    parser.add_argument("--input-dir", required=True, help="Directory containing source .nt files.")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for derived .nt files. Defaults to the input directory.",
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

    output_dir = args.output_dir or args.input_dir
    results = derive_cell_line_has_relations(args.input_dir, output_dir, parallel=args.parallel, sort_mem=args.sort_mem)
    logging.info("Generated %s derived files", len(results))
    for output_path, line_count in results:
        logging.info("%s: %s triples", output_path, line_count)


if __name__ == "__main__":
    main()
