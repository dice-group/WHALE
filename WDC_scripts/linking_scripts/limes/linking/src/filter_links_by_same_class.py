#!/usr/bin/env python3
import argparse
import logging
import os
from collections import Counter

RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"


def iter_nt_files(path: str) -> list[str]:
    if os.path.isfile(path):
        return [path]

    files: list[str] = []
    for entry in os.scandir(path):
        if entry.is_file() and entry.name.endswith(".nt"):
            files.append(entry.path)
    files.sort()
    return files


def load_link_resources(links_path: str) -> tuple[set[str], set[str], int]:
    left_resources: set[str] = set()
    right_resources: set[str] = set()
    link_count = 0

    with open(links_path, "r", encoding="utf-8", errors="replace") as fin:
        for line in fin:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            left_resources.add(parts[0])
            right_resources.add(parts[1])
            link_count += 1

    return left_resources, right_resources, link_count


def parse_type_line(line: str) -> tuple[str, str] | None:
    parts = line.split()
    if len(parts) < 4:
        return None
    if parts[1] != RDF_TYPE:
        return None
    if not parts[0].startswith("<") or not parts[2].startswith("<"):
        return None
    return parts[0], parts[2]


def collect_resource_classes(dataset_path: str, resource_filter: set[str]) -> tuple[dict[str, set[str]], set[str], Counter[str], int]:
    resource_classes: dict[str, set[str]] = {}
    all_classes: set[str] = set()
    class_counts: Counter[str] = Counter()
    type_triples = 0

    input_files = iter_nt_files(dataset_path)
    if not input_files:
        raise FileNotFoundError(f"No .nt files found in {dataset_path}")

    for input_file in input_files:
        logging.info("Scanning %s", input_file)
        with open(input_file, "r", encoding="utf-8", errors="replace") as fin:
            for line in fin:
                parsed = parse_type_line(line)
                if not parsed:
                    continue
                subject, class_uri = parsed
                type_triples += 1
                all_classes.add(class_uri)
                class_counts[class_uri] += 1
                if subject in resource_filter:
                    resource_classes.setdefault(subject, set()).add(class_uri)

    return resource_classes, all_classes, class_counts, type_triples


def write_class_inventory(output_path: str, classes: set[str], class_counts: Counter[str]) -> None:
    with open(output_path, "w", encoding="utf-8") as fout:
        for class_uri in sorted(classes, key=lambda item: (-class_counts[item], item)):
            fout.write(f"{class_uri}\t{class_counts[class_uri]}\n")


def filter_links(
    links_path: str,
    output_path: str,
    left_resource_classes: dict[str, set[str]],
    right_resource_classes: dict[str, set[str]],
    audit_path: str | None = None,
) -> dict[str, int]:
    stats = {
        "total_links": 0,
        "kept_links": 0,
        "dropped_links": 0,
        "left_missing_class": 0,
        "right_missing_class": 0,
        "both_missing_class": 0,
        "class_mismatch": 0,
    }

    with open(links_path, "r", encoding="utf-8", errors="replace") as fin, open(output_path, "w", encoding="utf-8") as fout:
        audit_out = open(audit_path, "w", encoding="utf-8") if audit_path else None
        try:
            for line in fin:
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 2:
                    continue

                left_uri = parts[0]
                right_uri = parts[1]
                stats["total_links"] += 1

                left_classes = left_resource_classes.get(left_uri)
                right_classes = right_resource_classes.get(right_uri)

                if not left_classes and not right_classes:
                    stats["both_missing_class"] += 1
                    stats["dropped_links"] += 1
                    continue
                if not left_classes:
                    stats["left_missing_class"] += 1
                    stats["dropped_links"] += 1
                    continue
                if not right_classes:
                    stats["right_missing_class"] += 1
                    stats["dropped_links"] += 1
                    continue

                shared_classes = sorted(left_classes & right_classes)
                if not shared_classes:
                    stats["class_mismatch"] += 1
                    stats["dropped_links"] += 1
                    continue

                fout.write(line if line.endswith("\n") else f"{line}\n")
                stats["kept_links"] += 1
                if audit_out is not None:
                    score = parts[2] if len(parts) >= 3 else "1.0"
                    audit_out.write(
                        f"{left_uri}\t{right_uri}\t{score}\t{'|'.join(shared_classes)}\n"
                    )
        finally:
            if audit_out is not None:
                audit_out.close()

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Filter cross-dataset link pairs, keeping only those where left and right resources share at least one exact rdf:type class."
    )
    parser.add_argument("--left-dataset", required=True, help="Directory or .nt file for the left dataset.")
    parser.add_argument("--right-dataset", required=True, help="Directory or .nt file for the right dataset.")
    parser.add_argument("--links", required=True, help="Input TSV with <left>\\t<right>\\t<score> links.")
    parser.add_argument("--output", required=True, help="Output TSV with only same-class links.")
    parser.add_argument(
        "--audit",
        default=None,
        help="Optional audit TSV with an extra column listing shared classes.",
    )
    parser.add_argument(
        "--left-class-inventory",
        default=None,
        help="Optional output TSV of unique classes found in the left dataset with rdf:type triple counts.",
    )
    parser.add_argument(
        "--right-class-inventory",
        default=None,
        help="Optional output TSV of unique classes found in the right dataset with rdf:type triple counts.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s [%(levelname)s] %(message)s")

    output_dir = os.path.dirname(os.path.abspath(args.output)) or os.getcwd()
    os.makedirs(output_dir, exist_ok=True)
    if args.audit:
        os.makedirs(os.path.dirname(os.path.abspath(args.audit)) or os.getcwd(), exist_ok=True)
    if args.left_class_inventory:
        os.makedirs(os.path.dirname(os.path.abspath(args.left_class_inventory)) or os.getcwd(), exist_ok=True)
    if args.right_class_inventory:
        os.makedirs(os.path.dirname(os.path.abspath(args.right_class_inventory)) or os.getcwd(), exist_ok=True)

    left_resources, right_resources, link_count = load_link_resources(args.links)
    logging.info(
        "Loaded %s links with %s unique left resources and %s unique right resources",
        link_count,
        len(left_resources),
        len(right_resources),
    )

    left_resource_classes, left_classes, left_class_counts, left_type_triples = collect_resource_classes(
        args.left_dataset, left_resources
    )
    logging.info(
        "Left dataset: %s rdf:type triples, %s unique classes, %s linked resources with classes",
        left_type_triples,
        len(left_classes),
        len(left_resource_classes),
    )

    right_resource_classes, right_classes, right_class_counts, right_type_triples = collect_resource_classes(
        args.right_dataset, right_resources
    )
    logging.info(
        "Right dataset: %s rdf:type triples, %s unique classes, %s linked resources with classes",
        right_type_triples,
        len(right_classes),
        len(right_resource_classes),
    )

    if args.left_class_inventory:
        write_class_inventory(args.left_class_inventory, left_classes, left_class_counts)
    if args.right_class_inventory:
        write_class_inventory(args.right_class_inventory, right_classes, right_class_counts)

    stats = filter_links(
        args.links,
        args.output,
        left_resource_classes,
        right_resource_classes,
        audit_path=args.audit,
    )

    logging.info(
        "Kept %s of %s links; dropped %s (left missing class=%s, right missing class=%s, both missing=%s, class mismatch=%s)",
        stats["kept_links"],
        stats["total_links"],
        stats["dropped_links"],
        stats["left_missing_class"],
        stats["right_missing_class"],
        stats["both_missing_class"],
        stats["class_mismatch"],
    )


if __name__ == "__main__":
    main()
