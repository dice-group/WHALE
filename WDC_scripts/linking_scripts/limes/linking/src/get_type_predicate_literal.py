import argparse
from collections import defaultdict

TYPE_PREDICATE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"


def load_types(path):
    types = set()

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if line:
                types.add(line)

    return types


def main():
    parser = argparse.ArgumentParser(
        description="Find type-predicate pairs where the predicate has a literal object."
    )

    parser.add_argument(
        "--in",
        dest="input_file",
        required=True,
        help="Input N-Triples file"
    )

    parser.add_argument(
        "--types",
        required=True,
        help="File containing rdf:type objects, one per line"
    )

    parser.add_argument(
        "--out",
        dest="output_file",
        required=True,
        help="Output TSV file containing type-predicate pairs"
    )

    args = parser.parse_args()

    target_types = load_types(args.types)

    print(f"Loaded {len(target_types):,} types")

    # subject -> set of its target types
    subject_types = defaultdict(set)

    #
    # Pass 1:
    # Find subjects belonging to the requested types.
    #
    with open(args.input_file, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            parts = line.split(maxsplit=2)

            if len(parts) < 3:
                continue

            subject, predicate, obj = parts

            if predicate != TYPE_PREDICATE:
                continue

            if obj.endswith(" ."):
                obj = obj[:-2].strip()

            if obj in target_types:
                subject_types[subject].add(obj)

    print(f"Found {len(subject_types):,} subjects with matching types")

    pairs = set()

    #
    # Pass 2:
    # Find predicates whose object is a literal.
    #
    with open(args.input_file, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            parts = line.split(maxsplit=2)

            if len(parts) < 3:
                continue

            subject, predicate, obj = parts

            if subject not in subject_types:
                continue

            # N-Triples literals begin with "
            if not obj.startswith('"'):
                continue

            for rdf_type in subject_types[subject]:
                pairs.add((rdf_type, predicate))

    with open(args.output_file, "w", encoding="utf-8") as f:
        for rdf_type, predicate in sorted(pairs):
            f.write(f"{rdf_type}\t{predicate}\n")

    print(f"Found {len(pairs):,} unique type-predicate pairs")


if __name__ == "__main__":
    main()