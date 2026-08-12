import argparse

TYPE_PREDICATE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"


def main():
    parser = argparse.ArgumentParser(
        description="Extract unique objects of rdf:type from an N-Triples file."
    )
    parser.add_argument(
        "--in",
        dest="input_file",
        required=True,
        help="Input .nt file"
    )
    parser.add_argument(
        "--out",
        dest="output_file",
        required=True,
        help="Output file for unique rdf:type objects"
    )

    args = parser.parse_args()

    unique_objects = set()

    with open(args.input_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            parts = line.split(maxsplit=2)

            if len(parts) < 3:
                continue

            subject, predicate, obj = parts

            if predicate == TYPE_PREDICATE:
                if obj.endswith(" ."):
                    obj = obj[:-2].strip()

                unique_objects.add(obj)

    with open(args.output_file, "w", encoding="utf-8") as f:
        for obj in sorted(unique_objects):
            f.write(obj + "\n")

    print(f"Found {len(unique_objects):,} unique rdf:type objects")


if __name__ == "__main__":
    main()