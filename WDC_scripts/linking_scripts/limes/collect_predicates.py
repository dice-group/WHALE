import sys
import re
from collections import defaultdict

def collect_predicates(input_file, output_file):
    print('Collecting predicates from the input file...')
    predicate_counts = defaultdict(int)
    quad_pattern = re.compile(r'^(<[^>]*>|_:b[0-9]+)\s(<[^>]*>|[^\s]+)\s(.+?)\s(<[^>]*>)?\s*\.\s*$')

    try:
        with open(input_file, 'r', encoding='utf-8') as infile:
            line_count = 0
            for line in infile:
                line_count += 1
                if line_count % 1000000 == 0:
                    print(f"Processing {line_count} lines...")
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                match = quad_pattern.match(line)
                if not match:
                    continue

                subject, predicate, obj, graph = match.groups()
                predicate = predicate.strip()
                predicate_counts[predicate] += 1
    except Exception as e:
        print(f"Error processing input file: {e}")
        sys.exit(1)

    sorted_predicates = sorted(predicate_counts.items(), key=lambda x: x[1], reverse=True)
    try:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            for predicate, count in sorted_predicates:
                outfile.write(f"{predicate}: {count}\n")
    except Exception as e:
        print(f"Error writing to the output file: {e}")
        sys.exit(1)

    print(f'Predicates are written on {output_file}.')

if __name__ == "__main__":
    input_file = "/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/datasets/bio2rdf/chebi/chebi.nt"
    output_file = "/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/datasets/bio2rdf/chebi/chebi_predicates.txt"
    collect_predicates(input_file, output_file)
