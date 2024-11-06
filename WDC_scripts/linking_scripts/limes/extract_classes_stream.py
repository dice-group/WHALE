from rdflib import Graph, RDF, RDFS, SKOS
import time

def extract_classes(input_file, output_file, label_predicates=None):
    if label_predicates is None:
        label_predicates = [RDFS.label, SKOS.prefLabel]

        classes  = set()
        class_labels = {}

        start_time = time.time()
        print("Starting streaming parsing of the input N-Quads file...")

        try:
            with open(input_file, 'r', encoding='utf-8') as infile:
                for idx, line in enumerate(infile):
                    if 'rdf-syntax-ns#type' in line or any(pred in line for pred in ['rdfs#label', 'skos#prefLabel']):
                        try:
                            g = Graph()
                            g.parse(data=line, format='nquads')
                            for s, p, o in g:
                                if p == RDF.type:
                                    classes.add(o)
                                elif p in label_predicates and s in classes:
                                    class_labels.setdefault(s, set()).add(str(o))
                        except Exception:
                            continue
                    if (idx + 1) % 1000000 == 0:
                        print(f"Processed {idx + 1} lines.")
        except Exception as e:
            print(f"Error during streaming parsing: {e}")
            return

        parse_time = time.time()
        print(f"Streaming parsing completed in {parse_time - start_time:.2f} seconds.")
        print(f"Total unique classes found: {len(classes)}")

        for cls in classes:
            if cls not in class_labels:
                label = cls.split('/')[-1].split('#')[-1]
                class_labels[cls] = {label}

        print("Writing classes and their labels to the output N-Triples file...")

        try:
            with open(output_file, 'w', encoding='utf-8') as outfile:
                for cls, labels in class_labels.items():
                    concatenated_labels = '; '.join(labels)
                    escaped_labels = concatenated_labels.replace('\\', '\\\\').replace('"', '\\"')
                    outfile.write(f'<{cls}> <{RDFS.label}> "{escaped_labels}" .\n')
        except Exception as e:
            print(f"Error writing to the output file: {e}")
            return

        write_time = time.time()
        print(f"Writing complted in {write_time - parse_time:.2f} seconds.")
        print(f"Classes and their labels have been successfully saved to {output_file}")

        total_time = time.time()
        print(f"\nTotal execution time: {total_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    input_file = "/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/datasets/bio2rdf/merged_goa_clean.nq"
    output_file = "/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/datasets/bio2rdf/merged_goa_classes_stream.nt"
    extract_classes(input_file, output_file)
