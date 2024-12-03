from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import Graph, RDF, RDFS, SKOS, URIRef
import time

def extract_classes(source, output_file, source_type="file", input_format="nquads", label_predicates=None):
    if label_predicates is None:
        label_predicates = [
            RDFS.label
        ]

    classes  = set()
    class_labels = {}

    start_time = time.time()
    print("Starting data extraction...")

    try:
        if source_type == "file":
            print(f"Processing local file: {source}")
            with open(source, 'r', encoding='utf-8') as infile:
                for idx, line in enumerate(infile):
                    if 'rdf-syntax-ns#type' in line or any(pred in line for pred in label_predicates):
                        try:
                            g = Graph()
                            g.parse(data=line, format=input_format)
                            for s, p, o in g:
                                if p == RDF.type:
                                    classes.add(s)
                                elif p in label_predicates and s in classes:
                                    class_labels.setdefault(s, set()).add(str(o))
                        except Exception:
                            continue
                    if (idx + 1) % 1000000 == 0:
                        print(f"Processed {idx + 1} lines.")

        elif source_type == 'sparql':
            print(f"Querying SPARQL endpoint: {source}")
            sparql = SPARQLWrapper(source)
            sparql.setReturnFormat(JSON)
            sparql.setTimeout(600)

            print("Fetching all classes...")
            limit = 10000
            offset = 0
            while True:
                class_query = f"""
                select distinct ?s where {{
                    graph <http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3> {{
                        ?s a ?type .
                    }} 
                }} limit {limit} offset {offset}
                """
                sparql.setQuery(class_query)
                try:
                    results = sparql.query().convert()
                    if "results" in results and "bindings" in results["results"]:
                        bindings = results["results"]["bindings"]
                    else:
                        print(f"Unexpected response structure at {offset}: {results}")
                        break
                except Exception as e:
                    print(f"Error during data extraction at offset {offset}: {e}")
                    try:
                        response = sparql.query().response.read()
                        print(f"Response from endpoint: {response}")
                    except Exception as inner_e:
                        print(f"Could not retrieve response content: {inner_e}")
                    break
                if not bindings:
                    break
                for result in bindings:
                    s = result["s"]["value"]
                    classes.add(s)
                offset += limit
                print(f"Fetched {len(classes)} classes so far...")

            print(f"Total classes fetched: {len(classes)}")

            print("Fetching labels for classes...")
            for predicate in label_predicates:
                print(f"Fetching labels with predicate {predicate}")
                limit = 10000
                offset = 0
                while True:
                    label_query = f"""
                    select distinct ?s ?label where {{
                        graph <http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3> {{
                            ?s <{predicate}> ?label .
                        }}
                    }} limit {limit} offset {offset}
                    """
                    sparql.setQuery(label_query)
                    results = sparql.query().convert()
                    bindings = results["results"]["bindings"]
                    if not bindings:
                        break
                    for result in bindings:
                        s = result["s"]["value"]
                        label = result["label"]["value"]
                        if s in classes:
                            class_labels.setdefault(s, set()).add(label)
                    offset += limit
                print(f"Finished fetching labels with predicate {predicate}")

        else:
            print("Unsuported source_type. Please use 'file' or 'sparql'.")
            return

    except Exception as e:
        print(f"Error during streaming parsing: {e}")
        return

    extraction_time = time.time()
    print(f"Data extraction completed in {extraction_time - start_time:.2f} seconds.")
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
    print(f"Writing complted in {write_time - extraction_time:.2f} seconds.")
    print(f"Classes and their labels have been successfully saved to {output_file}")

    total_time = time.time()
    print(f"\nTotal execution time: {total_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    output_file = "/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/datasets/sgd_sparql/classes.nt"
    source = "/scratch/hpc-prf-whale/bio2rdf/raw_data/sgd_sparql.nt"
    extract_classes(source, output_file, input_format='nt')
