from rdflib import ConjunctiveGraph, RDF, RDFS
from rdflib.namespace import SKOS, OWL, DC
import sys
import re

def extract_classes_streaming(input_file, output_file):
    print('Processing the input file line by line...')
    class_set = set()
    label_dict = {}
    type_predicates = {
        '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
        '<http://bio2rdf.org/goa_vocabulary:function>',
        '<http://bio2rdf.org/goa_vocabulary:process>',
        '<http://bio2rdf.org/goa_vocabulary:component>',
        '<http://bio2rdf.org/goa_vocabulary:not-in-component>',
        '<http://bio2rdf.org/goa_vocabulary:not-in-process>',
        '<http://bio2rdf.org/goa_vocabulary:not-has-function>',
        'rdf:type',
        'a' 
    }
    label_predicates = {
        '<http://www.w3.org/2000/01/rdf-schema#label>',
        '<http://purl.org/dc/terms/title>',
        '<http://bio2rdf.org/goa_vocabulary:synonym>',
        '<http://bio2rdf.org/goa_vocabulary:symbol>',
        '<http://purl.org/dc/terms/identifier>'
    }

    quad_pattern = re.compile(r'^(<[^>]*>|_:b[0-9]+)\s(<[^>]*>|[^\s]+)\s(.+?)\s(<[^>]*>)?\s*\.\s*$')

    try:
        with open(input_file, 'r', encoding='utf-8') as infile:
            line_count = 0
            for line in infile:
                line_count += 1
                if line_count % 1000000 == 0:
                    print(f'Processed {line_count} lines...')
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                match = quad_pattern.match(line)
                if  not match:
                    continue

                subject, predicate, obj, graph = match.groups()
                predicate = predicate.strip()
                obj = obj.strip().rstrip('.')

                if predicate in type_predicates:
                    if obj.startswith('<') and obj.endswith('>'):
                        class_set.add(obj)
                elif predicate in label_predicates:
                    if subject in class_set:
                        label_match = re.match(r'^"(.+?)"(?:@[\w\-]+|\^\^<[^>]+>)?$', obj)
                        if label_match:
                            label = label_match.group(1)
                            label_dict.setdefault(subject, set()).add(label)
    except Exception as e:
        print(f'Error processing the input file: {e}')
        sys.exit(1)

    print(f'Total unique classes found: {len(class_set)}')
    print("Writting classes and their labels to the output file...")

    try:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            for cls in class_set:
                labels = label_dict.get(cls, {cls.strip('<>').split('/')[-1].split('#')[-1]})
                labels_str = '; '.join(labels).replace('\\', '\\\\').replace('"', '\\"')
                outfile.write(f'{cls} <http://www.w3.org/2000/01/rdf-schema#label> "{labels_str}" .\n')
    except Exception as e:
        print(f"Error writing to the output file: {e}")
        sys.exit(1)

    print(f"Classes and their labels have been successfully extracted and saved to {output_file}")

def extract_classes(input_file, output_file):
    g = ConjunctiveGraph()
    print('Parsing the input file with rdflib...')
    try:
        g.parse(input_file, format='nquads')
    except Exception as e:
        print(f"Error parsing the input file: {e}")
        sys.exit(1)

    print("Executing SPARQL query to extract classes and their labels...")
    query = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX dc: <http://purl.org/dc/terms/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    SELECT DISTINCT ?class (GROUP_CONCAT(DISTINCT ?label; separator="; ") AS ?labels)
    WHERE {
        GRAPH ?g {
            ?instance rdf:type ?class .
            FILTER(isIRI(?class))
            OPTIONAL { ?class rdfs:label ?rdfs_label . }
            OPTIONAL { ?class dc:title ?dc_title . }
            OPTIONAL { ?class skos:prefLabel ?skos_label . }
            BIND(COALESCE(?rdfs_label, ?dc_title, ?skos_label, "") AS ?label)
        }
    }
    GROUP BY ?class
    """

    results = g.query(query)
    print(f"Total unique classes found: {len(results)}")

    print("Writing classes and their labels to the output file...")
    try:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            for row in results:
                cls = row['class']
                labels = row['labels'] if row['labels'] else str(cls).split('/')[-1].split('#')[-1]
                escaped_labels = str(labels).replace('\\', '\\\\').replace('"', '\\"')
                outfile.write(f'<{cls}> <{RDFS.label}> "{escaped_labels}" .\n')
    except Exception as e:
        print(f"Error writing to the output file: {e}")
        sys.exit(1)

    print(f"Classes and their labels have been successfully extracted and saved to {output_file}")
    print("Displaying some examples of classes with labels:")

    try:
        with open(output_file, 'r', encoding='utf-8') as outfile:
            for i in range(25):
                line = outfile.readline()
                if not line:
                    break
                print(line.strip())
    except Exception as e:
        print(f"Error reading the output file: {e}")

if __name__ == "__main__":
    input_file = "/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/datasets/bio2rdf/merged_goa_clean.nq"
    output_file = "/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/datasets/bio2rdf/merged_goa_clean_classes.nt"
    extract_classes_streaming(input_file, output_file)
