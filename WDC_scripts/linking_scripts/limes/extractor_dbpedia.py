import sys
from rdflib import Graph, URIRef
from rdflib.namespace import RDF, OWL
from rdflib.util import guess_format

def process_rdf(input_file, output_file_classes, output_file_properties):
    rdf_format = guess_format(input_file)
    if rdf_format is None:
        raise ValueError(f'Could not determine the RDF format for the input file: {input_file}')

    g = Graph()

    g.parse(input_file, format=rdf_format)

    classes_dict = {}
    properties_dict = {}

    for subj, pred, obj in g:
        pred_local = get_local_name(pred)
        properties_dict[pred] = pred_local

        if pred == RDF.type and isinstance(obj, URIRef):
            class_local = get_local_name(obj)
            classes_dict[obj] = class_local

    with open(output_file_classes, 'w', encoding='utf-8') as f_classes:
        for class_uri, class_local in classes_dict.items():
            triple = f'<{class_uri}> <{OWL.Class}> \"{class_local}\" .\n'
            f_classes.write(triple)

    with open(output_file_properties, 'w', encoding='utf-8') as f_properties:
        for prop_uri, prop_local in properties_dict.items():
            triple = f'<{prop_uri}> <{RDF.Property}> \"{prop_local}\" .\n'
            f_properties.write(triple)

    print(f"Classes with labels have been written to {output_file_classes}")
    print(f"Properties with labels have been written to {output_file_properties}")

def get_local_name(uri):
    uri_str = str(uri)
    if '#' in uri_str:
        return uri_str.split('#')[-1]
    else:
        return uri_str.rstrip('/').split('/')[-1]

def main():
    if len(sys.argv) != 4:
        print("Usage: python script.py <input_file> <output_file_classes> <output_file_properties>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file_classes = sys.argv[2]
    output_file_properties = sys.argv[3]

    try:
        process_rdf(input_file, output_file_classes, output_file_properties)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
