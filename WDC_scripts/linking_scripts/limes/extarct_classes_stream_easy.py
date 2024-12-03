import sys
from rdflib import Graph
from rdflib.namespace import RDFS, OWL, RDF
from rdflib.util import guess_format

def process_rdf(input_file, output_file_classes, output_file_properties):
    rdf_format = guess_format(input_file)
    if rdf_format is None:
        raise ValueError(f'Could not determine the RDF format for the input file: {input_file}')

    g = Graph()

    with open(output_file_classes, "w") as f_classes, open(output_file_properties, 'w') as f_properties:

        for stmt in g.parse(input_file, format=rdf_format):
            if stmt[1] == RDF.type and stmt[2] == OWL.Class:
                label = g.value(stmt[0], RDFS.label)

                if label:
                    triple = f'<{stmt[0]}> <{OWL.Class}> \"{label}\" .\n'
                    f_classes.write(triple)

            if stmt[1] == RDF.type and stmt[2] == RDF.Property:
                label = g.value(stmt[0], RDFS.label)
                if label:
                    triple = f'<{stmt[0]}> <{RDF.Property}> \"{label}\" .\n'
                    f_properties.write(triple)

            if stmt[1] == RDF.type and stmt[2] in [OWL.ObjectProperty, OWL.DatatypeProperty]:
                label = g.value(stmt[0], RDFS.label)
                if label:
                    property_type = "owl:ObjectProperty" if stmt[2] == OWL.ObjectProperty else "OWL:DatatypeProperty"
                    triple = f'<{stmt[0]}> <{property_type}> \"{label}\" .\n'
                    f_properties.write(triple)

    print(f"Classes with labels have been written to {output_file_classes}")
    print(f"Properties with labels have been written to {output_file_properties}")

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
