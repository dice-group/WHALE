import sys
from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import RDF, OWL
from pathlib import Path

if len(sys.argv) != 4:
    print("Usage: python rdf_to_nt.py <input_rdf_file> <output_nt_file> <relation_type>")
    print("Relation type should be either 'eq' for equivalentClass, 'sa' for sameAs' or 'eqp' for equivalentProperty.")
    sys.exit(1)

file_path_input = Path(sys.argv[1])
file_path_output = Path(sys.argv[2])
relation_type = sys.argv[3]

if relation_type not in ['eq', 'sa', 'eqp']:
    print("Invalid relation type. Use 'eq' for equivalentClass, 'sa' for sameAs and 'eqp' for equivalentProperty.")
    sys.exit(1)

ALIGN = Namespace("http://knowledgeweb.semanticweb.org/heterogeneity/alignment")

g = Graph()
g.parse(file_path_input, format="application/rdf+xml")

output_graph = Graph()

for cell in g.subjects(RDF.type, ALIGN.Cell):
    entity1 = g.value(cell, ALIGN.entity1)
    entity2 = g.value(cell, ALIGN.entity2)

    if isinstance(entity1, URIRef) and isinstance(entity2, URIRef):
        if relation_type == 'eq':
            output_graph.add((entity1, OWL.equivalentClass, entity2))
        elif relation_type == 'sa':
            output_graph.add((entity1, OWL.sameAs, entity2))
        else:
            output_graph.add((entity1, OWL.equivalentProperty, entity2))


output_graph.serialize(destination=file_path_output, format="nt")

print(f"New N-Triples file created at: {file_path_output}")
