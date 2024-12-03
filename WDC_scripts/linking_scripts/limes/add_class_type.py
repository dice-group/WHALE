import sys
from pathlib import Path
from rdflib import Graph, Namespace, RDF
from triples_validator import sanitize

if len(sys.argv) != 3:
    print("Usage: python add_class_type.py <input_nt_file> <output_rdf_file>")
    sys.exit(1)

input_file_path = Path(sys.argv[1])
output_file_path = Path(sys.argv[2])

sanitized_file_path = input_file_path.with_name(f"{input_file_path.stem}_sanitized.nt")

print(f"Sanitizing input file: {input_file_path}")
sanitize(str(input_file_path), str(sanitized_file_path))

OWL = Namespace("http://www.w3.org/2002/07/owl#")
g = Graph()
g.parse(sanitized_file_path, format="nt")

for subject in set(g.subjects()):
    g.add((subject, RDF.type, OWL.Class))

g.serialize(output_file_path, format="pretty-xml")

print(f"New RDF file created at: {output_file_path}")

sanitized_file_path.unlink(missing_ok=True)
