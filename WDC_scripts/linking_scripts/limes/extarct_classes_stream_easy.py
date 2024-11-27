from rdflib import Graph
from rdflib.namespace import RDFS, OWL, RDF

input_file = "/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/datasets/sgd_sparql/sgd_sparql_clean.nt"
output_file = "/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/datasets/sgd_sparql/classes.nt"

with open(output_file, "w") as f:
    g = Graph()

    for stmt in g.parse(input_file, format='nt'):
        if stmt[1] == RDF.type and stmt[2] == OWL.Class:
            label = g.value(stmt[0], RDFS.label)

            if label:
                triple = f'<{stmt[0]}> <{OWL.Class}> \"{label}\" .\n'
                f.write(triple)

print(f"Classes with labels have been written to {output_file}")

