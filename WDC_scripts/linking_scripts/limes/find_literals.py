from rdflib import Graph, URIRef
from rdflib.namespace import RDF, OWL

input_file = '/scratch/hpc-prf-whale/albert/WHALE/WDC_scripts/linking_scripts/limes/datasets/sgd_sparql/sgd_sparql_clean.nt'

g = Graph()
g.parse(input_file, format='nt')

class_ = URIRef('http://bio2rdf.org/sgd_vocabulary:GlutamineCount')

for s, p, o in g.triples((None, None, OWL.Class)):
        print(f'<{s}> <{p}> <{o}> .')
