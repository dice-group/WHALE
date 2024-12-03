from SPARQLWrapper import SPARQLWrapper
import json

sparql = SPARQLWrapper('http://localhost:8890/sparql')

query = """
select distinct ?class
where {
    graph <http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3> {
        ?class rdf:type owl:Class .
    }
}
"""

sparql.setQuery(query)

sparql.setReturnFormat('json')

try:
    results = sparql.query()
    results_dict = results.convert()

    if all(var in [binding for result in results_dict['results']['bindings'] for binding in result] for var in ['s', 'p', 'o']):
        ntriples_output = []

        for binding in results_dict['results']['bindings']:
            subject = binding['s']['value']
            predicate = binding['p']['value']
            object_value = binding['o']['value']

            ntriples_line = f"<{subject}> <{predicate}> <{object_value}> ."
            ntriples_output.append(ntriples_line)

        output_file = 'virtuoso_said.nt'
        with open(output_file, 'w') as nt_file:
            for line in ntriples_output:
                nt_file.write(line + '\n')

        print(f'Triples saved to {output_file}')

    else:
        output_file = 'virtuoso_guy_said.json'
        with open(output_file, 'w') as json_file:
            json.dump(results_dict, json_file, indent=4)
        print(f'Results saved to {output_file}')

except Exception as e:
    print(f"An error occured: {e}")
