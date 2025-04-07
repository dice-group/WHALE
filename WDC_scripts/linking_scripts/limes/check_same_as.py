import requests
import time
from rdflib import Graph

sparql_endpoint = "https://dbpedia.org/sparql"

input_file = "/scratch/hpc-prf-whale/albert/WHALE/LIMES/output/leaf_classes_dbpedia_de/same_as_total.nt"
output_file = "/scratch/hpc-prf-whale/albert/WHALE/LIMES/output/leaf_classes_dbpedia_de/checked_same_as_total.nt"

max_retries = 10
initial_wait_time = 5

def ask_query(subject, obj):
    query = f"""
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX dbpedia: <http://dbpedia.org/resource/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/2000/01/rdf-schema#>

    ask{{
        {subject} <http://www.w3.org/2002/07/owl#sameAs> {obj} .
        filter(strstarts(str({obj}), "http://de.dbpedia.org/resource/"))
    }}
    """
    
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(sparql_endpoint, params={'query': query, 'format': 'application/sparql-results+json'})
            response.raise_for_status()
            try:
                return response.json().get('boolean', False)
            except ValueError:
                print(f"Error: Invalid JSON returned for {subject} -> {obj}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"Attempts {attempt} failed with error: {e}")
            if attempt < max_retries:
                wait_time = initial_wait_time * (2 ** (attempt - 1))
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print("Max retries reached. Skipping this query.")
                return False

with open(input_file, 'r') as infile:
    with open(output_file, 'w') as output:
        for line in infile:
            parts = line.strip().split('\t')
            s = parts[0]
            o = parts[1]
            similarity = parts[2]
            result = ask_query(s, o)

            result_pattern = f"{s} {o} {similarity}"
            s1 = f"{result_pattern} ✅\n"
            s2 = f"{result_pattern} ❌\n"

            if result:
                print(s1)
                output.write(s1)
            else:
                print(s2)
                output.write(s2)

print(f"Checked triples saved in {output_file}")

    

# graph = Graph()
# graph.parse(input_file, format='nt')

# with open(output_file, 'w') as output:
#     for subject, predicate, obj in graph:
#         subject_str = str(subject)
#         object_str = str(obj)

#         result = ask_query(subject_str, object_str)

#         result_pattern = f"<{subject_str}> <http://www.w3.org/2002/07/owl#sameAs> <{object_str}> ."
#         s1 = f"{result_pattern} ✅\n"
#         s2 = f"{result_pattern} ❌\n"

#         if result:
#             print(s1)
#             output.write(s1)
#         else:
#             print(s2)
#             output.write(s2)

# print(f"Checked triples saved in {output_file}")
