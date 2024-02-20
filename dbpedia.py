import logging
from SPARQLWrapper import SPARQLWrapper, JSON
import time

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize SPARQL endpoint
sparql_endpoint = "http://dbpedia.org/sparql"

# Function to read the last URI from the seed.txt file and count the entries
def read_last_uri_and_count_entries(file_path):
    count = 0
    last_uri = None
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            last_uri = line.strip()
            count += 1
    return last_uri, count

# Safe query execution with retries
def safe_query(sparql, retries=3, delay=5):
    for attempt in range(retries):
        try:
            return sparql.query().convert()
        except Exception as e:
            logging.error(f"Query failed on attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                raise

# Define the base query with an adjustable starting point, reduce LIMIT if necessary
def get_query(last_uri, limit=10000):  # Reduced batch size to 10000
    return f"""
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?entity WHERE {{
        ?class a owl:Class .
        FILTER(STRSTARTS(STR(?class), STR(dbo:)))
        ?entity rdf:type ?class .
        ?entity rdfs:label ?label .
        FILTER(LANG(?label) = "en")
        FILTER (STR(?entity) > "{last_uri}")
    }}
    ORDER BY ?entity
    LIMIT {limit}
    """

def fetch_entities_and_save(endpoint, initial_uri, filename):
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    last_uri = initial_uri
    appended_entities = 0
    total_entries = initial_entry_count
    
    while True:
        query = get_query(last_uri)
        sparql.setQuery(query)
        results = safe_query(sparql)  # Use safe_query for error handling
        
        batch_entities = [result["entity"]["value"] for result in results["results"]["bindings"]]
        batch_size = len(batch_entities)
        if batch_size == 0:
            break  # Exit loop if no more results
        
        # Save fetched entities to file immediately after each batch
        with open(filename, 'a', encoding='utf-8') as file:
            for entity in batch_entities:
                file.write(entity + '\n')
        
        last_uri = batch_entities[-1]
        appended_entities += batch_size
        total_entries += batch_size
        logging.debug(f"Appended {batch_size} entities. Total entries appended: {appended_entities}. Total entries in seed.txt: {total_entries}.")

# Path to your existing seed.txt file
seed_file_path = 'seed.txt'

# Read the last URI from the seed.txt file and count the existing entries
last_fetched_uri, initial_entry_count = read_last_uri_and_count_entries(seed_file_path)
logging.info(f"Starting with {initial_entry_count} entries already in {seed_file_path}.")

try:
    # Continue fetching entities and save after each batch
    fetch_entities_and_save(sparql_endpoint, last_fetched_uri, seed_file_path)
    logging.info("Finished fetching and saving all entities.")
except Exception as e:
    logging.error("An error occurred", exc_info=True)
