import logging
from SPARQLWrapper import SPARQLWrapper, JSON
import time
import argparse
import os

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Argument parsing
parser = argparse.ArgumentParser(description="Fetch data from DBpedia or Wikidata.")
parser.add_argument(
    "dataset",
    type=str,
    choices=["dbpedia", "wikidata"],
    help="The dataset to fetch data from ('dbpedia' or 'wikidata').",
)
args = parser.parse_args()

# Setting dataset-specific parameters based on user input
if args.dataset == "dbpedia":
    sparql_endpoint = "http://dbpedia.org/sparql"
    seed_file_path = "seed_dbpedia.txt"
    dataset_prefixes = """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    """
elif args.dataset == "wikidata":
    sparql_endpoint = "https://query.wikidata.org/sparql"
    seed_file_path = "seed_wikidata.txt"
    dataset_prefixes = """
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX bd: <http://www.bigdata.com/rdf#>
    """


# Function to read the last URI from the seed.txt file and count the entries
def read_last_uri_and_count_entries(file_path):
    # Check if the file exists
    if not os.path.exists(file_path):
        # If the file does not exist, create it and return initial values
        with open(file_path, "w", encoding="utf-8") as file:
            pass  # Just create the file, no need to write anything
        return None, 0  # Assuming None is an appropriate initial value for last_uri

    # If the file exists, proceed as before
    count = 0
    last_uri = None
    with open(file_path, "r", encoding="utf-8") as file:
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


# Modify the get_query function to use dataset-specific prefixes and query structure
def get_query(dataset, last_uri=None, limit=10000):
    filter_clause = ""
    if last_uri:
        if dataset == "dbpedia":
            filter_clause = f'FILTER (STR(?entity) > "{last_uri}")'
        elif dataset == "wikidata":
            filter_clause = f'FILTER (STR(?entity) > "wd:{last_uri}")'

    if dataset == "dbpedia":
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
            {filter_clause}
        }}
        ORDER BY ?entity
        LIMIT {limit}
        """
    elif dataset == "wikidata":
        return f"""
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX wikibase: <http://wikiba.se/ontology#>
        PREFIX bd: <http://www.bigdata.com/rdf#>

        SELECT DISTINCT ?entity WHERE {{
          ?class wdt:P31 wd:Q16889133. # Items that are instances of "class"
          ?entity wdt:P31 ?class.
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
          FILTER EXISTS {{ ?entity rdfs:label ?label FILTER(LANG(?label) = "en") }}
          {filter_clause}
        }}
        ORDER BY ?entity
        LIMIT {limit}
        """


# Function to fetch entities and save to a file, adapted to use the dataset argument
def fetch_entities_and_save(dataset, endpoint, last_fetched_uri, filename):
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    last_uri = last_fetched_uri
    appended_entities = 0

    while True:
        query = get_query(dataset, last_uri)
        sparql.setQuery(query)
        results = safe_query(sparql)  # Use safe_query for error handling

        if not results or "bindings" not in results["results"]:
            break

        batch_entities = [
            result["entity"]["value"] for result in results["results"]["bindings"]
        ]
        if not batch_entities:
            break  # Exit loop if no more results

        with open(filename, "a", encoding="utf-8") as file:
            for entity in batch_entities:
                file.write(entity + "\n")

        last_uri = batch_entities[-1].split("/")[
            -1
        ]  # Update for both DBpedia and Wikidata
        appended_entities += len(batch_entities)
        logging.debug(
            f"Appended {len(batch_entities)} entities. Total entries now: {appended_entities}."
        )


# Read the last URI from the seed.txt file and count the existing entries
last_fetched_uri, initial_entry_count = read_last_uri_and_count_entries(seed_file_path)
logging.info(
    f"Starting with {initial_entry_count} entries already in {seed_file_path}."
)

try:
    fetch_entities_and_save(
        args.dataset, sparql_endpoint, last_fetched_uri, seed_file_path
    )
    logging.info("Finished fetching and saving all entities.")
except Exception as e:
    logging.error("An error occurred", exc_info=True)
