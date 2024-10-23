import logging
import time
from SPARQLWrapper import SPARQLWrapper
from urllib.error import HTTPError
from tqdm import tqdm

class SPARQLQueryExecutor:
    def __init__(self, endpoint_url, base_query, output_file, limit=10000, max_attempts=5):
        """
        Initializes the SPARQLQueryExecutor with the given parameters.

        :param endpoint_url: The SPARQL endpoint URL.
        :param base_query: The base SPARQL CONSTRUCT query without LIMIT and OFFSET.
        :param output_file: The path to the output file where results will be written.
        :param limit: The number of triples to fetch per query.
        :param max_attempts: The maximum number of retry attempts for failed queries.
        """
        self.endpoint_url = endpoint_url
        self.base_query = base_query
        self.output_file = output_file
        self.limit = limit
        self.offset = 0
        self.max_attempts = max_attempts
        self.sparql = SPARQLWrapper(endpoint_url)
        # Do not set return format; use endpoint's default
        self.sparql.setTimeout(600)  # Set timeout to 10 minutes
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

    def execute_safe_query(self, query):
        """
        Executes the SPARQL query with retry logic.

        :param query: The complete SPARQL query with LIMIT and OFFSET.
        :return: Serialized query results as a string if successful, else None.
        """
        base_delay = 1  # Base delay in seconds for retries
        for attempt in range(1, self.max_attempts + 1):
            try:
                self.sparql.setQuery(query)
                results = self.sparql.queryAndConvert()
                data = results.serialize().strip()
                return data
            except HTTPError as e:
                logging.error(f"HTTP error on attempt {attempt}: {e}")
                if e.code == 429:
                    sleep_time = base_delay * 2 ** (attempt - 1)
                    logging.info(f"Rate limit exceeded. Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                elif e.code == 406:
                    logging.error("Received 406 Not Acceptable. The requested format is not supported.")
                    break  # Do not retry for unsupported formats
                else:
                    logging.error(f"Unhandled HTTP error: {e}")
                    break  # Do not retry for other HTTP errors
            except Exception as e:
                logging.error(f"An error occurred on attempt {attempt}: {e}")
                sleep_time = base_delay * 2 ** (attempt - 1)
                logging.info(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
        logging.error("Failed to execute query after multiple attempts.")
        return None

    def _construct_query(self):
        """
        Constructs the complete SPARQL query by appending LIMIT and OFFSET.

        :return: The complete SPARQL query string.
        """
        return f"""
        {self.base_query}
        LIMIT {self.limit}
        OFFSET {self.offset}
        """

    def fetch_and_write(self):
        """
        Fetches data in batches and writes the triples to the output file in N-Triples format.
        """
        results_obtained = True
        total_triples = 0
        with open(self.output_file, 'ab') as f_out:  # Open in append and binary mode
            pbar = tqdm(desc="Triples fetched", unit="triples")
            while results_obtained:
                query = self._construct_query()
                logging.info(f"Fetching results with OFFSET {self.offset} and LIMIT {self.limit}")
                data = self.execute_safe_query(query)
                if data:
                    try:
                        # Split the serialized data into individual lines
                        lines = data.split('\n\n')
                        # Filter out prefix declarations
                        triples = [line.strip() for line in lines]
                        if triples:
                            # Write each triple to the output file
                            for triple in triples:
                                f_out.write((triple + '\n').encode('utf-8'))
                                total_triples += 1
                                pbar.update(1)
                            logging.info(f"Fetched {len(triples)} triples")
                            self.offset += self.limit
                        else:
                            logging.info("No more triples found.")
                            results_obtained = False
                    except Exception as e:
                        logging.error(f"Error writing data: {e}")
                        results_obtained = False
                else:
                    logging.info("No more data to fetch.")
                    results_obtained = False
            pbar.close()
            logging.info(f"Total triples fetched: {total_triples}")

if __name__ == "__main__":
    # SPARQL endpoint
    endpoint_url = "https://bio2rdf.org/sparql"

    # Base query without LIMIT and OFFSET
    base_query = """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>

    CONSTRUCT {
      ?s1 ?p1 ?o1 .
      ?bioportalEntity ?p ?o .
      ?s2 ?p2 ?o2 .
    }
    WHERE {
      {
        GRAPH <http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3> {
          ?s1 ?p1 ?o1 .
        }
      }
      UNION
      {
        GRAPH <http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3> {
          ?bioportalEntity owl:sameAs ?goEntity .
        }
        GRAPH <http://bio2rdf.org/go_resource:bio2rdf.dataset.go.R3> {
          ?goEntity ?p ?o .
        }
      }
      UNION
      {
        GRAPH <http://bio2rdf.org/go_resource:bio2rdf.dataset.go.R3> {
          ?s2 ?p2 ?o2 .
          FILTER NOT EXISTS {
            GRAPH <http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3> {
              ?anyBioportalEntity owl:sameAs ?s2 .
            }
          }
        }
      }
    }
    """

    output_file = 'bioportal_GO_dataset_1.ttl'
    
    executor = SPARQLQueryExecutor(endpoint_url, base_query, output_file)
    executor.fetch_and_write()
    logging.info("Data has been written to bioportal_GO_dataset_1.ttl successfully.")
