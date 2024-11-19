import logging
import time
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.error import HTTPError
from tqdm import tqdm

class SPARQLQueryExecutor:
    def __init__(self, endpoint_url, base_query, output_file, limit=10000, max_attempts=5):
        """
        Initializes the SPARQLQueryExecutor with the given parameters.

        :param endpoint_url: The SPARQL endpoint URL.
        :param base_query: The base SPARQL SELECT query without LIMIT and OFFSET.
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
        self.sparql.setReturnFormat(JSON)
        self.sparql.setTimeout(600)  # Set timeout to 10 minutes
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

    def execute_safe_query(self, query):
        """
        Executes the SPARQL query with retry logic.

        :param query: The complete SPARQL query with LIMIT and OFFSET.
        :return: Query results as a JSON object if successful, else None.
        """
        base_delay = 1  # Base delay in seconds for retries
        for attempt in range(1, self.max_attempts + 1):
            try:
                self.sparql.setQuery(query)
                results = self.sparql.queryAndConvert()
                return results
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

    def format_term(self, term):
        """
        Formats a term (subject, predicate, or object) into N-Triples format.

        :param term: The term dictionary from the SPARQL JSON result.
        :return: The term formatted as a string in N-Triples format.
        """
        if term['type'] == 'uri':
            return f"<{term['value']}>"
        elif term['type'] == 'bnode':
            return f"_:b{term['value']}"
        elif term['type'] == 'literal':
            # Escape special characters in literals
            value = term['value'].replace('\\', '\\\\').replace('"', '\\"')
            if 'xml:lang' in term:
                return f"\"{value}\"@{term['xml:lang']}"
            elif 'datatype' in term:
                return f"\"{value}\"^^<{term['datatype']}>"
            else:
                return f"\"{value}\""
        else:
            logging.error(f"Unknown term type: {term['type']}")
            return ""

    def format_triple(self, s, p, o):
        """
        Formats a triple into N-Triples format.

        :param s: The subject term.
        :param p: The predicate term.
        :param o: The object term.
        :return: The triple formatted as a string in N-Triples format.
        """
        s_formatted = self.format_term(s)
        p_formatted = self.format_term(p)
        o_formatted = self.format_term(o)
        return f"{s_formatted} {p_formatted} {o_formatted} ."

    def fetch_and_write(self):
        """
        Fetches data in batches and writes the triples to the output file in N-Triples format.
        """
        results_obtained = True
        total_triples = 0
        with open(self.output_file, 'a', encoding='utf-8') as f_out:
            pbar = tqdm(desc="Triples fetched", unit="triples")
            while results_obtained:
                query = self._construct_query()
                logging.info(f"Fetching results with OFFSET {self.offset} and LIMIT {self.limit}")
                data = self.execute_safe_query(query)
                if data:
                    try:
                        bindings = data['results']['bindings']
                        if bindings:
                            for binding in bindings:
                                s = binding['s']
                                p = binding['p']
                                o = binding['o']
                                triple = self.format_triple(s, p, o)
                                f_out.write(triple + '\n')
                                total_triples += 1
                                pbar.update(1)
                            logging.info(f"Fetched {len(bindings)} triples")
                            self.offset += self.limit
                        else:
                            logging.info("No more triples found.")
                            results_obtained = False
                    except Exception as e:
                        logging.error(f"Error processing data: {e}")
                        results_obtained = False
                else:
                    logging.info("No more data to fetch.")
                    results_obtained = False
            pbar.close()
            logging.info(f"Total triples fetched: {total_triples}")

if __name__ == "__main__":
    # SPARQL endpoint
    endpoint_url = "https://bio2rdf.org/sparql"

    # Create affymetrix_sparql.nt
    base_query_affymetrix = """
    SELECT ?s ?p ?o
    WHERE {
      GRAPH <http://bio2rdf.org/affymetrix_resource:bio2rdf.dataset.affymetrix.R3> {
        ?s ?p ?o .
      }
    }
    """
    output_file_affymetrix = '/scratch/hpc-prf-whale/bio2rdf/raw_data/affymetrix_sparql.nt'
    executor_affymetrix = SPARQLQueryExecutor(endpoint_url, base_query_affymetrix, output_file_affymetrix)
    executor_affymetrix.fetch_and_write()
    logging.info("Data has been written to affymetrix_sparql.nt successfully.")

    # Create sgd_sparql.nt
    base_query_sgd = """
    SELECT ?s ?p ?o
    WHERE {
      GRAPH <http://bio2rdf.org/sgd_resource:bio2rdf.dataset.sgd.R3> {
        ?s ?p ?o .
      }
    }
    """
    output_file_sgd = '/scratch/hpc-prf-whale/bio2rdf/raw_data/sgd_sparql.nt'
    executor_sgd = SPARQLQueryExecutor(endpoint_url, base_query_sgd, output_file_sgd)
    executor_sgd.fetch_and_write()
    logging.info("Data has been written to sgd_sparql.nt successfully.")

    # Create bioportal_sgd_affymetrix_dataset_1.nt
    base_query_combined = """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>

    SELECT ?s ?p ?o
    WHERE {
      {
        GRAPH <http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3> {
          ?s ?p ?o .
        }
      }
      UNION
      {
        GRAPH <http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3> {
          ?bioportalEntity owl:sameAs ?sgdEntity .
        }
        GRAPH <http://bio2rdf.org/sgd_resource:bio2rdf.dataset.sgd.R3> {
          ?sgdEntity ?p ?o .
        }
        BIND(?bioportalEntity AS ?s)
      }
      UNION
      {
        GRAPH <http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3> {
          ?bioportalEntity owl:sameAs ?affymetrixEntity .
        }
        GRAPH <http://bio2rdf.org/affymetrix_resource:bio2rdf.dataset.affymetrix.R3> {
          ?affymetrixEntity ?p ?o .
        }
        BIND(?bioportalEntity AS ?s)
      }
      UNION
      {
        GRAPH <http://bio2rdf.org/sgd_resource:bio2rdf.dataset.sgd.R3> {
          ?sgdEntity owl:sameAs ?affymetrixEntity .
        }
        GRAPH <http://bio2rdf.org/affymetrix_resource:bio2rdf.dataset.affymetrix.R3> {
          ?affymetrixEntity ?p ?o .
        }
        BIND(?sgdEntity AS ?s)
      }
      UNION
      {
        GRAPH <http://bio2rdf.org/sgd_resource:bio2rdf.dataset.sgd.R3> {
          ?s ?p ?o .
          FILTER NOT EXISTS {
            GRAPH <http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3> {
              ?anyBioportalEntity owl:sameAs ?s .
            }
          }
        }
      }
      UNION
      {
        GRAPH <http://bio2rdf.org/affymetrix_resource:bio2rdf.dataset.affymetrix.R3> {
          ?s ?p ?o .
          FILTER NOT EXISTS {
            {
              GRAPH <http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3> {
                ?anyBioportalEntity owl:sameAs ?s .
              }
            }
            UNION
            {
              GRAPH <http://bio2rdf.org/sgd_resource:bio2rdf.dataset.sgd.R3> {
                ?anySgdEntity owl:sameAs ?s .
              }
            }
          }
        }
      }
    }
    """
    output_file_combined = '/scratch/hpc-prf-whale/bio2rdf/raw_data/bioportal_sgd_affymetrix_dataset_1.nt'
    executor_combined = SPARQLQueryExecutor(endpoint_url, base_query_combined, output_file_combined)
    executor_combined.fetch_and_write()
    logging.info("Data has been written to bioportal_sgd_affymetrix_dataset_1.nt successfully.")
