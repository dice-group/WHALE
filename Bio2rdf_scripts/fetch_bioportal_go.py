import logging
import time
import argparse
import json
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.error import HTTPError
from tqdm import tqdm

class SPARQLQueryExecutor:
    def __init__(self, endpoint_url, limit=100000, max_attempts=5):
        """
        Initializes the SPARQLQueryExecutor with the given parameters.

        :param endpoint_url: The SPARQL endpoint URL.
        :param limit: The number of triples to fetch per query.
        :param max_attempts: The maximum number of retry attempts for failed queries.
        """
        self.endpoint_url = endpoint_url
        self.limit = limit
        self.max_attempts = max_attempts
        self.sparql = SPARQLWrapper(endpoint_url)
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
                self.sparql.setReturnFormat(JSON)
                response = self.sparql.query().convert()
                return response
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

    def fetch_and_write_individual_graphs(self, graphs_and_files):
        """
        Fetches data from individual graphs and writes the triples to separate output files in N-Triples format.

        :param graphs_and_files: A list of tuples containing graph URIs and output file names.
        """
        for graph_uri, output_file in graphs_and_files:
            logging.info(f"Processing graph {graph_uri} into file {output_file}")
            offset = 0
            results_obtained = True
            total_triples = 0
            with open(output_file, 'a', encoding='utf-8') as f_out:
                pbar = tqdm(desc=f"Triples fetched from {graph_uri}", unit="triples")
                while results_obtained:
                    query = f"""
                    SELECT ?s ?p ?o
                    WHERE {{
                        GRAPH {graph_uri} {{
                            ?s ?p ?o .
                        }}
                    }}
                    LIMIT {self.limit}
                    OFFSET {offset}
                    """
                    logging.info(f"Fetching results with OFFSET {offset} and LIMIT {self.limit}")
                    data = self.execute_safe_query(query)
                    if data and 'results' in data and 'bindings' in data['results']:
                        bindings = data['results']['bindings']
                        if bindings:
                            for result in bindings:
                                s = result['s']['value']
                                p = result['p']['value']
                                o = result['o']['value']
                                o_type = result['o']['type']
                                if o_type == 'uri':
                                    triple = f"<{s}> <{p}> <{o}> .\n"
                                elif o_type == 'literal':
                                    # Handle literals with possible datatype and language tags
                                    if 'xml:lang' in result['o']:
                                        lang = result['o']['xml:lang']
                                        triple = f"<{s}> <{p}> \"{o}\"@{lang} .\n"
                                    elif 'datatype' in result['o']:
                                        datatype = result['o']['datatype']
                                        triple = f"<{s}> <{p}> \"{o}\"^^<{datatype}> .\n"
                                    else:
                                        triple = f"<{s}> <{p}> \"{o}\" .\n"
                                else:
                                    # Handle other types if necessary
                                    triple = f"<{s}> <{p}> \"{o}\" .\n"
                                f_out.write(triple)
                                total_triples += 1
                                pbar.update(1)
                            offset += self.limit
                        else:
                            logging.info("No more triples found.")
                            results_obtained = False
                    else:
                        logging.info("No more data to fetch.")
                        results_obtained = False
                pbar.close()
                logging.info(f"Total triples fetched from {graph_uri}: {total_triples}")

    def fetch_and_write(self, base_query, output_file):
        """
        Fetches data in batches and writes the triples to the output file in N-Triples format.

        :param base_query: The base SPARQL query without LIMIT and OFFSET.
        :param output_file: The path to the output file where results will be written.
        """
        results_obtained = True
        total_triples = 0
        offset = 0
        with open(output_file, 'a', encoding='utf-8') as f_out:
            pbar = tqdm(desc="Triples fetched", unit="triples")
            while results_obtained:
                query = f"""
                SELECT ?s1 ?p1 ?o1 ?bioportalEntity ?p ?o ?s2 ?p2 ?o2
                WHERE {{
                  {{
                    GRAPH <http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3> {{
                      ?s1 ?p1 ?o1 .
                    }}
                  }}
                  UNION
                  {{
                    GRAPH <http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3> {{
                      ?bioportalEntity <http://www.w3.org/2002/07/owl#sameAs> ?goEntity .
                    }}
                    GRAPH <http://bio2rdf.org/go_resource:bio2rdf.dataset.go.R3> {{
                      ?goEntity ?p ?o .
                    }}
                  }}
                  UNION
                  {{
                    GRAPH <http://bio2rdf.org/go_resource:bio2rdf.dataset.go.R3> {{
                      ?s2 ?p2 ?o2 .
                      FILTER NOT EXISTS {{
                        GRAPH <http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3> {{
                          ?anyBioportalEntity <http://www.w3.org/2002/07/owl#sameAs> ?s2 .
                        }}
                      }}
                    }}
                  }}
                }}
                LIMIT {self.limit}
                OFFSET {offset}
                """
                logging.info(f"Fetching results with OFFSET {offset} and LIMIT {self.limit}")
                data = self.execute_safe_query(query)
                if data and 'results' in data and 'bindings' in data['results']:
                    bindings = data['results']['bindings']
                    if bindings:
                        for result in bindings:
                            # Process first pattern (?s1 ?p1 ?o1)
                            if 's1' in result and 'p1' in result and 'o1' in result:
                                s = result['s1']['value']
                                p = result['p1']['value']
                                o = result['o1']['value']
                                o_type = result['o1']['type']
                                triple = self.format_triple(s, p, o, o_type, result['o1'])
                                f_out.write(triple)
                                total_triples += 1
                                pbar.update(1)
                            # Process second pattern (?bioportalEntity ?p ?o)
                            if 'bioportalEntity' in result and 'p' in result and 'o' in result:
                                s = result['bioportalEntity']['value']
                                p = result['p']['value']
                                o = result['o']['value']
                                o_type = result['o']['type']
                                triple = self.format_triple(s, p, o, o_type, result['o'])
                                f_out.write(triple)
                                total_triples += 1
                                pbar.update(1)
                            # Process third pattern (?s2 ?p2 ?o2)
                            if 's2' in result and 'p2' in result and 'o2' in result:
                                s = result['s2']['value']
                                p = result['p2']['value']
                                o = result['o2']['value']
                                o_type = result['o2']['type']
                                triple = self.format_triple(s, p, o, o_type, result['o2'])
                                f_out.write(triple)
                                total_triples += 1
                                pbar.update(1)
                        offset += self.limit
                    else:
                        logging.info("No more triples found.")
                        results_obtained = False
                else:
                    logging.info("No more data to fetch.")
                    results_obtained = False
            pbar.close()
            logging.info(f"Total triples fetched: {total_triples}")

    def format_triple(self, s, p, o, o_type, o_info):
        """
        Formats a triple in N-Triples format.

        :param s: Subject URI.
        :param p: Predicate URI.
        :param o: Object value.
        :param o_type: Type of the object ('uri', 'literal', etc.).
        :param o_info: Additional information about the object (datatype, language).
        :return: A formatted triple string.
        """
        if o_type == 'uri':
            triple = f"<{s}> <{p}> <{o}> .\n"
        elif o_type == 'literal':
            # Handle literals with possible datatype and language tags
            o_escaped = o.replace('"', '\\"')
            if 'xml:lang' in o_info:
                lang = o_info['xml:lang']
                triple = f"<{s}> <{p}> \"{o_escaped}\"@{lang} .\n"
            elif 'datatype' in o_info:
                datatype = o_info['datatype']
                triple = f"<{s}> <{p}> \"{o_escaped}\"^^<{datatype}> .\n"
            else:
                triple = f"<{s}> <{p}> \"{o_escaped}\" .\n"
        else:
            # Handle other types if necessary
            o_escaped = o.replace('"', '\\"')
            triple = f"<{s}> <{p}> \"{o_escaped}\" .\n"
        return triple

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="SPARQL Query Executor")
    parser.add_argument('--individual', action='store_true', help='Extract individual graphs into separate files.')
    args = parser.parse_args()

    # SPARQL endpoint
    endpoint_url = "https://bio2rdf.org/sparql"

    executor = SPARQLQueryExecutor(endpoint_url)

    if args.individual:
        # Extract individual graphs into separate files
        graphs_and_files = [
            ("<http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3>", "bioportal.nt"),
            ("<http://bio2rdf.org/go_resource:bio2rdf.dataset.go.R3>", "go.nt")
        ]
        executor.fetch_and_write_individual_graphs(graphs_and_files)
        logging.info("Individual graphs have been extracted successfully.")
    else:
        # Base query without LIMIT and OFFSET
        base_query = """
        """
        output_file = 'bioportal_GO_dataset_1.ttl'
        executor.fetch_and_write(base_query, output_file)
        logging.info(f"Data has been written to {output_file} successfully.")
