import rdflib
import os
import logging
import pickle
from tqdm import tqdm
from SPARQLWrapper import SPARQLWrapper, JSON
import argparse
import json
from urllib.error import HTTPError
import time
import pandas as pd
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',) #filename='processing.log', filemode='a')

class RDFProcessor:
    def __init__(self, base_directory, config_output_path, output_path):
        self.base_directory = base_directory
        self.linked_classes = []
        current_working_dir = os.getcwd()

        # Check if the current working directory is already inside 'WDC_scripts/linking_scripts/limes'
        if current_working_dir.endswith(os.path.join('WDC_scripts', 'linking_scripts', 'limes')):
            class_mapping_file_relative_path = os.path.join('output_files', 'WDC_wikidata_verynear.nt')
            namespace_file_relative_path = os.path.join('raw_data', 'all_prefix.csv')
        else:
            class_mapping_file_relative_path = os.path.join('WDC_scripts', 'linking_scripts', 'limes', 'output_files', 'WDC_wikidata_verynear.nt')
            namespace_file_relative_path = os.path.join('WDC_scripts', 'linking_scripts', 'limes', 'raw_data', 'all_prefix.csv')

        # Get the absolute paths
        class_mapping_file_absolute_path = os.path.abspath(class_mapping_file_relative_path)
        namespace_file_absolute_path = os.path.abspath(namespace_file_relative_path)

        # Load the class mapping and namespace files using the absolute paths
        self.load_classes(class_mapping_file_absolute_path)
        self.namespace_to_prefix = self.load_namespaces(namespace_file_absolute_path)

        self.sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        self.total_count_wiki = 0
        self.output_dir_path = output_path
        self.config_dir_path = config_output_path

    def load_classes(self, filename):
        try:
            with open(filename, 'r', encoding="utf-8") as file:
                for line in file:
                    parts = line.split()
                    if len(parts) > 1:
                        local_class = parts[0].strip('<>')
                        wikidata_class = parts[2].strip('<>')
                        self.linked_classes.append((local_class, wikidata_class))
            logging.info("Classes loaded successfully!")
        except Exception as e:
            logging.error("Failed to load Classes: " + str(e))

    def load_namespaces(self, filename):
        df = pd.read_csv(filename, header=None, names=['prefix', 'namespace'])
        logging.info("Namespaces loaded successfully!")
        return dict(zip(df['namespace'], df['prefix']))
    
    def replace_with_prefix(self, iri):
        for namespace, prefix in self.namespace_to_prefix.items():
            if iri.startswith(namespace):
                self.prefix_namespace_dict[namespace] = prefix
                return iri.replace(namespace, prefix + ':')
        assert "/" not in iri
        return iri

    def execute_safe_query(self):
        base_delay = 1 
        for attempt in range(5):
            try:
                response = self.sparql.query().response
                raw_data = response.read().decode('utf-8')
                results = json.loads(raw_data)
                return results
            except json.JSONDecodeError as e:
                logging.error(f"JSON decode failed on attempt {attempt + 1}: {e}")
                if "Invalid control character" in str(e):
                    continue
            except HTTPError as e:
                logging.error(f"HTTP error on attempt {attempt + 1}: {e}")
                if e.code == 429:  # Handle rate limiting
                    sleep_time = base_delay * 2 ** attempt
                    logging.info(f"Rate limit exceeded. Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    raise e
            except Exception as e:
                logging.error(f"Unhandled exception: {e}")
                return {}

    def process_directory(self, directory, class_set):
        files = [f for f in os.listdir(directory) if f.endswith('.txt') or f.endswith('.nt')]
        for filename in tqdm(files, desc=f"Processing files in {directory}"):
            self.process_file(os.path.join(directory, filename), class_set)

    def process_file(self, file_path, class_set):
        g = rdflib.ConjunctiveGraph()
        try:
            logging.info(f"Loading local graph {os.path.basename(file_path)}...")
            g.parse(file_path, format='nquads')
            logging.info(f"Loaded successfully: {file_path}")
        except Exception as e:
            logging.error(f"Failed to load RDF file {file_path}: {str(e)}")
            return
        
        self.class_properties_local = {}
        self.coverage_dict_local = {}

        self.class_properties_wiki = {}
        self.coverage_dict_wiki = {}

        self.prefix_namespace_dict = {}
        self.prefixed_iri_dict = {}

        self.nested_props = {}

        for idx, class_sets in enumerate(tqdm(class_set, desc="Processing classes")):
            local_class, wiki_class = class_sets
            total_count, properties_info, nested_prop = self.query_graph(g, local_class)
            if total_count > 0:
                sorted_props = sorted(properties_info.items(), key=lambda x: x[1], reverse=True)
                logging.debug(f"Found instances for class {local_class}")
                threshold = 0.7
                filtered_properties = []
                for prop, literal_count in sorted_props:
                    coverage = literal_count / total_count
                    if coverage > threshold:
                        logging.debug(f"Coverage above {threshold} for {prop} ...")
                        filtered_properties.append(prop)
                if not filtered_properties:
                    logging.debug(f"No property having coverage above {threshold} ...")
                    top_properties = [prop for prop, _ in sorted_props[:5]]
                    logging.debug(f"Took top {len(top_properties)} properties ...")
                    # Update local class-properties dictionary
                    self.class_properties_local[local_class] = top_properties
                else:
                    logging.debug(f"Found {len(filtered_properties)} properties out of {len(sorted_props)}")
                    self.class_properties_local[local_class] = filtered_properties
                coverage_dict = {}
                for prop, count in properties_info.items():
                    coverage_dict[prop] = count / total_count
                # Update coverage dictionary
                self.coverage_dict_local[local_class] = coverage_dict

                # Update nested properties dictionary
                self.nested_props = nested_prop
                
                # Add to prefixed dictionary
                prefixed_class_iri = self.replace_with_prefix(iri=local_class)
                prefixed_properties = {self.replace_with_prefix(prop) for prop in filtered_properties}
                self.prefixed_iri_dict[prefixed_class_iri] = prefixed_properties

            if local_class in self.coverage_dict_local:
                # WIKIDATA
                total_count, properties_info = self.query_wikidata(wiki_class)
                sorted_props = sorted(properties_info.items(), key=lambda x: x[1], reverse=True)
                if total_count > 0:
                    logging.debug(f"Found instances for class {wiki_class}")
                    threshold = 0.4
                    filtered_properties = []
                    for prop, literal_count in sorted_props:
                        coverage = literal_count / total_count
                        if coverage > threshold:
                            logging.debug(f"Coverage above {threshold} for {prop} ...")
                            filtered_properties.append(prop)                    
                    if not filtered_properties:
                        logging.debug(f"No property having coverage above {threshold} ...")
                        top_properties = [prop for prop, _ in sorted_props[:5]]
                        logging.debug(f"Took top {len(top_properties)} properties ...")
                        self.class_properties_wiki[wiki_class] = top_properties
                    else:
                        logging.debug(f"Found {len(filtered_properties)} properties out of {len(sorted_props)}")
                        self.class_properties_wiki[wiki_class] = filtered_properties
                    coverage_dict = {}
                    for prop, count in properties_info.items():
                        coverage_dict[prop] = count / total_count
                    self.coverage_dict_wiki[wiki_class] = coverage_dict

                    self.total_count_wiki = total_count

                    # Add to prefixed dictionary
                    prefixed_class_iri = self.replace_with_prefix(iri=wiki_class)
                    prefixed_properties = {self.replace_with_prefix(prop) for prop in filtered_properties}
                    prefixed_properties.add(self.replace_with_prefix("http://www.wikidata.org/prop/direct/P31"))
                    self.prefixed_iri_dict[prefixed_class_iri] = prefixed_properties

            comparison_result_local, self.prefixed_iri_dict = self.compare_dictionaries(self.class_properties_local, self.prefixed_iri_dict)
            comparison_result_wiki, self.prefixed_iri_dict = self.compare_dictionaries(self.class_properties_wiki, self.prefixed_iri_dict)

            if comparison_result_local and comparison_result_wiki:
                logging.debug("All keys and properties from the original dictionary are present in the updated dictionary.")
            else:
                logging.error("Some keys or properties from the original dictionary are missing in the updated dictionary.")
                
            self.create_xml(local_class, wiki_class, idx, file_path)
        
        if logging.getLevelName(logging.getLogger().getEffectiveLevel()) == "DEBUG":
            self.save_dictionaries(file_path) # For debugging
        logging.info(f"Processed and saved data for {file_path}")

    def create_xml(self, local_class, wiki_class, idx, file_path):
        limes = ET.Element("LIMES")

        # Add owl prefix
        prefix_elem = ET.SubElement(limes, 'PREFIX')
        namespace_elem = ET.SubElement(prefix_elem, 'NAMESPACE')
        namespace_elem.text = 'https://www.w3.org/2002/07/owl#'
        label_elem = ET.SubElement(prefix_elem, 'LABEL')
        label_elem.text = 'owl'

        # Add PREFIX section
        for namespace, prefix in self.prefix_namespace_dict.items():
            prefix_elem = ET.SubElement(limes, 'PREFIX')
            namespace_elem = ET.SubElement(prefix_elem, 'NAMESPACE')
            namespace_elem.text = namespace
            label_elem = ET.SubElement(prefix_elem, 'LABEL')
            label_elem.text = prefix
        if local_class in self.class_properties_local.keys() and wiki_class in self.class_properties_wiki.keys():
            prefixed_class_iri_local = self.replace_with_prefix(iri=local_class)        
            filtered_properties = self.class_properties_local[local_class]
            prefixed_properties_local = set()

            for prop in filtered_properties:
                prefixed_prop = self.replace_with_prefix(prop)
                found_in_nested = False

                for nested_prop_set in self.nested_props.values():
                    if prefixed_prop in nested_prop_set:
                        found_in_nested = True
                        break

                if not found_in_nested:
                    prefixed_properties_local.add(prefixed_prop)

            # SOURCE section
            source = ET.SubElement(limes, 'SOURCE')
            ET.SubElement(source, 'ID').text = 'WDC_labels'
            # relative_path = os.path.join(*file_path.split(os.sep)[5:])
            # linux_path = relative_path.replace("\\", "/")
            ET.SubElement(source, 'ENDPOINT').text = file_path # WINDOWS: linux_path
            ET.SubElement(source, 'VAR').text = '?x'
            ET.SubElement(source, 'PAGESIZE').text = '-1'
            ET.SubElement(source, 'RESTRICTION').text = f'?x a {prefixed_class_iri_local}'
            for prop in prefixed_properties_local:
                property_elem = ET.SubElement(source, 'PROPERTY')
                property_elem.text = f'{prop} AS lowercase'

            for interp, prop_set in self.nested_props.items():
                for prop in prop_set:
                    if any(prop in props for props in self.prefixed_iri_dict.values()):
                        property_elem = ET.SubElement(source, 'PROPERTY')
                        property_elem.text = f'{interp}/{prop} AS lowercase'

            
            ET.SubElement(source, 'TYPE').text = 'N-TRIPLE'

            prefixed_class_iri_wiki = self.replace_with_prefix(iri=wiki_class)        
            filtered_properties = self.class_properties_wiki[wiki_class]
            prefixed_properties_wiki = [self.replace_with_prefix(prop) for prop in filtered_properties]

            # TARGET section
            target = ET.SubElement(limes, 'TARGET')
            ET.SubElement(target, 'ID').text = 'wikidata'
            ET.SubElement(target, 'ENDPOINT').text = 'https://query.wikidata.org/sparql'
            ET.SubElement(target, 'VAR').text = '?y'

            if self.total_count_wiki > 15:
                ET.SubElement(target, 'PAGESIZE').text = '100000' 
            else:
                ET.SubElement(target, 'PAGESIZE').text = '-1'
            ET.SubElement(target, 'RESTRICTION').text = f'?y wdt:P31 {prefixed_class_iri_wiki}'
            for prop in prefixed_properties_wiki:
                property_elem = ET.SubElement(target, 'PROPERTY')
                property_elem.text = f'{prop} AS lowercase'

            
            # MLALGORITHM section
            mlalgorithm = ET.SubElement(limes, 'MLALGORITHM')
            ET.SubElement(mlalgorithm, 'NAME').text = 'wombat simple'
            ET.SubElement(mlalgorithm, 'TYPE').text = 'unsupervised'
            parameter = ET.SubElement(mlalgorithm, 'PARAMETER')
            ET.SubElement(parameter, 'NAME').text = 'max execution time in minutes'
            ET.SubElement(parameter, 'VALUE').text = '60'
            parameter = ET.SubElement(mlalgorithm, 'PARAMETER')
            ET.SubElement(parameter, 'NAME').text = 'atomic measures'
            ET.SubElement(parameter, 'VALUE').text = 'jaccard, trigrams, cosine, qgrams'
            ET.SubElement(limes, 'EXPLAIN_LS').text = 'English'

            accept_filename = f'{os.path.basename(file_path)}_{idx}.nt'
            review_filename = f'{os.path.basename(file_path)}_{idx}_near.nt'
            # ACCEPTANCE section
            acceptance = ET.SubElement(limes, 'ACCEPTANCE')
            ET.SubElement(acceptance, 'THRESHOLD').text = '0.7'
            ET.SubElement(acceptance, 'FILE').text = os.path.join(self.output_dir_path, accept_filename)
            ET.SubElement(acceptance, 'RELATION').text = 'owl:sameAs'

            # REVIEW section
            review = ET.SubElement(limes, 'REVIEW')
            ET.SubElement(review, 'THRESHOLD').text = '0.4'
            ET.SubElement(review, 'FILE').text = os.path.join(self.output_dir_path, review_filename)
            ET.SubElement(review, 'RELATION').text = 'owl:near'

            # EXECUTION section
            execution = ET.SubElement(limes, 'EXECUTION')
            ET.SubElement(execution, 'REWRITER').text = 'default'
            ET.SubElement(execution, 'PLANNER').text = 'default'
            ET.SubElement(execution, 'ENGINE').text = 'default'
            ET.SubElement(execution, 'OPTIMIZATION_TIME').text = '1000'
            ET.SubElement(execution, 'EXPECTED_SELECTIVITY').text = '0.5'
            ET.SubElement(limes, 'OUTPUT').text = 'NT'

            self.save_pretty_xml(limes, filename=f'config_{os.path.basename(file_path)}_{idx}.xml')

    def save_pretty_xml(self, element, filename):
        rough_string = ET.tostring(element, 'utf-8')
        reparsed = minidom.parseString(rough_string)
        pretty_xml_as_string = reparsed.toprettyxml(indent="  ")
        pretty_xml_as_string = '\n'.join(pretty_xml_as_string.split('\n')[1:])

        if not os.path.exists(self.config_dir_path):
            os.makedirs(self.config_dir_path)
            
        # Add XML declaration and DOCTYPE
        with open(os.path.join(self.config_dir_path, filename), 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n')
            f.write('<!DOCTYPE LIMES SYSTEM "limes.dtd">\n')
            f.write(pretty_xml_as_string) 

    def query_graph(self, graph, class_uri):
        total_query = f"""
        SELECT (COUNT(DISTINCT ?i) AS ?orderCount)
        WHERE {{
            ?i a <{class_uri}> .
        }}
        """
        total_results = graph.query(total_query)
        total_count = 0
        for result in total_results:
            total_count = int(result.orderCount)
        # total_count = sum(int(result.orderCount) for result in total_results)
        if total_count > 0:
            literal_query = f"""
            SELECT ?intermediateProperty ?finalProperty (COUNT(DISTINCT ?instance) AS ?literalCount)
            WHERE {{
            ?instance a <{class_uri}> .
            {{
                ?instance ?finalProperty ?literalValue .
                FILTER(isLiteral(?literalValue))
            }}
            UNION {{
                ?instance ?intermediateProperty ?connectedResource .
                ?connectedResource ?finalProperty ?literalValue .
                FILTER(isLiteral(?literalValue))
            }}
            }}
            GROUP BY ?finalProperty ?intermediateProperty
            """
            nested_prop_dict = {}
            properties_info = {}
            literal_results = graph.query(literal_query)
            for result in literal_results:
                if result.intermediateProperty:
                    intermediate_prop = self.replace_with_prefix(str(result.intermediateProperty))
                    if intermediate_prop not in nested_prop_dict:
                        nested_prop_dict[intermediate_prop] = set()
                    nested_prop_dict[intermediate_prop].add(self.replace_with_prefix(str(result.finalProperty)))
                properties_info[str(result.finalProperty)] = int(result.literalCount)
            return total_count, properties_info, nested_prop_dict
        else:
            return 0, {}, {}
        
    def query_wikidata(self, wiki_class):
        total_query_wiki = f"""
                SELECT (COUNT(DISTINCT ?i) AS ?orderCount)
                WHERE {{
                    ?i wdt:P31 wd:{wiki_class.split('/')[-1]} .
                }}
                """
        self.sparql.setQuery(total_query_wiki)
        self.sparql.setReturnFormat(JSON)
        results = self.execute_safe_query()
        total_count = int(results["results"]["bindings"][0]["orderCount"]["value"]) if results["results"]["bindings"] else 0

        literal_query_wiki = f"""
                SELECT ?property (COUNT(DISTINCT ?instance) AS ?literalCount)
                WHERE {{
                    ?instance wdt:P31 wd:{wiki_class.split('/')[-1]} .
                    ?instance ?property ?value .
                    FILTER(isLiteral(?value))
                }}
                GROUP BY ?property
                """
        self.sparql.setQuery(literal_query_wiki)
        self.sparql.setTimeout(600)
        self.sparql.setReturnFormat(JSON)
        results = self.execute_safe_query()
        properties_info = {result['property']['value']: int(result['literalCount']['value']) for result in results["results"]["bindings"]}
        return total_count,properties_info     
            
    def compare_dictionaries(self, original_dict, updated_dict):
        for class_iri, properties in original_dict.items():
            prefixed_class_iri = self.replace_with_prefix(class_iri)
            if prefixed_class_iri not in updated_dict:
                updated_dict[prefixed_class_iri] = set()
                logging.warning(f"Missing class IRI in updated dictionary: {prefixed_class_iri}")
            
            for prop in properties:
                prefixed_prop = self.replace_with_prefix(prop)
                if prefixed_prop not in updated_dict[prefixed_class_iri]:
                    updated_dict[prefixed_class_iri].add(prefixed_prop)
                    logging.warning(f"Missing property IRI in updated dictionary: {prefixed_prop} for class {prefixed_class_iri}")
        return True, updated_dict

    def save_dictionaries(self, file_path):
        dir_name = os.path.dirname(file_path)
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        dict_path = os.path.join(dir_name, base_name)
        if not os.path.exists(dict_path):
            os.makedirs(dict_path)
        
        with open(os.path.join(dict_path, 'class_properties_local.pkl'), 'wb') as f:
            pickle.dump(self.class_properties_local, f)
        with open(os.path.join(dict_path, 'class_properties_wiki.pkl'), 'wb') as f:
            pickle.dump(self.class_properties_wiki, f)

        with open(os.path.join(dict_path, 'coverage_local.pkl'), 'wb') as f:
            pickle.dump(self.coverage_dict_local, f)
        with open(os.path.join(dict_path, 'coverage_wiki.pkl'), 'wb') as f:
            pickle.dump(self.coverage_dict_wiki, f)

        with open(os.path.join(dict_path, 'prefix_namespace_dict.pkl'), 'wb') as f:
            pickle.dump(self.prefix_namespace_dict, f)
        with open(os.path.join(dict_path, 'prefixed_iri_dict.pkl'), 'wb') as f:
            pickle.dump(self.prefixed_iri_dict, f)

        with open(os.path.join(dict_path, 'nested_properties.pkl'), 'wb') as f:
            pickle.dump(self.nested_props, f)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Process RDF data based on user input')
    parser.add_argument('action', choices=['all', 'wikidata', 'specific'],
                        help='Specify "all" to process all subdirectories, "wikidata" for Wikidata endpoint, or "specific" for a specific directory or file.')
    parser.add_argument('-p', '--path',
                        help='Specify the path to an input directory or input KG file when using "specific".')
    parser.add_argument('-c', '--config_output_path',
                        help='Specify the path to a directory for storing the config files.', required=True)
    parser.add_argument('-o', '--output_path',
                        help='Specify the path to a directory for output of config files.', required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    base_directory = "WDC_scripts\dataset_extraction_scripts\domain_specific\linking_dataset" # '/scratch/hpc-prf-dsg/WHALE-data/domain_specific/linking_dataset/'
    
    # Check if base_directory exists, if not create it
    if not os.path.exists(base_directory):
        os.makedirs(base_directory)
        logging.info(f"Created directory: {base_directory}")
    else:
        logging.info(f"Directory already exists: {base_directory}")
        
    rdf_processor = RDFProcessor(base_directory, args.config_output_path, args.output_path)
    
    if args.action == 'all':
        rdf_subdirectories = [os.path.join(rdf_processor.base_directory, d) for d in os.listdir(rdf_processor.base_directory) if os.path.isdir(os.path.join(rdf_processor.base_directory, d))]
        for subdirectory in tqdm(rdf_subdirectories, desc="Processing subdirectories"):
            rdf_processor.process_directory(subdirectory, rdf_processor.linked_classes)
    elif args.action == 'specific':
        if args.path:
            if os.path.isdir(args.path):
                rdf_processor.process_directory(args.path, rdf_processor.linked_classes)
            elif args.path.endswith('.txt') or args.path.endswith('.nt'):
                rdf_processor.process_file(args.path, rdf_processor.linked_classes)
            else:
                logging.error("Specified path is not a directory or .txt file.")
        else:
            logging.error("Path must be specified for the 'specific' action.")
    else:
        logging.error("Invalid action specified.")
