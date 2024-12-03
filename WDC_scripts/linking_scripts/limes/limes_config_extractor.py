import rdflib
from rdflib.namespace import split_uri
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

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',)

class RDFProcessor:
    """
    A class to process RDF data and generate configuration files for LIMES.

    Parameters
    ----------
    base_directory : str
        The base directory containing the RDF data.
    config_output_path : str
        The directory where configuration files will be stored.
    output_path : str
        The directory where output files will be stored.
    class_mapping_path : str, optional
        The path to the class mapping file. If not provided, a default path is used.
    property_mapping_path : str, optional
        The path to the property mapping file.
    target_graph_path : str, optional
        The path to the target graph file. If not provided, the Wikidata endpoint is used.

    Attributes
    ----------
    linked_classes : list of tuple
        List of tuples containing source and target classes.
    linked_properties : list of tuple
        List of tuples containing source and target properties.
    namespace_to_prefix : dict
        A dictionary mapping namespaces to prefixes.
    prefix_namespace_dict : dict
        A dictionary to keep track of prefixes used.
    prefixed_iri_dict : dict
        A dictionary mapping prefixed IRIs to properties.
    nested_props : dict
        A dictionary for nested properties.
    output_dir_path : str
        The directory where output files will be stored.
    config_dir_path : str
        The directory where configuration files will be stored.
    total_count_target : int
        Total count of target instances.
    g_target : rdflib.Graph
        The target RDF graph.

    Methods
    -------
    load_classes(filename)
        Loads the class mapping from the given file.
    load_properties(filename)
        Loads the property mapping from the given file.
    load_namespaces(filename)
        Loads namespaces and prefixes from the given file.
    replace_with_prefix(iri)
        Replaces a full IRI with its prefixed version.
    execute_safe_query()
        Executes a SPARQL query with retry logic.
    process_directory(directory, class_set)
        Processes all files in a directory.
    process_file(file_path, class_set)
        Processes a single RDF file.
    create_xml(source_class, target_class, idx, file_path, use_property_mapping)
        Creates an XML configuration file for LIMES.
    save_pretty_xml(element, filename)
        Saves an XML element to a file with pretty formatting.
    query_graph(graph, class_uri)
        Queries a graph to get instance counts and property coverage.
    query_target_graph(target_class)
        Queries the target graph or Wikidata to get instance counts and property coverage.
    compare_dictionaries(original_dict, updated_dict)
        Compares two dictionaries and updates the second with missing entries.
    save_dictionaries(file_path)
        Saves internal dictionaries to pickle files for debugging.
    """

    def __init__(self, base_directory, config_output_path, output_path, class_mapping_path=None, property_mapping_path=None, target_graph_path=None):
        """
        Initializes the RDFProcessor.

        Parameters
        ----------
        base_directory : str
            The base directory containing the RDF data.
        config_output_path : str
            The directory where configuration files will be stored.
        output_path : str
            The directory where output files will be stored.
        class_mapping_path : str, optional
            The path to the class mapping file.
        property_mapping_path : str, optional
            The path to the property mapping file.
        target_graph_path : str, optional
            The path to the target graph file.
        """
        self.base_directory = base_directory
        self.linked_classes = []
        self.linked_properties = []
        self.total_count_target = 0
        current_working_dir = os.getcwd()

        # Check if the current working directory is already inside 'WDC_scripts/linking_scripts/limes'
        if class_mapping_path:
            class_mapping_file_absolute_path = os.path.abspath(class_mapping_path)
            
            # Load the class mapping file
            self.load_classes(class_mapping_file_absolute_path)
        else:
            self.linked_classes = []

        if property_mapping_path:
            property_mapping_file_absolute_path = os.path.abspath(property_mapping_path)
            # Load the property mapping file
            self.load_properties(property_mapping_file_absolute_path)

        # Namespace file path
        if current_working_dir.endswith(os.path.join('WDC_scripts', 'linking_scripts', 'limes')):
            namespace_file_relative_path = os.path.join('raw_data', 'all_prefix.csv')
        else:
            namespace_file_relative_path = os.path.join('WDC_scripts', 'linking_scripts', 'limes', 'raw_data', 'all_prefix.csv')
        namespace_file_absolute_path = os.path.abspath(namespace_file_relative_path) # TODO: Change to general paths
        # namespace_file_absolute_path = '/home/sshivam/Work/Bio2RDF/raw_data/all_prefix.csv'

        if os.path.exists(namespace_file_absolute_path):
            self.namespace_to_prefix = self.load_namespaces(namespace_file_absolute_path)
        else:
            logging.warning(f"Namespace file not found: {namespace_file_absolute_path}. Skipping prefix loading.")
            self.namespace_to_prefix = {}

        # if current_working_dir.endswith(os.path.join('WDC_scripts', 'linking_scripts', 'limes')):
        #     namespace_file_relative_path = os.path.join('raw_data', 'all_prefix.csv')
        # else:
        #     namespace_file_relative_path = os.path.join('WDC_scripts', 'linking_scripts', 'limes', 'raw_data', 'all_prefix.csv')
        # namespace_file_absolute_path = os.path.abspath(namespace_file_relative_path)

        # Load namespaces
        # self.namespace_to_prefix = self.load_namespaces(namespace_file_absolute_path)

        self.output_dir_path = output_path
        self.config_dir_path = config_output_path

        self.target_graph_path = target_graph_path
        if target_graph_path:
            # Load the target graph from the provided path
            self.g_target = rdflib.ConjunctiveGraph()
            try:
                logging.info(f"Loading target graph from {target_graph_path}...")
                if target_graph_path.endswith('.nq'):
                    format = 'nquads'
                elif target_graph_path.endswith('.nt'):
                    format = 'nt'
                elif target_graph_path.endswith('.ttl'):
                    format = 'turtle'
                else:
                    # Optionally, you can handle other formats or raise an error
                    logging.error("Unsupported file extension for target graph. Please provide a '.nt', '.nq' or .ttl file.")
                    exit(1)
                
                self.g_target.parse(target_graph_path, format=format)
                logging.info("Target graph loaded successfully.")
            except Exception as e:
                logging.error(f"Failed to load target RDF file {target_graph_path}: {str(e)}")
                exit(1)

        else:
            # Use Wikidata endpoint
            self.sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

    def load_classes(self, filename):
        """
        Loads the class mapping from the given file.

        The file should contain lines with two IRIs representing the source and target classes.

        Parameters
        ----------
        filename : str
            The path to the class mapping file.

        Raises
        ------
        Exception
            If the file cannot be read.
        """
        try:
            with open(filename, 'r', encoding="utf-8") as file:
                for line in file:
                    parts = line.split()
                    if len(parts) > 1:
                        source_class = parts[0].strip('<>')
                        target_class = parts[2].strip('<>')
                        self.linked_classes.append((source_class, target_class))
            logging.info("Classes loaded successfully!")
        except Exception as e:
            logging.error("Failed to load Classes: " + str(e))

    def load_properties(self, filename):
        """
        Loads the property mapping from the given file.

        The file should contain lines with two IRIs representing the source and target properties.

        Parameters
        ----------
        filename : str
            The path to the property mapping file.

        Raises
        ------
        Exception
            If the file cannot be read.
        """
        try:
            with open(filename, 'r', encoding="utf-8") as file:
                for line in file:
                    parts = line.strip().split()
                    if len(parts) <= 4:
                        prop1 = parts[0].strip('<>')
                        prop2 = parts[2].strip('<>')
                        self.linked_properties.append((prop1, prop2))
            logging.info("Properties loaded successfully!")
        except Exception as e:
            logging.error("Failed to load properties: " + str(e))

    def load_namespaces(self, filename):
        """
        Loads namespaces and prefixes from the given file.

        The file should be a CSV with 'prefix' and 'namespace' columns.

        Parameters
        ----------
        filename : str
            The path to the namespaces file.

        Returns
        -------
        dict
            A dictionary mapping namespaces to prefixes.
        """
        df = pd.read_csv(filename, header=None, names=['prefix', 'namespace'])
        logging.info("Namespaces loaded successfully!")
        return dict(zip(df['namespace'], df['prefix']))
        
    def extract_namespaces(self, iri):
        try:
            namespace, local_name = split_uri(iri)
        except ValueError:
            logging.warning(f"Could not split IRI: {iri}")
            namespace = iri
            local_name = ''
        if namespace not in self.namespace_to_prefix:
            prefix = f'ns{len(self.namespace_to_prefix)}'
            self.namespace_to_prefix[namespace] = prefix
        return self.namespace_to_prefix[namespace]

    def replace_with_prefix(self, iri):
        """
        Replaces a full IRI with its prefixed version using the loaded namespaces.

        Parameters
        ----------
        iri : str
            The full IRI to be replaced.

        Returns
        -------
        str
            The prefixed IRI.

        Raises
        ------
        AssertionError
            If the IRI does not start with any known namespace and contains a '/' character.
        """
        # for namespace, prefix in self.namespace_to_prefix.items():
        #     if iri.startswith(namespace):
        #         self.prefix_namespace_dict[namespace] = prefix
        #         return iri.replace(namespace, prefix + ':')
        # if "/" in iri:
        #     logging.warning(f"IRI doesn't match any known namespace and contains '/': {iri}")
        # else:
        #     logging.warning(f"IRI doesn't match any known namespace: {iri}")
        
        # return iri

        prefix = self.extract_namespaces(iri)
        try:
            _, local_name = split_uri(iri)
        except ValueError:
            logging.warning(f"Could not split IRI: {iri}")
            local_name = ''
        return f'{prefix}:{local_name}'

    def execute_safe_query(self):
        """
        Executes a SPARQL query with retry logic for transient errors.

        Retries the query up to 5 times with exponential backoff if rate limiting occurs.

        Returns
        -------
        dict
            The JSON results of the query.

        Raises
        ------
        Exception
            If an unhandled exception occurs.
        """
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
        """
        Processes all RDF files in the given directory.

        Parameters
        ----------
        directory : str
            The directory containing RDF files.
        class_set : list of tuple, None
            A list of tuples containing source and target classes.
        """
        files = [f for f in os.listdir(directory) if f.endswith('.txt') or f.endswith('.nt') or f.endswith('.ttl')]
        for filename in tqdm(files, desc=f"Processing files in {directory}"):
            self.process_file(os.path.join(directory, filename), class_set)

    def process_file(self, file_path, class_set):
        """
        Processes a single RDF file and generates configuration files.

        Parameters
        ----------
        file_path : str
            The path to the RDF file.
        class_set : list of tuple, None
            A list of tuples containing source and target classes.
        """
        g = rdflib.ConjunctiveGraph()
        try:
            logging.info(f"Loading source graph {os.path.basename(file_path)}...")
            # Determine the format based on the file extension
            extension = file_path.lower()
            if extension.endswith('.nq'):
                format = 'nquads'
            elif extension.endswith('.nt'):
                format = 'nt'
            elif extension.endswith('.ttl'):
                format = 'turtle'
            else:
                logging.error("Unsupported file extension for source graph. Please provide a '.nt', '.nq', or '.ttl' file.")
                return
        
            g.parse(file_path, format=format)
            logging.info(f"Loaded successfully: {file_path}")
        except Exception as e:
            logging.error(f"Failed to load RDF file {file_path}: {str(e)}")
            return

        
        self.class_properties_source = {}
        self.coverage_dict_source = {}

        self.class_properties_target = {}
        self.coverage_dict_target = {}

        self.prefixed_iri_dict = {}

        self.nested_props = {}
        
        for idx, class_sets in enumerate(tqdm(class_set, desc="Processing classes")):
            source_class, target_class = class_sets
            # Check if property mapping is provided and relevant
            use_property_mapping = False
            if self.linked_properties:
                # Filter properties relevant to the current classes
                relevant_properties = [props for props in self.linked_properties if props[0] in source_class or props[1] in target_class]
                if relevant_properties:
                    use_property_mapping = True
                    # Collect properties for source and target
                    source_properties = [props[0] for props in relevant_properties]
                    target_properties = [props[1] for props in relevant_properties]

                    # Add to prefixed dictionary
                    prefixed_class_iri_source = self.replace_with_prefix(iri=source_class)
                    prefixed_properties_source = {self.replace_with_prefix(prop) for prop in source_properties}
                    self.prefixed_iri_dict[prefixed_class_iri_source] = prefixed_properties_source

                    prefixed_class_iri_target = self.replace_with_prefix(iri=target_class)
                    prefixed_properties_target = {self.replace_with_prefix(prop) for prop in target_properties}
                    if not self.target_graph_path:
                        prefixed_properties_target.add(self.replace_with_prefix("http://www.wikidata.org/prop/direct/P31"))
                    self.prefixed_iri_dict[prefixed_class_iri_target] = prefixed_properties_target

            if not use_property_mapping:
                # SOURCE
                total_count, properties_info, nested_prop = self.query_graph(g, source_class)
                if total_count > 0:
                    sorted_props = sorted(properties_info.items(), key=lambda x: x[1], reverse=True)
                    logging.debug(f"Found instances for class {source_class}")
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
                        # Update source class-properties dictionary
                        self.class_properties_source[source_class] = top_properties
                    else:
                        logging.debug(f"Found {len(filtered_properties)} properties out of {len(sorted_props)}")
                        self.class_properties_source[source_class] = filtered_properties
                    coverage_dict = {}
                    for prop, count in properties_info.items():
                        coverage_dict[prop] = count / total_count
                    # Update coverage dictionary
                    self.coverage_dict_source[source_class] = coverage_dict

                    # Update nested properties dictionary
                    self.nested_props = nested_prop
                    
                    # Add to prefixed dictionary
                    prefixed_class_iri = self.replace_with_prefix(iri=source_class)
                    prefixed_properties = {self.replace_with_prefix(prop) for prop in filtered_properties}
                    self.prefixed_iri_dict[prefixed_class_iri] = prefixed_properties

                # TARGET
                if source_class in self.coverage_dict_source:
                    total_count, properties_info, _ = self.query_target_graph(target_class)
                    sorted_props = sorted(properties_info.items(), key=lambda x: x[1], reverse=True)
                    if total_count > 0:
                        logging.debug(f"Found instances for class {target_class}")
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
                            self.class_properties_target[target_class] = top_properties
                        else:
                            logging.debug(f"Found {len(filtered_properties)} properties out of {len(sorted_props)}")
                            self.class_properties_target[target_class] = filtered_properties
                        coverage_dict = {}
                        for prop, count in properties_info.items():
                            coverage_dict[prop] = count / total_count
                        self.coverage_dict_target[target_class] = coverage_dict

                        self.total_count_target = total_count

                        # Add to prefixed dictionary
                        prefixed_class_iri = self.replace_with_prefix(iri=target_class)
                        prefixed_properties = {self.replace_with_prefix(prop) for prop in filtered_properties}
                        if not self.target_graph_path:
                            prefixed_properties.add(self.replace_with_prefix("http://www.wikidata.org/prop/direct/P31"))
                        self.prefixed_iri_dict[prefixed_class_iri] = prefixed_properties

                    # Compare dictionaries
                    comparison_result_source, self.prefixed_iri_dict = self.compare_dictionaries(self.class_properties_source, self.prefixed_iri_dict)
                    comparison_result_target, self.prefixed_iri_dict = self.compare_dictionaries(self.class_properties_target, self.prefixed_iri_dict)

                    if comparison_result_source and comparison_result_target:
                        logging.debug("All keys and properties from the original dictionary are present in the updated dictionary.")
                    else:
                        logging.error("Some keys or properties from the original dictionary are missing in the updated dictionary.")
                            
            self.create_xml(source_class, target_class, idx, file_path, use_property_mapping)
        
        if logging.getLevelName(logging.getLogger().getEffectiveLevel()) == "DEBUG":
            self.save_dictionaries(file_path)  # For debugging
        logging.info(f"Processed and saved data for {file_path}")

    def create_xml(self, source_class, target_class, idx, file_path, use_property_mapping):
        """
        Creates an XML configuration file for LIMES.

        Parameters
        ----------
        source_class : str
            The IRI of the source class.
        target_class : str
            The IRI of the target class.
        idx : int
            An index used for naming the configuration file.
        file_path : str
            The path to the source RDF file.
        use_property_mapping : bool
            Indicates whether to use the property mapping or coverage method.
        """
        limes = ET.Element("LIMES")

        # Add owl prefix
        prefix_elem = ET.SubElement(limes, 'PREFIX')
        namespace_elem = ET.SubElement(prefix_elem, 'NAMESPACE')
        namespace_elem.text = 'https://www.w3.org/2002/07/owl#'
        label_elem = ET.SubElement(prefix_elem, 'LABEL')
        label_elem.text = 'owl'

        # Add PREFIX section
        for namespace, prefix in self.namespace_to_prefix.items():
            prefix_elem = ET.SubElement(limes, 'PREFIX')
            namespace_elem = ET.SubElement(prefix_elem, 'NAMESPACE')
            namespace_elem.text = namespace
            label_elem = ET.SubElement(prefix_elem, 'LABEL')
            label_elem.text = prefix

        prefixed_class_iri_source = self.replace_with_prefix(iri=source_class)
        prefixed_class_iri_target = self.replace_with_prefix(iri=target_class)

        # SOURCE section
        source = ET.SubElement(limes, 'SOURCE')
        ET.SubElement(source, 'ID').text = 'source_labels'
        ET.SubElement(source, 'ENDPOINT').text = file_path
        ET.SubElement(source, 'TYPE').text = 'N-TRIPLE'
        ET.SubElement(source, 'VAR').text = '?x'
        ET.SubElement(source, 'PAGESIZE').text = '-1'
        ET.SubElement(source, 'RESTRICTION').text = f'?x a {prefixed_class_iri_source}'

        # TARGET section
        target = ET.SubElement(limes, 'TARGET')
        ET.SubElement(target, 'ID').text = 'target_data'
        if self.target_graph_path:
            ET.SubElement(target, 'ENDPOINT').text = self.target_graph_path
            ET.SubElement(target, 'TYPE').text = 'N-TRIPLE'
            ET.SubElement(target, 'VAR').text = '?y'
            ET.SubElement(target, 'PAGESIZE').text = '-1'
            ET.SubElement(target, 'RESTRICTION').text = f'?y a {prefixed_class_iri_target}'
        else:
            ET.SubElement(target, 'ENDPOINT').text = 'https://query.wikidata.org/sparql'
            ET.SubElement(target, 'VAR').text = '?y'
            if self.total_count_target > 15:
                ET.SubElement(target, 'PAGESIZE').text = '100000' 
            else:
                ET.SubElement(target, 'PAGESIZE').text = '-1'
            ET.SubElement(target, 'RESTRICTION').text = f'?y wdt:P31 {prefixed_class_iri_target}'

        # Properties
        if use_property_mapping:
            # Use properties from the property mapping
            prefixed_properties_source = self.prefixed_iri_dict.get(prefixed_class_iri_source, [])
            for prop in prefixed_properties_source:
                property_elem = ET.SubElement(source, 'PROPERTY')
                property_elem.text = f'{prop} AS lowercase'

            prefixed_properties_target = self.prefixed_iri_dict.get(prefixed_class_iri_target, [])
            for prop in prefixed_properties_target:
                property_elem = ET.SubElement(target, 'PROPERTY')
                property_elem.text = f'{prop} AS lowercase'
        else:
            # Use properties from coverage method
            if source_class in self.class_properties_source.keys():
                filtered_properties = self.class_properties_source[source_class]
                prefixed_properties_source = set()
                for prop in filtered_properties:
                    prefixed_prop = self.replace_with_prefix(prop)
                    found_in_nested = False

                    for nested_prop_set in self.nested_props.values():
                        if prefixed_prop in nested_prop_set:
                            found_in_nested = True
                            break

                    if not found_in_nested:
                        prefixed_properties_source.add(prefixed_prop)

                for prop in prefixed_properties_source:
                    property_elem = ET.SubElement(source, 'PROPERTY')
                    property_elem.text = f'{prop} AS lowercase'

                for interp, prop_set in self.nested_props.items():
                    for prop in prop_set:
                        if any(prop in props for props in self.prefixed_iri_dict.values()):
                            property_elem = ET.SubElement(source, 'PROPERTY')
                            property_elem.text = f'{interp}/{prop} AS lowercase'

            if target_class in self.class_properties_target.keys():
                filtered_properties = self.class_properties_target[target_class]
                prefixed_properties_target = [self.replace_with_prefix(prop) for prop in filtered_properties]
                if not self.target_graph_path:
                    prefixed_properties_target.append(self.replace_with_prefix("http://www.wikidata.org/prop/direct/P31"))
                for prop in prefixed_properties_target:
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
        """
        Saves an XML element to a file with pretty formatting.

        Parameters
        ----------
        element : xml.etree.ElementTree.Element
            The XML element to save.
        filename : str
            The name of the output XML file.
        """
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
        """
        Queries a graph to get the total instance count and property coverage for a given class.

        Parameters
        ----------
        graph : rdflib.Graph
            The RDF graph to query.
        class_uri : str
            The IRI of the class to query.

        Returns
        -------
        total_count : int
            The total number of instances of the class.
        properties_info : dict
            A dictionary mapping property IRIs to their occurrence counts.
        nested_prop_dict : dict
            A dictionary of nested properties.
        """
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
        if total_count > 0:
            # Existing behavior when instances of class_uri are found
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
            # No instances of class_uri found
            # Consider all properties with literal values in the graph
            properties_query = f"""
            SELECT ?property (COUNT(DISTINCT ?property) AS ?propCount)
            WHERE {{
                ?i ?property <{class_uri}> .
            }}
            """
            properties_info = {}
            nested_prop_dict = {}  # No nested properties
            results = graph.query(properties_query)
            for result in results:
                properties_info[str(result.property)] = int(result.propCount)
            return total_count, properties_info, nested_prop_dict

        
    def query_target_graph(self, target_class):
        """
        Queries the target graph or Wikidata to get instance counts and property coverage for a given class.

        Parameters
        ----------
        target_class : str
            The IRI of the target class.

        Returns
        -------
        total_count : int
            The total number of instances of the class.
        properties_info : dict
            A dictionary mapping property IRIs to their occurrence counts.
        nested_prop_dict : dict
            A dictionary of nested properties (empty if querying Wikidata).
        """
        if self.target_graph_path:
            # Query the target graph file
            return self.query_graph(self.g_target, target_class)
        else:
            # Query Wikidata endpoint
            class_id = target_class.split('/')[-1]
            total_query_target = f"""
                    SELECT (COUNT(DISTINCT ?i) AS ?orderCount)
                    WHERE {{
                        ?i wdt:P31 wd:{class_id} .
                    }}
                    """
            self.sparql.setQuery(total_query_target)
            self.sparql.setReturnFormat(JSON)
            results = self.execute_safe_query()
            total_count = int(results["results"]["bindings"][0]["orderCount"]["value"]) if results["results"]["bindings"] else 0
            logging.debug(f"Total instances for target class {target_class}: {total_count}")

            literal_query_target = f"""
                    SELECT ?property (COUNT(DISTINCT ?instance) AS ?literalCount)
                    WHERE {{
                        ?instance wdt:P31 wd:{class_id} .
                        ?instance ?property ?value .
                        FILTER(isLiteral(?value))
                    }}
                    GROUP BY ?property
                    """
            self.sparql.setQuery(literal_query_target)
            self.sparql.setTimeout(600)
            self.sparql.setReturnFormat(JSON)
            results = self.execute_safe_query()
            properties_info = {result['property']['value']: int(result['literalCount']['value']) for result in results["results"]["bindings"]}
            return total_count, properties_info, {}
                
    def compare_dictionaries(self, original_dict, updated_dict):
        """
        Compares two dictionaries and updates the second with missing entries.

        Parameters
        ----------
        original_dict : dict
            The original dictionary with class IRIs and properties.
        updated_dict : dict
            The updated dictionary to compare and update.

        Returns
        -------
        bool
            True if all keys and properties are present, False otherwise.
        dict
            The updated dictionary.
        """
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
        """
        Saves internal dictionaries to pickle files for debugging purposes.

        Parameters
        ----------
        file_path : str
            The path to the source RDF file.
        """
        dir_name = os.path.dirname(file_path)
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        dict_path = os.path.join(dir_name, base_name)
        if not os.path.exists(dict_path):
            os.makedirs(dict_path)
        
        with open(os.path.join(dict_path, 'class_properties_source.pkl'), 'wb') as f:
            pickle.dump(self.class_properties_source, f)
        with open(os.path.join(dict_path, 'class_properties_target.pkl'), 'wb') as f:
            pickle.dump(self.class_properties_target, f)

        with open(os.path.join(dict_path, 'coverage_source.pkl'), 'wb') as f:
            pickle.dump(self.coverage_dict_source, f)
        with open(os.path.join(dict_path, 'coverage_target.pkl'), 'wb') as f:
            pickle.dump(self.coverage_dict_target, f)

        with open(os.path.join(dict_path, 'prefix_namespace_dict.pkl'), 'wb') as f:
            pickle.dump(self.namespace_to_prefix, f)
        with open(os.path.join(dict_path, 'prefixed_iri_dict.pkl'), 'wb') as f:
            pickle.dump(self.prefixed_iri_dict, f)

        with open(os.path.join(dict_path, 'nested_properties.pkl'), 'wb') as f:
            pickle.dump(self.nested_props, f)

def parse_arguments():
    """
    Parses command-line arguments for the script.

    Returns
    -------
    argparse.Namespace
        The parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description='Process RDF data based on user input')
    parser.add_argument('action', choices=['all', 'specific'],
                        help='Specify "all" to process all subdirectories, or "specific" for a specific source and target graphs.')
    parser.add_argument('-p', '--path',
                        help='Specify the path to an input directory or input KG file when using "specific".')
    parser.add_argument('-c', '--config_output_path',
                        help='Specify the path to a directory for storing the config files.', required=True)
    parser.add_argument('-o', '--output_path',
                        help='Specify the path to a directory for output of config files.', required=True)
    parser.add_argument('-pm', '--property_mapping',
                        help='Path to the property mapping file.')
    parser.add_argument('-cm', '--class_mapping',
                        help='Path to the class mapping file.')
    parser.add_argument('--use_endpoint', action='store_true',
                        help='Use endpoint for target graph when action is "specific".')
    parser.add_argument('--source_graph',
                        help='Path to the source graph when action is "specific".')
    parser.add_argument('--target_graph',
                        help='Path to the target graph when action is "specific".')
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    base_directory = "/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/dataset_extraction_scripts/domain_specific/linking_dataset/" # '/scratch/hpc-prf-dsg/WHALE-data/domain_specific/linking_dataset/'
    
    # Check if base_directory exists, if not create it
    if not os.path.exists(base_directory):
        os.makedirs(base_directory)
        logging.info(f"Created directory: {base_directory}")
    else:
        logging.info(f"Directory already exists: {base_directory}")
        
    rdf_processor = RDFProcessor(base_directory, args.config_output_path, args.output_path,
                                 class_mapping_path=args.class_mapping, property_mapping_path=args.property_mapping,
                                 target_graph_path=args.target_graph)
    
    if args.action == 'all':
        rdf_subdirectories = [os.path.join(rdf_processor.base_directory, d) for d in os.listdir(rdf_processor.base_directory) if os.path.isdir(os.path.join(rdf_processor.base_directory, d))]
        for subdirectory in tqdm(rdf_subdirectories, desc="Processing subdirectories"):
            rdf_processor.process_directory(subdirectory, rdf_processor.linked_classes)
    elif args.action == 'specific':
        if args.source_graph:
            if os.path.isdir(args.source_graph):
                rdf_processor.process_directory(args.source_graph, rdf_processor.linked_classes)
            elif args.source_graph.endswith('.txt') or args.source_graph.endswith('.nt') or args.source_graph.endswith('.ttl'):
                rdf_processor.process_file(args.source_graph, rdf_processor.linked_classes)
            else:
                logging.error("Specified source_graph is not a directory or .txt/.nt or .ttl file.")
                exit(1)
        else:
            if args.path:
                if os.path.isdir(args.path):
                    rdf_processor.process_directory(args.path, rdf_processor.linked_classes)
                elif args.path.endswith('.txt') or args.path.endswith('.nt') or args.path.endswith('.ttl'):
                    rdf_processor.process_file(args.path, rdf_processor.linked_classes)
                else:
                    logging.error("Specified path is not a directory or .txt/.nt or .ttl file.")
                    exit(1)
            else:
                logging.error("Source graph must be specified for the 'specific' action.")
                exit(1)

        if not args.target_graph and not args.use_endpoint:
            logging.error("Target graph was not set and --use_endpoint is not true.")
            exit(1)
        # Note: Target graph is handled within the RDFProcessor initialization
    else:
        logging.error("Invalid action specified.")
