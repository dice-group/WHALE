from rdflib import Graph, Literal, Namespace, RDF, RDFS, XSD, OWL
import os
import pandas as pd
from tqdm import tqdm
import time
import argparse
import logging
import re

import warnings

warnings.filterwarnings("ignore")

# Basic configuration for logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Function to check if a CSV file is empty (only headers)
def is_csv_empty(file_path):
    try:
        df = pd.read_csv(file_path)
        return df.empty
    except pd.errors.EmptyDataError:
        return True


# Initialize a graph
g = Graph()
g_ont = Graph()

# Define namespaces
wr = Namespace("http://whale.data.dice-research.org/resource#")
wo = Namespace("http://whale.data.dice-research.org/ontology/")

# Bind the namespaces to the graph
g.bind("wr", wr)
g.bind("wo", wo)
g_ont.bind("wr", wr)
g_ont.bind("wo", wo)

# Define OWL ontology - Classes
g.add((wo.Product, RDF.type, OWL.Class))
g_ont.add((wo.Product, RDF.type, OWL.Class))


def add_property_definitions(graph, properties):
    for prop, rng in properties.items():
        graph.add((prop, RDF.type, OWL.DatatypeProperty))
        graph.add((prop, RDFS.domain, wo.Product))
        graph.add((prop, RDFS.range, rng))


def add_object_property_definition(graph, property_uri, domain, range_uri):
    graph.add((property_uri, RDF.type, OWL.ObjectProperty))
    graph.add((property_uri, RDFS.domain, domain))
    graph.add((property_uri, RDFS.range, range_uri))


# Properties with their domains and ranges
properties = {
    wo.hasName: XSD.string,
    wo.hasImage: XSD.anyURI,
    wo.hasLink: XSD.anyURI,
    wo.hasRatings: XSD.int,
    wo.hasCurrency: XSD.string,
    wo.hasSymbol: XSD.string,
    wo.hasActualPrice: XSD.float,
    wo.hasDiscountPrice: XSD.float,
}

# Apply property definitions to both graphs
add_property_definitions(g, properties)
add_property_definitions(g_ont, properties)

# Object properties for categories
object_properties = {
    wo.hasMainCategory: RDFS.Class,
    wo.hasSubCategory: RDFS.Class,
}

# Apply object property definitions to both graphs
for prop, rng in object_properties.items():
    add_object_property_definition(g, prop, wo.Product, rng)
    add_object_property_definition(g_ont, prop, wo.Product, rng)


def camel_case(s):
    # Replace '&' with 'and'
    s = s.replace("&", "and")
    # Remove 's
    s = s.replace("'s", "")
    # Remove other special characters
    s = re.sub(r"[^a-zA-Z0-9\s]", "", s)
    # Convert to CamelCase
    return "".join(word.capitalize() for word in s.split())


# Function to add triple if value is not NaN
def add_triple_if_not_nan(subject, predicate, value, datatype, lang=None):
    if pd.notna(value) and value is not None:
        if datatype is XSD.int:
            g.add((subject, predicate, Literal(int(value), datatype=datatype)))
        elif lang:
            # Add the triple with a language tag, without specifying datatype
            g.add((subject, predicate, Literal(value, lang=lang)))
        else:
            g.add((subject, predicate, Literal(value, datatype=datatype)))


# Process CSV Files
def process_csv(file_path, global_counter):

    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path, index_col=False)

    # Function to safely convert to float
    def safe_float_convert(x):
        try:
            return float(x)
        except (ValueError, TypeError):
            return None

    # Ensure columns are treated as strings and clean the data
    for col in ["ratings", "no_of_ratings", "actual_price", "discount_price"]:
        if col in df.columns:
            # Replace commas only if present, and treat 'nan' as None
            df[col] = (
                df[col]
                .astype(str)
                .apply(lambda x: x.replace(",", "") if "," in x else x)
                .replace("nan", None)
            )

    # Convert to numeric, handling NaNs and other non-numeric values
    df["ratings"] = df["ratings"].apply(safe_float_convert)
    df["no_of_ratings"] = df["no_of_ratings"].apply(safe_float_convert)
    df["actual_price"] = (
        df["actual_price"].str.replace("₹", "").apply(safe_float_convert)
    )
    df["discount_price"] = (
        df["discount_price"].str.replace("₹", "").apply(safe_float_convert)
    )

    # Iterate with a progress bar
    for _, row in tqdm(
        df.iterrows(),
        total=df.shape[0],
        desc=f"Processing {os.path.basename(file_path)}",
    ):
        product_uri = wr[f"r{global_counter}"]  # URI for the product
        g.add((product_uri, RDF.type, OWL.NamedIndividual))
        g.add((product_uri, RDF.type, wo.Product))

        g_ont.add((product_uri, RDF.type, OWL.NamedIndividual))
        g_ont.add((product_uri, RDF.type, wo.Product))

        add_triple_if_not_nan(product_uri, wo.hasName, row["name"], XSD.string, "en")

        # Handling categories
        main_category = row["main_category"]
        sub_category = row["sub_category"]

        # Generate class URIs for categories
        main_category_class = wo[camel_case(main_category)]
        sub_category_class = wo[camel_case(sub_category)]

        # Connect product to categories via properties
        g.add((product_uri, wo.hasMainCategory, main_category_class))
        g.add((main_category_class, RDF.type, OWL.Class))
        g.add((main_category_class, RDFS.label, Literal(main_category, lang="en")))

        g_ont.add((main_category_class, RDF.type, OWL.Class))
        g_ont.add((main_category_class, RDFS.label, Literal(main_category, lang="en")))

        g.add((product_uri, wo.hasSubCategory, sub_category_class))
        g.add((sub_category_class, RDF.type, OWL.Class))
        g.add((sub_category_class, RDFS.label, Literal(sub_category, lang="en")))
        g.add((sub_category_class, RDFS.subClassOf, main_category_class))

        g_ont.add((sub_category_class, RDF.type, OWL.Class))
        g_ont.add((sub_category_class, RDFS.label, Literal(sub_category, lang="en")))
        g_ont.add((sub_category_class, RDFS.subClassOf, main_category_class))

        add_triple_if_not_nan(product_uri, wo.hasImage, row["image"], XSD.anyURI)
        add_triple_if_not_nan(product_uri, wo.hasLink, row["link"], XSD.anyURI)
        add_triple_if_not_nan(product_uri, wo.hasRatings, row["ratings"], XSD.float)
        add_triple_if_not_nan(
            product_uri, wo.hasNumberOfRatings, row["no_of_ratings"], XSD.int
        )
        add_triple_if_not_nan(product_uri, wo.hasCurrency, "Indian Rupees", XSD.string)
        add_triple_if_not_nan(product_uri, wo.hasSymbol, "₹", XSD.string)
        add_triple_if_not_nan(
            product_uri, wo.hasActualPrice, float(row["actual_price"]), XSD.float
        )
        add_triple_if_not_nan(
            product_uri, wo.hasDiscountPrice, float(row["discount_price"]), XSD.float
        )

        # Increment the global counter
        global_counter += 1

    return global_counter


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="This script processes CSV files to create a knowledge graph and ontology. The files are read from a specified dataset directory."
    )
    parser.add_argument(
        "--dataset",
        default="../Data/Amazon_Products/",
        help="The name of the folder containing the dataset. Default is 'Data'.",
    )
    parser.add_argument(
        "--data-file",
        default=None,
        help="Optional. The path to a specific data file to process. If provided, the script processes only this file.",
    )
    parser.add_argument(
        "--kg-output",
        default="knowledge_graph.ttl",
        help="Filename for the output knowledge graph. Default is 'knowledge_graph.ttl'.",
    )
    parser.add_argument(
        "--ont-output",
        default="ontology.owl",
        help="Filename for the output ontology. Default is 'ontology.owl'.",
    )

    # Parse arguments
    args = parser.parse_args()

    # Initialize global counter
    global_counter = 1

    if args.data_file:
        # If a specific data file is provided, process only this file
        logging.info(f"Processing specified data file: {args.data_file}")
        global_counter = process_csv(args.data_file, global_counter)
    else:
        # If no specific data file is provided, process all files in the dataset folder
        dataset_folder = args.dataset
        extraction_path = os.path.join(os.getcwd(), dataset_folder)
        logging.info(f"Processing dataset in folder: {dataset_folder}")

        empty_csv_files = []
        for root, dirs, files in os.walk(extraction_path):
            for file in files:
                if file.endswith(".csv"):
                    file_path = os.path.join(root, file)
                    if is_csv_empty(file_path):
                        empty_csv_files.append(file)

        empty_files_path = os.path.join(os.getcwd(), "empty_csv_files.txt")
        with open(empty_files_path, "w") as f:
            for file in empty_csv_files:
                f.write("%s\n" % file)

        with open(empty_files_path, "r") as file:
            empty_csv_files = [line.strip() for line in file]

        for file in tqdm(os.listdir(extraction_path), desc="Overall Progress"):
            if file.endswith(".csv") and file not in empty_csv_files:
                file_path = os.path.join(extraction_path, file)
                logging.info(f"Processing file: {file}")
                global_counter = process_csv(file_path, global_counter)

    num_triples = len(g)
    logging.info(f"Number of triples in the graph: {num_triples}")

    logging.info("Serializing the graph...")

    start_time = time.perf_counter()

    # Serialize the graph
    kg_output_path = args.kg_output
    ont_output_path = args.ont_output

    logging.info(f"Serializing the knowledge graph to {kg_output_path}")
    g.serialize(destination=kg_output_path, format="turtle")

    logging.info(f"Serializing the ontology to {ont_output_path}")
    g_ont.serialize(destination=ont_output_path, format="turtle")

    end_time = time.perf_counter()
    total_time = end_time - start_time

    logging.info("Done.")
    logging.info(f"Serialization took {total_time:.4f} seconds")
