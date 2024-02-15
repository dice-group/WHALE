from rdflib import Graph, Literal, Namespace, RDF, RDFS, XSD, OWL
import os
import pandas as pd
from tqdm import tqdm
import time
import argparse
import logging

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

# Declare the ontology itself
g.add((wo[""], RDF.type, OWL.Ontology))
g_ont.add((wo[""], RDF.type, OWL.Ontology))

# Define OWL ontology - Classes
g.add((wo.Product, RDF.type, OWL.Class))
g_ont.add((wo.Product, RDF.type, OWL.Class))
g.add((wo.MainCategory, RDF.type, OWL.Class))
g_ont.add((wo.MainCategory, RDF.type, OWL.Class))
g.add((wo.SubCategory, RDF.type, OWL.Class))
g_ont.add((wo.SubCategory, RDF.type, OWL.Class))

# Define OWL ontology - Subclass
g.add((wo.SubCategory, RDFS.subClassOf, wo.MainCategory))
g_ont.add((wo.SubCategory, RDFS.subClassOf, wo.MainCategory))

# Define OWL ontology - Properties with their domains and ranges
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

# Loop through properties to set them as DatatypeProperty and define their domain and range
for prop, rng in properties.items():
    g.add((prop, RDF.type, OWL.DatatypeProperty))
    g_ont.add((prop, RDF.type, OWL.DatatypeProperty))
    g.add((prop, RDFS.domain, wo.Product))
    g_ont.add((prop, RDFS.domain, wo.Product))
    g.add((prop, RDFS.range, rng))
    g_ont.add((prop, RDFS.range, rng))


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
        g_ont.add((product_uri, RDF.type, OWL.NamedIndividual))
        g.add((product_uri, RDF.type, wo.Product))
        g_ont.add((product_uri, RDF.type, wo.Product))
        add_triple_if_not_nan(product_uri, wo.hasName, row["name"], XSD.string, "en")
        add_triple_if_not_nan(
            product_uri, wo.MainCategory, row["main_category"], XSD.string, "en"
        )
        add_triple_if_not_nan(
            product_uri, wo.SubCategory, row["sub_category"], XSD.string, "en"
        )
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

    return global_counter  # Return the updated counter


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="This script processes CSV files to create a knowledge graph. The files are read from a specified dataset directory."
    )
    parser.add_argument(
        "--dataset",
        default="../Data",
        help="The name of the folder containing the dataset. Default is 'Data'.",
    )

    # Parse arguments
    args = parser.parse_args()

    # Use the dataset argument as the folder name
    dataset_folder = args.dataset
    extraction_path = os.path.join(os.getcwd(), dataset_folder)
    logging.info(f"Processing dataset in folder: {dataset_folder}")

    # List all files in the extracted directory and check for empty CSVs
    empty_csv_files = []
    for root, dirs, files in os.walk(extraction_path):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                if is_csv_empty(file_path):
                    empty_csv_files.append(file)

    # Write the list of empty CSV files to a text file
    empty_files_path = os.path.join(os.getcwd(), "empty_csv_files.txt")
    with open(empty_files_path, "w") as f:
        for file in empty_csv_files:
            f.write("%s\n" % file)

    # Initialize global counter
    global_counter = 1

    # Read the list of empty CSV files
    with open(empty_files_path, "r") as file:
        empty_csv_files = [line.strip() for line in file]

    # List CSV files and process them
    for file in tqdm(os.listdir(extraction_path), desc="Overall Progress"):
        if file.endswith(".csv") and file not in empty_csv_files:
            # Pass and update global_counter
            global_counter = process_csv(
                os.path.join(extraction_path, file), global_counter
            )

    num_triples = len(g)
    logging.info(f"Number of triples in the graph: {num_triples}")

    logging.info("Serializing the graph...")

    start_time = time.perf_counter()
    # Serialize the graph
    g.serialize(destination="knowledge_graph.ttl", format="turtle")
    g_ont.serialize(destination="ontology.owl", format="turtle")

    end_time = time.perf_counter()
    total_time = end_time - start_time

    logging.info("Done.")
    logging.info(f"Serialization took {total_time:.4f} seconds")
