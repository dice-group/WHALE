from rdflib import Graph, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD
import os
import pandas as pd
from tqdm import tqdm
import time
import argparse


# Function to check if a CSV file is empty (only headers)
def is_csv_empty(file_path):
    try:
        df = pd.read_csv(file_path)
        return df.empty
    except pd.errors.EmptyDataError:
        return True


# Initialize a graph
g = Graph()

# Define namespaces
wr = Namespace("http://whale.data.dice-research.org/resource/")
wo = Namespace("http://whale.data.dice-research.org/ontology/")
xsd = Namespace("http://www.w3.org/2001/XMLSchema#")

# Bind the namespaces to the graph
g.bind("wr", wr)
g.bind("wo", wo)
g.bind("xsd", xsd)

# Add class definitions to the graph
g.add((wo.Price, RDF.type, RDFS.Class))
g.add((wo.ActualPrice, RDF.type, RDFS.Class))
g.add((wo.DiscountPrice, RDF.type, RDFS.Class))
g.add((wo.Product, RDF.type, RDFS.Class))

# Specify subclass relationships
g.add((wo.ActualPrice, RDFS.subClassOf, wo.Price))
g.add((wo.DiscountPrice, RDFS.subClassOf, wo.Price))

# Function to add triple if value is not NaN
def add_triple_if_not_nan(subject, predicate, value, datatype):
    if pd.notna(value) and value is not None:
        g.add((subject, predicate, Literal(value, datatype=datatype)))


# Function to add price details
def add_price_details(price_uri, price_value, price_type):
    try:
        # Attempt to convert the price value to float
        price_value_float = float(price_value)
        g.add((price_uri, RDF.type, price_type))
        g.add(
            (price_uri, wo.hasCurrency, Literal("Indian Rupees", datatype=XSD.string))
        )
        g.add((price_uri, wo.hasSymbol, Literal("₹", datatype=XSD.string)))
        g.add((price_uri, wo.hasAmount, Literal(price_value_float, datatype=XSD.float)))
    except ValueError as e:
        print(
            f"Error converting price value to float: {e} - Skipping price for {price_uri}"
        )

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

    # Function to safely convert to int
    def safe_int_convert(x):
        try:
            return int(x)
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
    df["no_of_ratings"] = df["no_of_ratings"].apply(safe_int_convert)
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
        g.add((product_uri, RDF.type, wo.Product))
        add_triple_if_not_nan(product_uri, wo.hasName, row["name"], XSD.string)
        add_triple_if_not_nan(
            product_uri, wo.hasMainCategory, row["main_category"], XSD.string
        )
        add_triple_if_not_nan(
            product_uri, wo.hasSubCategory, row["sub_category"], XSD.string
        )
        add_triple_if_not_nan(product_uri, wo.hasImage, row["image"], XSD.anyURI)
        add_triple_if_not_nan(product_uri, wo.hasLink, row["link"], XSD.anyURI)
        add_triple_if_not_nan(product_uri, wo.hasRatings, row["ratings"], XSD.float)
        add_triple_if_not_nan(
            product_uri, wo.hasNumberOfRatings, row["no_of_ratings"], XSD.int
        )

        # Check for actual price and add details
        if pd.notna(row["actual_price"]):
            actual_price_uri = wr[f"r{global_counter}_act_price1"]
            add_price_details(actual_price_uri, row["actual_price"], wo.ActualPrice)
            g.add((product_uri, wo.hasActualPrice, actual_price_uri))

        # Check for discount price and add details
        if pd.notna(row["discount_price"]):
            discount_price_uri = wr[f"r{global_counter}_dis_price1"]
            add_price_details(
                discount_price_uri, row["discount_price"], wo.DiscountPrice
            )
            g.add((product_uri, wo.hasDiscountPrice, discount_price_uri))

        # Increment the global counter
        global_counter += 1

    return global_counter  # Return the updated counter


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="This script processes CSV files to create a knowledge graph. The files are read from a specified dataset directory.")
    parser.add_argument("--dataset", default="Data", help="The name of the folder containing the dataset. Default is 'Data'.")

    # Parse arguments
    args = parser.parse_args()

    # Use the dataset argument as the folder name
    dataset_folder = args.dataset
    extraction_path = os.path.join(os.getcwd(), dataset_folder)

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
    print(f"Number of triples in the graph: {num_triples}")

    print("Serializing the graph... \n")

    start_time = time.perf_counter()
    # Serialize the graph
    g.serialize(destination="knowledge_graph.ttl", format="turtle")
    # Serialize the graph to RDF/XML (commonly used with .owl files)
    g.serialize(destination="knowledge_ontology.owl", format="application/rdf+xml")
    
    end_time = time.perf_counter()
    total_time = end_time - start_time

    print("Done.\n")
    print(f"Serialization took {total_time:.4f} seconds ")
