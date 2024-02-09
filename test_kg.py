import logging
import pickle
from rdflib import Graph

# Configure logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def run_queries(g):
    # Query to count the number of products
    query_num_products = """
    PREFIX wo: <http://whale.data.dice-research.org/ontology/>
    
    SELECT (COUNT(?product) as ?numProducts)
    WHERE {
      ?product a wo:Product .
    }
    """
    for row in g.query(query_num_products):
        logging.info(f"Number of products: {row.numProducts}")

    # Query to get product details: name, main category, and ratings
    query_product_details = """
    PREFIX wo: <http://whale.data.dice-research.org/ontology/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    
    SELECT ?name ?mainCategory ?ratings
    WHERE {
      ?product a wo:Product ;
               wo:hasName ?name ;
               wo:hasMainCategory ?mainCategory ;
               wo:hasRatings ?ratings .
    }
    LIMIT 20
    """
    logging.info("\nProduct Details (Name, Main Category, Ratings):")
    for row in g.query(query_product_details):
        logging.info(f"- {row.name}, {row.mainCategory}, {row.ratings}")

    # Query to find products with both actual and discount prices
    query_price_details = """
    PREFIX wo: <http://whale.data.dice-research.org/ontology/>
    
    SELECT ?productName ?actualPrice ?discountPrice
    WHERE {
      ?product a wo:Product ;
               wo:hasName ?productName ;
               wo:hasActualPrice ?actualPriceRes ;
               wo:hasDiscountPrice ?discountPriceRes .
      ?actualPriceRes wo:hasAmount ?actualPrice .
      ?discountPriceRes wo:hasAmount ?discountPrice .
    }
    LIMIT 20
    """
    logging.info("\nProducts with Actual and Discount Prices:")
    for row in g.query(query_price_details):
        logging.info(f"- {row.productName}: Actual Price = {row.actualPrice}, Discount Price = {row.discountPrice}")

def save_graph(graph, filename):
    with open(filename, 'wb') as file:
        pickle.dump(graph, file)
    logging.info(f"Graph saved to {filename}")

def load_graph(filename):
    with open(filename, 'rb') as file:
        graph = pickle.load(file)
    logging.info(f"Graph loaded from {filename}")
    return graph

def load_and_run_test(file, save_filename):
    try:
        # Attempt to load the graph from a saved file first
        g = load_graph(save_filename)
    except (FileNotFoundError, EOFError, pickle.UnpicklingError):
        logging.info(f"Saved graph not found or corrupted. Parsing from {file} instead.")
        format_type = "ttl" if file.endswith(".ttl") else "xml"
        g = Graph()
        g.parse(file, format=format_type)
        save_graph(g, save_filename)
        
    run_queries(g)

if __name__ == "__main__":
    file = "knowledge_graph.owl"
    save_filename = "saved_graph.pkl"
    load_and_run_test(file, save_filename)
