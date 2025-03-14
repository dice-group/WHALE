import logging
from typing import List, Dict, Any
from SPARQLWrapper import SPARQLWrapper, JSON

def get_top_props(endpoint: str, query_file: str) -> List[Dict[str, Any]]:
    logging.info(f"Executing SPARQL query from file '{query_file}' on endpoint: {endpoint}")
    with open(query_file, 'r') as file:
        query = file.read()

    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    results = sparql.query().convert()
    top_props = [
        {
            "property": result["p"]["value"],
            "count": int(result["count"]["value"]),
            "coverage": float(result["coverage"]["value"])
        }
        for result in results["results"]["bindings"] 
        if float(result["coverage"]["value"]) >= 50
    ]
    logging.info(f"SPARQL query returned {len(top_props)} properties.")
    return top_props
