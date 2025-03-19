import os
import json
import logging
from typing import List, Dict, Any
from SPARQLWrapper import SPARQLWrapper, JSON
from helper import compute_cache_filename

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

def get_top_props_cached(cache_dir: str, endpoint: str, query_file: str) -> List[Dict[str, Any]]:
    cache_file = compute_cache_filename(cache_dir, endpoint, query_file)
    
    if os.path.exists(cache_file):
        logging.info(f"Loading cached property data from {cache_file}")
        with open(cache_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        logging.info("No cached property data found. Executing query...")
        data = get_top_props(endpoint, query_file)
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        logging.info(f"Cached property data to {cache_file}")
    return data