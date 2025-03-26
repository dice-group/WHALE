import os
import json
import rdflib
import logging
from typing import List, Dict, Any
from SPARQLWrapper import SPARQLWrapper, JSON
from helper import compute_cache_filename

def get_top_props_local(data_file: str, query_file: str) -> List[Dict[str, Any]]:
    logging.info(f"Executing local SPARQL query from file '{query_file}' on data file: {data_file}")

    with open(query_file, 'r') as file:
        query = file.read()

    g = rdflib.Graph()
    fmt = rdflib.util.guess_format(data_file)
    g.parse(data_file, format=fmt)

    results = g.query(query)

    top_props = []
    for row in results:
        try:
            prop_val = str(row["p"])
            count_val = int(row["count"])
            coverage_val = float(row["coverage"])
        except (KeyError, ValueError) as e:
            logging.error(f"Error processing row {row}: {e}")
            continue

        if coverage_val >= 50:
            top_props.append({
                "property": prop_val,
                "count": count_val,
                "coverage": coverage_val
            })

    logging.info(f"Local SPARQL query returned {len(top_props)} properties.")
    return top_props

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

def get_top_props_cached(cache_dir: str, source: str, query_file: str) -> List[Dict[str, Any]]:
    cache_file = compute_cache_filename(cache_dir, source, query_file)
    
    if os.path.exists(cache_file):
        logging.info(f"Loading cached property data from {cache_file}")
        with open(cache_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        logging.info("No cached property data found. Executing query...")
        if os.path.exists(source) and os.path.isfile(source):
            data = get_top_props_local(source, query_file)
        else:
            data = get_top_props(source, query_file)
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        logging.info(f"Cached property data to {cache_file}")
    return data