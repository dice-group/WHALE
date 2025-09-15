import os
import json
import rdflib
import logging
from typing import List, Dict, Any
from SPARQLWrapper import SPARQLWrapper, JSON
from helper import compute_cache_filename

def _load_rdf_any(fmt: str):
    is_quad = fmt in ("nquads", "trig")

    g = rdflib.Dataset() if is_quad else rdflib.Graph()
    if hasattr(g, "default_union"):
        g.default_union = True

    return g

def _select_top_by_coverage(props: List[Dict[str, Any]], slack: float = 0.2) -> List[Dict[str, Any]]:
    if not props:
        return []
    max_cov = max(p["coverage"] for p in props)
    threshold = max_cov * (1 - slack)
    top = [p for p in props if p["coverage"] >= threshold]
    top.sort(key=lambda d: (d["coverage"], d["count"]), reverse=True)
    logging.info(f"Best coverage: {max_cov}; threshold (within {int(slack*100)}%): {threshold}; kept {len(top)}/{len(props)}")
    return top

def get_top_props_local(data_file: str, query_file: str) -> List[Dict[str, Any]]:
    logging.info(f"Executing local SPARQL query from file '{query_file}' on data file: {data_file}")

    with open(query_file, 'r') as file:
        query = file.read()

    fmt = rdflib.util.guess_format(data_file)
    g = _load_rdf_any(fmt)
    g.parse(data_file, format=fmt)

    results = g.query(query)

    all_props: List[Dict[str, Any]] = []
    for row in results:
        try:
            prop_val = str(row["p"])
            count_val = int(row["count"])
            coverage_val = float(row["coverage"])
            all_props.append({"property": prop_val, "count": count_val, "coverage": coverage_val})
        except (KeyError, ValueError) as e:
            logging.error(f"Error processing row {row}: {e}")
            continue

    top_props = _select_top_by_coverage(all_props)
    return top_props

def get_top_props(endpoint: str, query_file: str) -> List[Dict[str, Any]]:
    logging.info(f"Executing SPARQL query from file '{query_file}' on endpoint: {endpoint}")
    with open(query_file, 'r') as file:
        query = file.read()

    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    results = sparql.query().convert()

    all_props = []
    for r in results["results"]["bindings"]:
        try:
            all_props.append({
                "property": r["p"]["value"],
                "count": int(r["count"]["value"]),
                "coverage": float(r["coverage"]["value"])
            })
        except (KeyError, ValueError) as e:
            logging.error(f"Error processing row {r}: {e}")
            continue

    top_props = _select_top_by_coverage(all_props)
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