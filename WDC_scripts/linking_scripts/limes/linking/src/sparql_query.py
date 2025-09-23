import os
import json
import rdflib
import logging
from typing import List, Dict, Any
from helper import compute_cache_filename
from prop_coverage import coverage_from_sparql, coverage_from_local, to_jsonable

def load_rdf_any(fmt: str):
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

def get_top_props(endpoint: str, local=True) -> List[Dict[str, Any]]:
    if local: data = coverage_from_local(endpoint)
    else: data = coverage_from_sparql(endpoint)
    all_props = to_jsonable(data)

    top_props = _select_top_by_coverage(all_props)
    return top_props

def get_top_props_cached(cache_dir: str, source: str) -> List[Dict[str, Any]]:
    cache_file = compute_cache_filename(cache_dir, source)
    
    if os.path.exists(cache_file):
        logging.info(f"Loading cached property data from {cache_file}")
        with open(cache_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        logging.info("No cached property data found. Executing query...")
        if os.path.exists(source) and os.path.isfile(source):
            data = get_top_props(source)
        else:
            data = get_top_props(source, local=False)
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        logging.info(f"Cached property data to {cache_file}")
    return data