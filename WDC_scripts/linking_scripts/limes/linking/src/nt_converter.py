import os
from urllib.parse import urlsplit, urlunsplit, quote
from rdflib import Graph, URIRef
from rdflib.namespace import OWL
from rdflib.util import guess_format
import logging
from typing import Optional

def fix_uri(uri: str) -> str:
    uri = uri[1:-1]

    parts = urlsplit(uri)
    encoded_path = quote(parts.path, safe="/")
    encoded_query = quote(parts.query, safe="=&?/")
    encoded_fragment = quote(parts.fragment, safe="")
    fixed_uri = urlunsplit((parts.scheme, parts.netloc, encoded_path, encoded_query, encoded_fragment))
    return fixed_uri

def enhance_dataset_with_same_as(dataset_file: str, same_as_file: str) -> None:
    dataset_format: Optional[str] = guess_format(dataset_file)

    if dataset_format is None:
        logging.error(f"Unable to guess format for file: {dataset_file}")
        return

    logging.info(f"Guessed format for {dataset_file}: {dataset_format}")

    g = Graph()

    try:
        g.parse(dataset_file, format=dataset_format)
        logging.info(f"Dataset '{dataset_file}' parsed using format '{dataset_format}'.")
    except Exception as e:
        logging.error(f"Error parsing dataset file '{dataset_file}': {e}")
        return

    try:
        with open(same_as_file, "r") as f:
            for line in f:
                parts = line.strip().split("\t")

                resource1 = URIRef(fix_uri(parts[0]))
                resource2 = URIRef(fix_uri(parts[1]))

                g.add((resource1, OWL.sameAs, resource2))
    except Exception as e:
        logging.error(f"Error processing sameAs file '{same_as_file}': {e}")
        return

    output_file = os.path.splitext(dataset_file)[0] + ".nt"

    try:
        g.serialize(destination=output_file, format="nt")
        logging.info(f"Successfully converted {dataset_file} to N-Triples format. Output to {output_file}.")
    except Exception as e:
        logging.error(f"Error serializing to N-Triples: {e}")

enhance_dataset_with_same_as('/scratch/hpc-prf-whale/oaei/the_old_republic_wiki/index.xml', '/scratch/hpc-prf-whale/albert/WHALE/LIMES/output/starwars_the_old_republic_wiki_local/same_as_total.nt')
enhance_dataset_with_same_as('/scratch/hpc-prf-whale/oaei/starwars/index_cleaned.xml', '/scratch/hpc-prf-whale/albert/WHALE/LIMES/output/starwars_the_old_republic_wiki_local/same_as_total.nt')