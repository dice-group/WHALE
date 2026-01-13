import argparse
import re
import sys
from typing import Set
from urllib.parse import unquote

from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF, RDFS, OWL

def local_name(iri: str) -> str:
    s = iri
    s = unquote(s)

    if "#" in s:
        s = s.rsplit("#", 1)[1]
    else:
        s = s.rsplit("/", 1)[-1]

    s = re.sub(r"[_\-\.\+]+", " ", s)

    s = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", s)

    s = re.sub(r"\s+", " ", s).strip().lower()

    return s or "class"

def find_missing_class_decls_nt(input_path: str) -> Set[URIRef]:
    g = Graph()
    g.parse(input_path, format="nt")

    used_as_type: Set[URIRef] = set()
    declared: Set[URIRef] = set()

    for _, _, o in g.triples((None, RDF.type, None)):
        if isinstance(o, URIRef):
            used_as_type.add(o)

    for s, _, _ in g.triples((None, RDF.type, OWL.Class)):
        if isinstance(s, URIRef):
            declared.add(s)

    for s, _, _ in g.triples((None, RDF.type, RDFS.Class)):
        if isinstance(s, URIRef):
            declared.add(s)

    return used_as_type - declared

def write_class_decls_with_labels_nt(classes: Set[URIRef], out_stream, lang: str):
    for c in sorted(classes, key=str):
        label = local_name(str(c))
        out_stream.write(f"<{c}> <{RDF.type} {OWL.Class}> .\n")
        lit = Literal(label, lang=lang).n3()
        out_stream.write(f"<{c}> <{RDFS.label}> {lit} .\n")

def main():
    ap = argparse.ArgumentParser(
        description="Add owl:Class + rdfs:label for resources."
    )
    ap.add_argument("--in", dest="inp", required=True, help="Input .nt")
    ap.add_argument("--out", dest="out", required=True, help="Save declaration file separately.")
    args = ap.parse_args()

    missing = find_missing_class_decls_nt(args.inp)

    with open(args.out, "w", encoding="utf-8") as f:
        write_class_decls_with_labels_nt(missing, f, lang="en")

if __name__ == "__main__":
    main()