import argparse
import re
from typing import Set, TextIO
from urllib.parse import unquote

from rdflib import Literal
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

def _open_nt(path: str) -> TextIO:
    return open(path, "r", encoding="utf-8", errors="replace")

def find_missing_class_decls_nt_streaming(input_path: str) -> Set[str]:
    rdf_type = f"<{str(RDF.type)}>"
    owl_class = f"<{str(OWL.Class)}>"
    rdfs_class = f"<{str(RDFS.Class)}>"

    used_as_type: Set[str] = set()
    declared: Set[str] = set()

    with _open_nt(input_path) as f:
        for line in f:
            if rdf_type not in line:
                continue

            line = line.strip()
            if not line or line[0] == "#":
                continue

            parts = line.split()
            if len(parts) < 4:
                continue

            s_tok, p_tok, o_tok = parts[0], parts[1], parts[2]
            if p_tok != rdf_type:
                continue

            if not (o_tok.startswith("<") and o_tok.endswith(">")):
                continue

            o_iri = o_tok[1:-1]
            used_as_type.add(o_iri)

            if o_tok == owl_class or o_tok == rdfs_class:
                if s_tok.startswith("<") and s_tok.endswith(">"):
                    s_iri = s_tok[1:-1]
                    declared.add(s_iri)

    return used_as_type - declared

def write_class_decls_with_labels_nt(classes: Set[str], out_stream, lang: str):
    rdf_type_iri = str(RDF.type)
    owl_class_iri = str(OWL.Class)
    rdfs_label_iri = str(RDFS.label)
    
    for c in sorted(classes):
        label = local_name(str(c))
        out_stream.write(f"<{c}> <{rdf_type_iri}> <{owl_class_iri}> .\n")
        lit = Literal(label, lang=lang).n3()
        out_stream.write(f"<{c}> <{rdfs_label_iri}> {lit} .\n")

def main():
    ap = argparse.ArgumentParser(
        description="Add owl:Class + rdfs:label for resources."
    )
    ap.add_argument("--in", dest="inp", required=True, help="Input .nt")
    ap.add_argument("--out", dest="out", required=True, help="Save declaration file separately.")
    args = ap.parse_args()

    missing = find_missing_class_decls_nt_streaming(args.inp)

    with open(args.out, "w", encoding="utf-8") as f:
        write_class_decls_with_labels_nt(missing, f, lang="en")

if __name__ == "__main__":
    main()