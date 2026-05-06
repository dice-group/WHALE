#!/bin/bash
set -euo pipefail

DATASET="${1:?Usage: $0 dataset.nt > types_with_labels.txt}"

RDF_TYPE="<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"

awk -v rdf_type="$RDF_TYPE" '
  # First: remember all types of each subject
  $2 == rdf_type && $3 ~ /^</ {
    types[$1][$3] = 1
  }

  # Then: if subject has one of wanted predicates, mark its types as useful
  $2 == "<http://www.w3.org/2000/01/rdf-schema#label>" ||
  $2 == "<http://schema.org/name>" {
    has_label[$1] = 1
  }

  END {
    for (s in has_label) {
      if (s in types) {
        for (t in types[s]) {
          useful_types[t] = 1
        }
      }
    }

    for (t in useful_types) {
      print t
    }
  }
' "$DATASET" | sort -u