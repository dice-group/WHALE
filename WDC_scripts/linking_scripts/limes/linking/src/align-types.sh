#!/bin/bash
set -euo pipefail

SCRIPT="/scratch/hpc-prf-whale/albert/WHALE/WDC_scripts/linking_scripts/limes/linking/src/align.py"
INPUT="/scratch/hpc-prf-whale/akhomich/Lamarr/tmp/merge.nt"
TYPES_FILE="/scratch/hpc-prf-whale/akhomich/Lamarr/tmp/unique_types.txt"
OUT_BASE="/scratch/hpc-prf-whale/akhomich/Lamarr/data/gene_tract_project_score_rma_seq"

mkdir -p "$OUT_BASE"

while IFS= read -r TYPE_URI || [ -n "$TYPE_URI" ]; do
    # remove spaces
    TYPE_URI="$(echo "$TYPE_URI" | xargs)"

    # skip empty lines and comments
    if [ -z "$TYPE_URI" ] || [[ "$TYPE_URI" == \#* ]]; then
        continue
    fi

    # remove <...> if present
    TYPE_URI="${TYPE_URI#<}"
    TYPE_URI="${TYPE_URI%>}"

    # create safe folder name from URI
    TYPE_NAME="$(basename "$TYPE_URI")"
    TYPE_NAME="${TYPE_NAME//#/_}"
    TYPE_NAME="${TYPE_NAME//[^a-zA-Z0-9._-]/_}"

    RUN_DIR="$OUT_BASE/$TYPE_NAME"
    mkdir -p "$RUN_DIR"

    echo "Running for type:"
    echo "  $TYPE_URI"
    echo "Output:"
    echo "  $RUN_DIR"
    echo

    python3 "$SCRIPT" \
      --input "$INPUT" \
      --db "$RUN_DIR/labels.sqlite" \
      --out "$RUN_DIR/duplicate_label_pairs.tsv" \
      --type-uri "$TYPE_URI" \
      --label-predicate "http://www.w3.org/2000/01/rdf-schema#label" \
      --label-predicate "http://schema.org/name" \
      --two-pass \
      > "$RUN_DIR/run.log" 2>&1

    echo "Done: $TYPE_URI"
    echo

done < "$TYPES_FILE"

echo "All done."