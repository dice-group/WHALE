#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <file.nt>" >&2
  exit 1
fi

FILE="$1"
if [[ ! -f "$FILE" ]]; then
  echo "Error: not a file: $FILE" >&2
  exit 1
fi

# Faster sorting in most cases
export LC_ALL=C

unique_predicates=$(
  awk '$1 ~ /^</ && $2 ~ /^</ {print $2}' "$FILE" | sort -u | wc -l | tr -d '[:space:]'
)

unique_uri_entities=$(
  awk '
  $1 ~ /^</ {
    s=$1; sub(/^</,"",s); sub(/>$/,"",s); print s
    if ($3 ~ /^</) { o=$3; sub(/^</,"",o); sub(/>$/,"",o); print o }
  }' "$FILE" | sort -u | wc -l | tr -d '[:space:]'
)

unique_triples=$(
  awk '
  /^[[:space:]]*$/ {next}
  {
    gsub(/[[:space:]]+/, " ");
    sub(/^[[:space:]]+/, "", $0);
    sub(/[[:space:]]+$/, "", $0);
    print $0
  }' "$FILE" | sort -u | wc -l | tr -d '[:space:]'
)

echo "unique predicates: $unique_predicates"
echo "unique uri entities: $unique_uri_entities"
echo "unique triples: $unique_triples"
