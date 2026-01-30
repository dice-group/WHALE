#!/usr/bin/env bash
set -euo pipefail

total=0

shopt -s nullglob
for f in ./*_same_as*; do
  # count rows where first two tab-separated fields differ
  c=$(awk -F'\t' '$1 != $2 {n++} END{print n+0}' "$f")
  printf "%s\t%d\n" "$f" "$c"
  total=$((total + c))
done

printf "TOTAL\t%d\n" "$total"
