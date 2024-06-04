#!/bin/bash

INPUT_FILE="mpi_combined_file.txt"
OUTPUT_FILE="mpi_combined_file_unique.txt"
TEMP_FILE="temp_sorted_file.txt"
LOG_FILE="remove_duplicates.log"

echo "Starting deduplication process at $(date)" > "$LOG_FILE"
echo "Input file: $INPUT_FILE" >> "$LOG_FILE"

echo "Sorting and removing duplicates..." >> "$LOG_FILE"
pv "$INPUT_FILE" | LC_ALL=C sort -u > "$TEMP_FILE" && mv "$TEMP_FILE" "$OUTPUT_FILE"

if [ $? -eq 0 ]; then
    echo "Deduplication completed successfully at $(date)" >> "$LOG_FILE"
    echo "Output file: $OUTPUT_FILE" >> "$LOG_FILE"
    wc -l "$OUTPUT_FILE" >> "$LOG_FILE"
else
    echo "An error occurred during the deduplication process" >> "$LOG_FILE"
fi
