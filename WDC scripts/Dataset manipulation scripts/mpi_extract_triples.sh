#!/bin/bash

# Assumes SOURCE_FILE is exported or defined in the environment
SOURCE_FILE="mpi_combined_file.txt"
OUTPUT_DIR="extract_triples"
LOG_DIR="${OUTPUT_DIR}/logs"
mkdir -p "$LOG_DIR" "$OUTPUT_DIR"

extract_triples() {
    local start_line=$1
    local num_lines=$2
    local index_suffix=$3
    local output_file="${OUTPUT_DIR}/extracted_file_${index_suffix}.txt"
    local log_file="${LOG_DIR}/extract_${index_suffix}.log"

    echo "Starting extraction of ranges from $start_line to $((start_line + num_lines - 1)) at $(date)" > "$log_file"
    tail -n +$((start_line + 1)) "$SOURCE_FILE" | head -n $num_lines > "$output_file"
    echo "Completed extraction at $(date)" >> "$log_file"
}

export -f extract_triples
export SOURCE_FILE OUTPUT_DIR LOG_DIR

ROWS_PER_FILE=500000000

# Job calculation for each SLURM process
start_line=$(($SLURM_PROCID * $ROWS_PER_FILE))
end_line=$(($start_line + $ROWS_PER_FILE))

suffix_start=$(printf "%.1fB" $(bc <<< "scale=1; $start_line/1000000000"))
suffix_end=$(printf "%.1fB" $(bc <<< "scale=1; $end_line/1000000000"))

index_suffix="$suffix_start"_"$suffix_end"
extract_triples $start_line $ROWS_PER_FILE "$index_suffix"
