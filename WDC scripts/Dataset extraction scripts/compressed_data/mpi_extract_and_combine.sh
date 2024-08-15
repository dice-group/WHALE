#!/bin/bash

SOURCE_DIR="compressed_data/zips"

# Check if the directory exists
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Directory $SOURCE_DIR does not exist. Creating it now."
    mkdir -p "$SOURCE_DIR"
else
    echo "Directory $SOURCE_DIR already exists."
fi

# Download the files using wget
wget -i http://webdatacommons.org/structureddata/2023-12/files/file.list -P "$SOURCE_DIR"

TARGET_DIR="compressed_data"
LOG_DIR="$TARGET_DIR/logs"
TEMP_DIR="$TARGET_DIR/temps"
COUNT_DIR="$TARGET_DIR/counts"
COMBINED_FILE="$TARGET_DIR/mpi_combined_file.txt"
LOG_FILE="$LOG_DIR/mpi_process_$SLURM_PROCID.log"
ENTITY_FILE="$COUNT_DIR/entities_$SLURM_PROCID.txt"
RELATION_FILE="$COUNT_DIR/relations_$SLURM_PROCID.txt"
TRIPLE_COUNT_FILE="$COUNT_DIR/triple_count_$SLURM_PROCID.txt"

# Create directories if they don't exist
mkdir -p "$LOG_DIR"
mkdir -p "$TEMP_DIR"
mkdir -p "$COUNT_DIR"

# Start logging
echo "Process $SLURM_PROCID started at $(date)" > "$LOG_FILE"

all_files=($(ls "$SOURCE_DIR"/*.gz))
total_files=${#all_files[@]}
echo "Total files to process: $total_files" >> "$LOG_FILE"

# Calculate the number of files per process
files_per_proc=10
start_index=$((files_per_proc * $SLURM_PROCID))
end_index=$((start_index + files_per_proc))

# Adjust the end index if it exceeds the total number of files
if [ $end_index -gt $total_files ]; then
  end_index=$total_files
fi

echo "Process $SLURM_PROCID will process files from $start_index to $end_index" >> "$LOG_FILE"

# Create temporary files for this process
temp_file="$TEMP_DIR/combined_file_$SLURM_PROCID.txt"
: > "$temp_file"
: > "$ENTITY_FILE"
: > "$RELATION_FILE"
: > "$TRIPLE_COUNT_FILE"

# Process the files assigned to this process
for ((i=$start_index; i<$end_index; i++)); do
  gz_file="${all_files[$i]}"
  echo "Process $SLURM_PROCID processing file: $gz_file" >> "$LOG_FILE"
  uncompressed_size=$(gzip -l "$gz_file" | awk 'NR==2 {print $2}')
  gunzip -c "$gz_file" | \
  pv -s $uncompressed_size | \
  sed -e "/['\"][^'\"]*['\"]/d" \
      -e 's/\([[:space:]]\|^\)_:\([a-zA-Z0-9]*\)/\1<http:\/\/whale.data.dice-research.org\/resource#\2>/g' | \
  awk '{if (NF > 3) print $0 | "sed -e \"s/ <[^>]*>\\s*.$/ ./g\""; else print $0}' >> "$temp_file"
done

# Count unique entities (from 1st and 3rd columns), unique relations (from 2nd column), and total triples
awk '{print $1 "\n" $3}' $temp_file | sort | uniq > "$ENTITY_FILE"    # Extract and count unique entities
awk '{print $2}' $temp_file | sort | uniq > "$RELATION_FILE"          # Extract and count unique relations
awk 'END {print NR}' $temp_file > "$TRIPLE_COUNT_FILE"                # Count total triples

echo "Process $SLURM_PROCID completed processing file" >> "$LOG_FILE"

# Wait for all processes to finish
wait

# Combine results into the final file by the root process
if [ $SLURM_PROCID -eq 0 ]; then
  echo "Combining files by process $SLURM_PROCID" >> "$LOG_FILE"
  : > "$COMBINED_FILE"
  : > "$TARGET_DIR/unique_entities.txt"
  : > "$TARGET_DIR/unique_relations.txt"
  total_triples=0

  for proc in $(seq 0 $((SLURM_NTASKS - 1))); do
    if [ -f "$TEMP_DIR/combined_file_$proc.txt" ]; then
      cat "$TEMP_DIR/combined_file_$proc.txt" >> "$COMBINED_FILE"
      cat "$COUNT_DIR/entities_$proc.txt" >> "$TARGET_DIR/unique_entities.txt"
      cat "$COUNT_DIR/relations_$proc.txt" >> "$TARGET_DIR/unique_relations.txt"
      triples=$(cat "$COUNT_DIR/triple_count_$proc.txt")
      total_triples=$((total_triples + triples))
    fi
  done

  # Remove duplicate entities and relations
  sort "$TARGET_DIR/unique_entities.txt" | uniq > "$TARGET_DIR/unique_entities_sorted.txt"
  mv "$TARGET_DIR/unique_entities_sorted.txt" "$TARGET_DIR/unique_entities.txt"
  sort "$TARGET_DIR/unique_relations.txt" | uniq > "$TARGET_DIR/unique_relations_sorted.txt"
  mv "$TARGET_DIR/unique_relations_sorted.txt" "$TARGET_DIR/unique_relations.txt"

  # Store the total number of triples
  echo $total_triples > "$TARGET_DIR/total_triples.txt"

  echo "Extraction, editing, and combination completed." >> "$LOG_FILE"
  echo "Total number of files processed: $total_files" >> "$LOG_FILE"
  echo "Total unique entities: $(wc -l < $TARGET_DIR/unique_entities.txt)" >> "$LOG_FILE"
  echo "Total unique relations: $(wc -l < $TARGET_DIR/unique_relations.txt)" >> "$LOG_FILE"
  echo "Total number of triples: $total_triples" >> "$LOG_FILE"
fi

# Do not delete temporary files (for testing purposes)
echo "Temporary files retained by process $SLURM_PROCID" >> "$LOG_FILE"
echo "Process $SLURM_PROCID finished at $(date)" >> "$LOG_FILE"
