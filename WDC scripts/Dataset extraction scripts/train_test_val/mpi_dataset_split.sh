#!/bin/bash

SOURCE_DIR="compressed_data/extract_and_combine/temps"
OUTPUT_DIR="compressed_data/train_test_val"
LOG_DIR="$OUTPUT_DIR/logs"
TRAIN_DIR="$OUTPUT_DIR/train"
TEST_DIR="$OUTPUT_DIR/test"
VAL_DIR="$OUTPUT_DIR/val"

mkdir -p "$LOG_DIR" "$TRAIN_DIR" "$TEST_DIR" "$VAL_DIR"

LOG_FILE="$LOG_DIR/mpi_process_$SLURM_PROCID.log"
echo "Process $SLURM_PROCID started at $(date)" > "$LOG_FILE"

FILE_NAME="combined_file_$SLURM_PROCID.txt"
FILE_PATH="$SOURCE_DIR/$FILE_NAME"

echo "Process $SLURM_PROCID processing file: $FILE_PATH" >> "$LOG_FILE"

if [ ! -f "$FILE_PATH" ]; then
    echo "File $FILE_PATH does not exist" >> "$LOG_FILE"
    exit 1
fi

TOTAL_LINES=$(wc -l < "$FILE_PATH")
TRAIN_LINES=$((TOTAL_LINES * 80 / 100))
TEST_LINES=$((TOTAL_LINES * 10 / 100))
VAL_LINES=$((TOTAL_LINES - TRAIN_LINES - TEST_LINES))

# shuf "$FILE_PATH" | tee >(head -n $TRAIN_LINES > "$TRAIN_DIR/train_$SLURM_PROCID.txt") \
#      | tail -n +$(($TRAIN_LINES + 1)) | head -n $TEST_LINES > "$TEST_DIR/test_$SLURM_PROCID.txt" \
#      | tail -n +$(($TRAIN_LINES + $TEST_LINES + 1)) > "$VAL_DIR/val_$SLURM_PROCID.txt"

head -n $TRAIN_LINES "$FILE_PATH" > "$TRAIN_DIR/train_$SLURM_PROCID.txt"
tail -n +$(($TRAIN_LINES + 1)) | head -n $TEST_LINES "$FILE_PATH" > "$TEST_DIR/test_$SLURM_PROCID.txt"
tail -n $VAL_LINES "$FILE_PATH" > "$VAL_DIR/val_$SLURM_PROCID.txt"

echo "Process $SLURM_PROCID completed at $(date)" >> "$LOG_FILE"

wait

if [ $SLURM_PROCID -eq 0 ]; then
    cat "${TRAIN_DIR}/train_"*.txt > "${OUTPUT_DIR}/final_train.txt"
    cat "${TEST_DIR}/test_"*.txt > "${OUTPUT_DIR}/final_test.txt"
    cat "${VAL_DIR}/val_"*.txt > "${OUTPUT_DIR}/final_val.txt"

    for dtype in train test val
    do
        awk '{print $1; print $3}' "$OUTPUT_DIR/final_${dtype}.txt" | sort | uniq > "$OUTPUT_DIR/unique_entities_${dtype}.txt"
        awk '{print $2}' "$OUTPUT_DIR/final_${dtype}.txt" | sort | uniq > "$OUTPUT_DIR/unique_relations_${dtype}.txt"
        wc -l "$OUTPUT_DIR/final_${dtype}.txt" > "$OUTPUT_DIR/total_triples_${dtype}.txt"
    done

    echo "Datasets combined and unique items calculated." >> "$LOG_FILE"
fi
