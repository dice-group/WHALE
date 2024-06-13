# Check if key does not exist just give it a value
import pandas as pd
import numpy as np
import shelve
import json
import os
import argparse
import logging
from tqdm import tqdm

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def initialize_memmap(filename, dtype, shape):
    mmap = np.memmap(filename, dtype=dtype, mode="w+", shape=shape)
    return mmap


def update_mapping(mapping, key, memmap_file, last_index, shape=None):
    key = str(key)
    if key not in mapping:
        index = last_index + 1  # Increment the last index # index = len(mapping)
        mapping[key] = index
        last_index = index  # Update the last index

        if index >= len(memmap_file):
            new_size = (
                (len(memmap_file) * 2, shape[1]) if shape else len(memmap_file) * 2
            )
            memmap_file._mmap.close()
            memmap_file = np.memmap(
                memmap_file.filename, dtype=memmap_file.dtype, mode="r+", shape=new_size
            )
        memmap_file[index] = index  # Store the index to keep the memmap active

    return mapping, memmap_file, last_index


def save_incremental_mapping(mapping, shelve_filename):
    with shelve.open(shelve_filename, writeback=True) as db:
        for key, value in mapping.items():
            str_key = str(key)  # Ensure key is a string
            db[str_key] = value


def update_metadata(metadata, filename):
    if os.path.exists(filename):
        with open(filename, "r") as f:
            existing_metadata = json.load(f)
    else:
        existing_metadata = {"triple_count": 0}

    # Update the counts
    existing_metadata["triple_count"] += metadata["triple_count"]

    with open(filename, "w") as f:
        json.dump(existing_metadata, f)


def read_with_pandas(data_path, output_dir, chunk_size):
    logging.info(f"Reading {data_path} with Pandas and Memory-Mapping")
    entity_path = os.path.join(output_dir, "entities.memmap")
    relation_path = os.path.join(output_dir, "relations.memmap")
    entity_shelve = os.path.join(output_dir, "entity_map.shelve")
    relation_shelve = os.path.join(output_dir, "relation_map.shelve")
    metadata_file = os.path.join(output_dir, "metadata.json")

    entity_mm = initialize_memmap(
        entity_path, dtype="int32", shape=(chunk_size,)
    )  # work on these files only
    relation_mm = initialize_memmap(
        relation_path, dtype="int32", shape=(chunk_size,)
    )  # work on these files only

    entity_map = {}  # get rid of them
    relation_map = {}  # get rid of them
    last_entity_index = -1  # Start from -1 so the first index is 0
    last_relation_index = -1  # Start from -1

    total_size = os.path.getsize(data_path)

    num_triples = 0
    if data_path.endswith((".ttl", ".txt", ".csv", ".zst", ".nq-all")):
        iterator = pd.read_csv(
            data_path,
            sep="\s+",
            header=None,
            names=["subject", "relation", "object"],
            usecols=[0, 1, 2],
            dtype=str,
            memory_map=True,
            low_memory=True,
            chunksize=chunk_size,
            nrows=100_000
        )

        pbar = tqdm(
            total=total_size,
            desc="Processing Chunks",
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        )
        for chunk in iterator:
            memory_usage = chunk.memory_usage(deep=True).sum()

            for s, r, o in zip(chunk["subject"], chunk["relation"], chunk["object"]):
                entity_map, entity_mm, last_entity_index = update_mapping(
                    entity_map, s, entity_mm, last_entity_index
                )
                entity_map, entity_mm, last_entity_index = update_mapping(
                    entity_map, o, entity_mm, last_entity_index
                )
                relation_map, relation_mm, last_relation_index = update_mapping(
                    relation_map, r, relation_mm, last_relation_index
                )

            num_triples += len(chunk)

            # Periodically save and clear maps to manage memory
            if (
                len(entity_map) > 1_000_000 or len(relation_map) > 1_000_000
            ):
                save_incremental_mapping(entity_map, entity_shelve)
                save_incremental_mapping(relation_map, relation_shelve)
                entity_map.clear()
                relation_map.clear()
            pbar.update(memory_usage)

        # Final save after loop
        save_incremental_mapping(entity_map, entity_shelve)
        save_incremental_mapping(relation_map, relation_shelve)

    pbar.close()
    entity_mm.flush()
    relation_mm.flush()
    update_metadata({"triple_count": num_triples}, metadata_file)

    logging.info("Mapping and file processing completed.")


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file-path", type=str, required=True, help="Path to the dataset file"
    )
    parser.add_argument(
        "--output-dir", type=str, required=True, help="Directory to store output files"
    )
    parser.add_argument(
        "--chunksize", type=int, required=True, help="Number of rows per chunk"
    )
    return parser.parse_args()


def main():
    args = parse_arguments()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    read_with_pandas(args.file_path, args.output_dir, args.chunksize)


if __name__ == "__main__":
    main()
