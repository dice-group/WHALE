import pandas as pd
import os
import logging
import argparse
from tqdm import tqdm
import dask.dataframe as dd
import numpy as np
from dask.distributed import Client
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def process_chunk(chunk_data):
    try:
        chunk = chunk_data.tobytes().decode('utf-8').splitlines()
        data = [line.split() for line in chunk if len(line.split()) == 4 and line.split()[3] == '.']
        
        if not data:  # If no data matches the condition, create an empty DataFrame with the appropriate columns
            chunk_df = pd.DataFrame(columns=['subject', 'predicate', 'object'])
        else:
            data = np.array([[parts[0], parts[1], parts[2]] for parts in data], dtype=str)  # Ignore the context element
            chunk_df = pd.DataFrame(data, columns=['subject', 'predicate', 'object'])

        total_triples = len(chunk_df)
        unique_entities = set(chunk_df['subject'].unique()).union(set(chunk_df['object'].unique()))
        unique_relations = set(chunk_df['predicate'].unique())

        memory_usage = chunk_df.memory_usage(deep=True).sum()
        memory_usage_mb = memory_usage / (1024 ** 2)

        return total_triples, unique_entities, unique_relations, memory_usage_mb
    except Exception as e:
        logging.error(f"Error processing chunk: {str(e)}")
        return 0, set(), set(), 0.0



def process_large_dataset(file_path, library="pandas", chunksize=1000000):
    path_parts = file_path.split(os.sep)
    relevant_path = os.sep.join(path_parts[-2:])
    logging.info(f"Processing file: {relevant_path}")

    total_size = os.path.getsize(file_path)
    total_size_gb = total_size / (1024**3)
    logging.info(f"Total dataset size: {total_size_gb:.2f} GB")

    unique_entities = set()
    unique_relations = set()
    total_triples = 0

    if library == "pandas":
        chunk_count = 0
        chunk_mem = []
        try:
            reader = pd.read_csv(
                file_path,
                sep="\s+",
                header=None,
                names=["subject", "relation", "object"],
                usecols=[0, 1, 2],
                dtype=str,
                chunksize=chunksize,
                memory_map=True,
                on_bad_lines="warn",
                engine="python",
            )

            for chunk in tqdm(reader, desc="Processing chunks"):
                chunk_count += 1
                total_triples += len(chunk)
                unique_entities.update(chunk["subject"].unique())
                unique_entities.update(chunk["object"].unique())
                unique_relations.update(chunk["relation"].unique())
                memory_usage = chunk.memory_usage(deep=True).sum()
                memory_usage_mb = memory_usage / (1024**2)
                chunk_mem.append(memory_usage_mb)

            logging.info(f"Total number of chunks: {chunk_count}")
            logging.info(
                f"Average memory usage of the chunk in MB: {np.mean(chunk_mem):.2f} MB"
            )

        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")

    elif library == "dask":
        try:
            client = Client()
            logging.info("Dask client initialized.")

            ddf = dd.read_csv(
                file_path,
                sep="\s+",
                header=None,
                usecols=[0, 1, 2],
                dtype=str,
                engine="python",
                blocksize="256MB",
            )
            ddf = ddf.rename(columns={0: "subject", 1: "relation", 2: "object"})

            unique_entities = (
                dd.concat([ddf["subject"], ddf["object"]]).drop_duplicates().compute()
            )
            unique_relations = ddf["relation"].drop_duplicates().compute()
            total_triples = len(ddf)

        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")

    elif library == "numpy":
        chunk_size_lines = chunksize
        chunk_size_bytes = None
        logging.info("Calculating line size in bytes and Chunk size in bytes ...")
        # First, we need to calculate the approximate byte size of each line
        try:
            with open(file_path, "r") as f:
                first_line = f.readline()
                line_size_bytes = len(first_line)
                chunk_size_bytes = line_size_bytes * chunk_size_lines
            logging.info(f"Calculated line size in bytes: {line_size_bytes}")
            logging.info(f"Chunk size in bytes: {chunk_size_bytes}")
        except Exception as e:
            logging.error(f"Failed to calculate line size: {str(e)}")
            return

        # Create a memory map of the file
        try:
            mmap = np.memmap(file_path, dtype="S1", mode="r")
            num_lines = len(mmap) // line_size_bytes
            num_chunks = (num_lines + chunk_size_lines - 1) // chunk_size_lines
            logging.info(f"Number of lines: {num_lines}")
            logging.info(f"Number of chunks: {num_chunks}")
        except Exception as e:
            logging.error(f"Failed to create memory map: {str(e)}")
            return

        chunk_mem = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for i in tqdm(range(num_chunks), desc="Processing chunks"):
                try:
                    start = i * chunk_size_bytes
                    end = min(start + chunk_size_bytes, len(mmap))
                    chunk_data = mmap[start:end]

                    if len(chunk_data) > 0:
                        future = executor.submit(process_chunk, chunk_data)
                        futures.append(future)
                    else:
                        logging.warning(f"Skipped empty chunk at index {i}")
                except Exception as e:
                    logging.error(f"Error submitting chunk {i}: {str(e)}")
                    continue

            for future in tqdm(futures, desc="Collecting results"):
                try:
                    result = future.result()
                    total_triples += result[0]
                    unique_entities.update(result[1])
                    unique_relations.update(result[2])
                    chunk_mem.append(result[3])
                except Exception as e:
                    logging.error(f"Error collecting result from future: {str(e)}")

        logging.info(f"Total number of chunks: {chunk_count}")
        logging.info(
            f"Average memory usage of the chunk in MB: {np.mean(chunk_mem):.2f} MB"
        )

    logging.info(f"Total number of triples: {total_triples}")
    logging.info(f"Number of unique entities: {len(unique_entities)}")
    logging.info(f"Number of unique relations: {len(unique_relations)}")


def main():
    parser = argparse.ArgumentParser(
        description="Process a large dataset file to count RDF triples, unique entities, and relations."
    )
    parser.add_argument(
        "file_path", type=str, help="The file path to the large dataset file."
    )
    parser.add_argument(
        "--library",
        type=str,
        choices=["pandas", "dask", "numpy"],
        default="pandas",
        help="Specify the library to use for processing the dataset.",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=1000000,
        help="Specify the library to use for processing the dataset.",
    )
    args = parser.parse_args()

    process_large_dataset(
        args.file_path, library=args.library, chunksize=args.chunksize
    )


if __name__ == "__main__":
    main()
