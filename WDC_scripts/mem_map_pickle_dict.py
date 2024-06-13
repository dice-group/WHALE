from mmappickle import mmapdict
import hashlib
import logging
from tqdm import tqdm
import argparse
import os
import pickle
import pandas as pd
import numpy as np
import traceback
import sys
import psutil
import multiprocessing as mp
import bisect
import time
import functools

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

def hash_value(string):
    return hashlib.sha256(string.encode("utf-8")).hexdigest()

def timeit(func):
    @functools.wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(
            f'{func.__name__} took {total_time:.4f} seconds '
            f'| Current Memory Usage {psutil.Process(os.getpid()).memory_info().rss / 1000000: .5} in MB')
        return result

    return timeit_wrapper

@timeit
def index_triples_with_pandas(
    train_set, entity_to_idx: dict, relation_to_idx: dict, hash_orig_map
) -> pd.core.frame.DataFrame:
    n, d = train_set.shape

    def get_index_or_hash_value(x, idx_map, hash_map):
        if x in hash_map.values():
            # Binary Search
            items = list(hash_map.items())
            values = [item[1] for item in items]
            index = bisect.bisect_left(values, x)
            if index != len(values) and values[index] == x:
                return items[index][0]
        return idx_map.__getitem__(x)

    train_set["subject"] = train_set["subject"].apply(
        lambda x: get_index_or_hash_value(x, entity_to_idx, hash_orig_map)
    )
    train_set["relation"] = train_set["relation"].apply(
        lambda x: get_index_or_hash_value(x, relation_to_idx, hash_orig_map)
    )
    train_set["object"] = train_set["object"].apply(
        lambda x: get_index_or_hash_value(x, entity_to_idx, hash_orig_map)
    )
    if isinstance(train_set, pd.core.frame.DataFrame):
        assert (n, d) == train_set.shape
    else:
        raise KeyError("Wrong type training data")
    return train_set

@timeit
def save_numpy_ndarray(data: np.ndarray, file_path: str):
    with open(file_path, "wb") as f:
        np.save(f, data)

@timeit
def index_entities(
    unique_entities,
    mmap_entity,
    next_entity_id,
    hash_orig_map,
    queue,
    first_chunk=False,
    threshold=0.9,
):
    logging.info("Creating entity indexing...")
    for key in unique_entities:
        try:
            if first_chunk:  # took 26:26 minutes to process 5221 entities
                mmap_entity[key] = next_entity_id
                next_entity_id += 1
                assert next_entity_id == len(mmap_entity.keys())
            elif not mmap_entity.__contains__(key): 
                # try: # took 26:34 minutes to process 5221 entities
                #     assert mmap_entity[key]
                # except:
                mmap_entity[key] = next_entity_id
                next_entity_id += 1
                assert next_entity_id == len(mmap_entity.keys())
        except:
            hashed_value = hash_value(key)
            if first_chunk:
                mmap_entity[hashed_value] = next_entity_id
                next_entity_id += 1
                assert next_entity_id == len(mmap_entity.keys())
            elif not mmap_entity.__contains__(hashed_value):
                mmap_entity[hashed_value] = next_entity_id
                next_entity_id += 1
                assert next_entity_id == len(mmap_entity.keys())
            hash_orig_map[hashed_value] = key

    assert next_entity_id == len(mmap_entity.keys())
    queue.put((hash_orig_map))

@timeit
def index_relations(
    unique_relations,
    mmap_relation,
    next_relation_id,
    hash_orig_map,
    queue,
    first_chunk=False,
    threshold = 0.9,
):
    logging.info("Creating relations indexing...")
    for key in unique_relations:
        try:
            if first_chunk:
                mmap_relation[key] = next_relation_id
                next_relation_id += 1
                assert next_relation_id == len(mmap_relation.keys())
            elif not mmap_relation.__contains__(key):
                mmap_relation[key] = next_relation_id
                next_relation_id += 1
                assert next_relation_id == len(mmap_relation.keys())
        except Exception as e:
            hashed_value = hash_value(key)
            if first_chunk:
                mmap_relation[hashed_value] = next_relation_id
                next_relation_id += 1
                assert next_relation_id == len(mmap_relation.keys())
            elif not mmap_relation.__contains__(hashed_value):
                mmap_relation[hashed_value] = next_relation_id
                next_relation_id += 1
                assert next_relation_id == len(mmap_relation.keys())
            hash_orig_map[hashed_value] = key

    assert next_relation_id == len(mmap_relation.keys())
    queue.put((hash_orig_map))

def memory_map(file_path, output_dir, file_lines, chunksize, nrows=None):
    path_parts = file_path.split(os.sep)
    input_filename = path_parts[-1].split(".")[-2]
    entity_mmap_filename = f"mmap_entity_{input_filename}.p"
    relation_mmap_filename = f"mmap_relation_{input_filename}.p"

    logging.info(f"Processing: {input_filename}")

    mmap_entity = mmapdict(f"{os.path.join(output_dir, entity_mmap_filename)}")
    mmap_relation = mmapdict(f"{os.path.join(output_dir, relation_mmap_filename)}")
    hash_orig_map = {}

    next_entity_id = max(len(mmap_entity.keys()), 0)
    next_relation_id = max(len(mmap_relation.keys()), 0)

    logging.info("Creating indexing of unique relations and entities...")

    reader = pd.read_csv(
        file_path,
        sep="\s+",
        header=None,
        names=["subject", "relation", "object"],
        usecols=[0, 1, 2],
        dtype=str,
        chunksize=chunksize,
        memory_map=True,
        low_memory=True,
        engine="python",
        nrows=None if nrows is None else nrows,
    )
    with tqdm(
        total=file_lines if nrows is None else nrows, desc="Processing file"
    ) as pbar:

        first_chunk = True
        for chunk in reader:
            try:
                start_time = time.perf_counter()
                unique_entities = pd.unique(
                    chunk[["subject", "object"]].values.ravel("K")
                )
                end_time = time.perf_counter()

                logging.info(
                    f"Time taken to calculate unique entities: {end_time - start_time:.6f} seconds"
                    f"| Current Memory Usage {psutil.Process(os.getpid()).memory_info().rss / 1000000: .5} in MB"
                )

                start_time = time.perf_counter()
                unique_relations = pd.unique(chunk["relation"].values.ravel("K"))
                end_time = time.perf_counter()

                logging.info(
                    f"Time taken to calculate unique relation: {end_time - start_time:.6f} seconds"
                    f"| Current Memory Usage {psutil.Process(os.getpid()).memory_info().rss / 1000000: .5} in MB"
                )
                logging.info(
                    f"Memory size of unique_entities: {sys.getsizeof(unique_entities)}"
                )
                logging.info(
                    f"Memory size of unique_relations: {sys.getsizeof(unique_relations)}"
                )

            except Exception as e:
                logging.error(f"{traceback.print_exc()}")
                logging.error(
                    f"Memory size of unique_entities: {sys.getsizeof(unique_entities)}"
                )
                logging.error(
                    f"Memory size of unique_relations: {sys.getsizeof(unique_relations)}"
                )

            logging.info(f"Unique entities: {len(unique_entities)}")
            logging.info(f"Unique relations: {len(unique_relations)}")

            queue = mp.Queue()
            entity_process = mp.Process(
                target=index_entities,
                args=(
                    unique_entities,
                    mmap_entity, 
                    next_entity_id,
                    hash_orig_map,
                    queue,
                    first_chunk,
                ),
            )
            relation_process = mp.Process(
                target=index_relations,
                args=(
                    unique_relations,
                    mmap_relation, 
                    next_relation_id,
                    hash_orig_map,
                    queue,
                    first_chunk,
                ),
            )

            entity_process.start()
            relation_process.start()

            entity_process.join()
            relation_process.join()

            entity_hash_map = queue.get()
            relation_hash_map = queue.get()

            next_entity_id = len(mmap_entity.keys())
            next_relation_id = len(mmap_relation.keys())

            assert next_entity_id == len(mmap_entity.keys())
            assert next_relation_id == len(mmap_relation.keys())

            hash_orig_map.update(entity_hash_map)
            hash_orig_map.update(relation_hash_map)

            pbar.update(chunksize)
            first_chunk = False

    hash_orig_map = dict(sorted(hash_orig_map.items(), key=lambda item: item[1]))

    with open(os.path.join(output_dir, "hash_value_mapping.pickle"), "wb") as handle:
        pickle.dump(hash_orig_map, handle)

    hash_orig_map = dict(sorted(hash_orig_map.items(), key=lambda item: item[1]))

    num_entities, num_relations = len(mmap_entity.keys()), len(mmap_relation.keys())

    logging.info(f"Total unique entities: {num_entities}")
    logging.info(f"Total unique relations: {num_relations}")

    all_chunks = None
    chunk_counter = 0

    logging.info("Index mapping done...")
    for row_index, chunk in enumerate(reader):
        try:
            chunk = index_triples_with_pandas(
                chunk, mmap_entity, mmap_relation, hash_orig_map
            )
            chunk = chunk.values
            if all_chunks is None:
                all_chunks = chunk
            else:
                all_chunks = np.concatenate((all_chunks, chunk))
            logging.info(f"Saving the indexed triples of chunk {chunk_counter}...")
            save_numpy_ndarray(data=all_chunks, file_path=output_dir + "/train_set.npy")
            chunk_counter += 1
        except Exception as e:
            logging.error(f"{traceback.print_exc()}")
            logging.error(f"Length of chunks array: {len(all_chunks)}")
            logging.error(f"Memory size of all_chunks: {sys.getsizeof(all_chunks)}")
            logging.error(
                f"Problem occured at line {chunk_counter * chunksize + row_index} containing data:\n{chunk.iloc[row_index]}"
            )

    logging.info(f"Done! The files are saved at {output_dir}")


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
    parser.add_argument(
        "--nrows", type=int, default=None, help="Number of rows to read"
    )
    return parser.parse_args()


def main():
    args = parse_arguments()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    filelines_map = {
        "10M": 10000000,
        "100M": 100000000,
        "500M": 500000000,
        "1B": 1_000_000_000,
    }

    for key in filelines_map:
        if key in args.file_path:
            file_lines = filelines_map[key]
            break
        else:
            file_lines = 57_189_425_968

    memory_map(
        file_path=args.file_path,
        output_dir=args.output_dir,
        file_lines=file_lines,
        chunksize=args.chunksize,
        nrows=args.nrows,
    )


if __name__ == "__main__":
    main()
