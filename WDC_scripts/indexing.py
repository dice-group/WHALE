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
import bisect
import time
import functools
from mmappickle import mmapdict

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

class DataProcessor:
    def __init__(self, file_path, output_dir, chunksize, nrows=None):
        self.file_path = file_path
        self.output_dir = output_dir
        self.chunksize = chunksize
        self.nrows = nrows
        self.mmap_entity = {}
        self.mmap_relation = {}
        self.hash_orig_map = {}
        self.next_entity_id = 0
        self.next_relation_id = 0
        self.saved = False

    @staticmethod
    def hash_value(string):
        return hashlib.sha256(string.encode("utf-8")).hexdigest()

    @staticmethod
    def save_dict_to_mmapdict(data_dict, mmap_file_path):
        mmap_dict = mmapdict(mmap_file_path)
        for key, value in data_dict.items():
            mmap_dict.__setitem__(key, value)
        print(f"Dictionary saved to {mmap_file_path}")

    def check_memory_and_save(self, data_dict, threshold=0.9, mmap_file_path='data_dict.mmap'):
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        memory_usage_gb = memory_info.rss / (1024 ** 3)
        total_memory_gb = psutil.virtual_memory().total / (1024 ** 3)
        usage_ratio = memory_usage_gb / total_memory_gb

        print(f"Memory usage: {memory_usage_gb:.2f} GB / {total_memory_gb:.2f} GB ({usage_ratio:.2%})")

        if usage_ratio > threshold:
            self.save_dict_to_mmapdict(data_dict, mmap_file_path)
            del data_dict
            self.saved = True

    def timeit(func):
        @functools.wraps(func)
        def timeit_wrapper(self, *args, **kwargs):
            start_time = time.perf_counter()
            result = func(self, *args, **kwargs)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            print(
                f"{func.__name__} took {total_time:.4f} seconds "
                f"| Current Memory Usage {psutil.Process(os.getpid()).memory_info().rss / (1024 ** 3): .5} in GB"
            )
            return result
        return timeit_wrapper

    @timeit
    def index_triples_with_pandas(self, train_set, entity_to_idx, relation_to_idx, hash_orig_map):
        n, d = train_set.shape

        def get_index_or_hash_value(x, idx_map, hash_map):
            if x in hash_map.values():
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
    def save_numpy_ndarray(self, data, file_path):
        with open(file_path, "wb") as f:
            np.save(f, data)

    @timeit
    def index_entities(self, unique_entities, first_chunk=False):
        logging.info("Creating entity indexing...")
        for key in tqdm(unique_entities, desc="Converting entities to index"):
            try:
                if first_chunk:
                    self.mmap_entity[key] = self.next_entity_id
                    self.next_entity_id += 1
                elif key not in self.mmap_entity:
                    self.mmap_entity[key] = self.next_entity_id
                    self.next_entity_id += 1
            except:
                hashed_value = self.hash_value(key)
                if first_chunk:
                    self.mmap_entity[hashed_value] = self.next_entity_id
                    self.next_entity_id += 1
                elif hashed_value not in self.mmap_entity:
                    self.mmap_entity[hashed_value] = self.next_entity_id
                    self.next_entity_id += 1
                self.hash_orig_map[hashed_value] = key

        self.check_memory_and_save(self.mmap_entity, mmap_file_path='/dev/shm/entities_to_idx.p')
        return self.mmap_entity, self.hash_orig_map

    @timeit
    def index_relations(self, unique_relations, first_chunk=False):
        logging.info("Creating relations indexing...")
        for key in tqdm(unique_relations, desc="Converting relations to index"):
            try:
                if first_chunk:
                    self.mmap_relation[key] = self.next_relation_id
                    self.next_relation_id += 1
                elif key not in self.mmap_relation:
                    self.mmap_relation[key] = self.next_relation_id
                    self.next_relation_id += 1
            except Exception as e:
                hashed_value = self.hash_value(key)
                if first_chunk:
                    self.mmap_relation[hashed_value] = self.next_relation_id
                    self.next_relation_id += 1
                elif hashed_value not in self.mmap_relation:
                    self.mmap_relation[hashed_value] = self.next_relation_id
                    self.next_relation_id += 1
                self.hash_orig_map[hashed_value] = key

        self.check_memory_and_save(self.mmap_relation, mmap_file_path='/dev/shm/relations_to_idx.p')
        return self.mmap_relation, self.hash_orig_map
    
    @timeit
    def memory_map(self, file_lines):
        path_parts = self.file_path.split(os.sep)
        input_filename = path_parts[-1].split(".")[-2]

        logging.info(f"Processing: {input_filename}")

        logging.info("Creating indexing of unique relations and entities...")

        start_time = time.perf_counter()
        reader = pd.read_csv(
            self.file_path,
            sep="\s+",
            header=None,
            names=["subject", "relation", "object"],
            usecols=[0, 1, 2],
            dtype=str,
            chunksize=self.chunksize,
            memory_map=True,
            engine="python",
            nrows=None if self.nrows is None else self.nrows,
        )
        end_time = time.perf_counter()
        logging.info(
            f"Reading took {end_time - start_time:.6f} seconds"
            f"| Current Memory Usage {psutil.Process(os.getpid()).memory_info().rss / (1024 ** 3): .5} in GB"
        )
        with tqdm(
            total=file_lines if self.nrows is None else self.nrows, desc="Processing file"
        ) as pbar:

            first_chunk = True
            for chunk in reader:
                try:
                    start_time = time.perf_counter()
                    ordered_list = pd.unique(chunk["relation"].values.ravel("K"))
                    end_time = time.perf_counter()
                    print("\n")
                    logging.info(
                        f"Time taken to calculate unique relation: {end_time - start_time:.6f} seconds"
                        f"| Current Memory Usage {psutil.Process(os.getpid()).memory_info().rss / (1024 ** 3): .5} in GB"
                    )
                    logging.info(f"Unique relations: {len(ordered_list)}")
                    self.mmap_relation, relation_hash_map = self.index_relations(
                        ordered_list, first_chunk,
                    )
                    logging.info(
                        f"Memory size of ordered_list: {sys.getsizeof(ordered_list)}"
                    )
                    
                    if not self.saved:
                        self.next_relation_id = len(self.mmap_relation.keys())
                        self.hash_orig_map.update(relation_hash_map)
                    else:
                        self.next_relation_id = len(self.mmap_relation.keys())
                        self.hash_orig_map.update(relation_hash_map)
                        self.save_hash_map()
                        self.mmap_relation.clear()
                        self.hash_orig_map.clear()

                    start_time = time.perf_counter()
                    ordered_list = pd.unique(chunk[["subject", "object"]].values.ravel("K"))
                    end_time = time.perf_counter()
                    logging.info(
                        f"Time taken to calculate unique entities: {end_time - start_time:.6f} seconds"
                        f"| Current Memory Usage {psutil.Process(os.getpid()).memory_info().rss / (1024 ** 3): .5} in GB"
                    )
                    logging.info(f"Unique entities: {len(ordered_list)}")
                    self.mmap_entity, entity_hash_map = self.index_entities(
                        ordered_list, first_chunk,
                    )
                    logging.info(
                        f"Memory size of ordered_list: {sys.getsizeof(ordered_list)}"
                    )
                    del ordered_list
                except Exception as e:
                    logging.error(f"{traceback.print_exc()}")
                    logging.error(
                        f"Memory size of unique_entities: {sys.getsizeof(ordered_list)}"
                    )
                    logging.info(
                        f"Current Memory Usage {psutil.Process(os.getpid()).memory_info().rss / (1024 ** 3): .5} in GB"
                    )

                if not self.saved:
                    self.next_entity_id = len(self.mmap_entity.keys())
                    self.hash_orig_map.update(entity_hash_map)
                else:
                    self.next_entity_id = len(self.mmap_entity.keys())
                    self.hash_orig_map.update(entity_hash_map)
                    self.save_hash_map()                    
                    self.mmap_entity.clear()
                    self.hash_orig_map.clear()

                pbar.update(self.chunksize)
                first_chunk = False

        logging.info("Index mapping done...")

        num_entities, num_relations = len(self.mmap_entity.keys()), len(self.mmap_relation.keys())

        logging.info(f"Total unique entities: {num_entities}")
        logging.info(f"Total unique relations: {num_relations}")

        all_chunks = None
        chunk_counter = 0

        reader = pd.read_csv(
            self.file_path,
            sep="\s+",
            header=None,
            names=["subject", "relation", "object"],
            usecols=[0, 1, 2],
            dtype=str,
            chunksize=self.chunksize,
            memory_map=True,
            low_memory=True,
            engine="python",
            nrows=None if self.nrows is None else self.nrows,
        )
        for chunk in reader:
            try:
                chunk = self.index_triples_with_pandas(
                    chunk, self.mmap_entity, self.mmap_relation, self.hash_orig_map
                )
                chunk = chunk.values
                if all_chunks is None:
                    all_chunks = chunk
                else:
                    all_chunks = np.concatenate((all_chunks, chunk))
                logging.info(f"Saving the indexed triples of chunk {chunk_counter}...")
                self.save_numpy_ndarray(data=all_chunks, file_path=self.output_dir + "/train_set.npy")
                chunk_counter += 1
            except Exception as e:
                logging.error(f"{traceback.print_exc()}")
                logging.error(f"Length of chunks array: {len(all_chunks)}")
                logging.error(f"Memory size of all_chunks: {sys.getsizeof(all_chunks)}")

        logging.info(f"Done! The files are saved at {self.output_dir}")

    def save_hash_map(self):
        self.hash_orig_map = dict(sorted(self.hash_orig_map.items(), key=lambda item: item[1]))
        with open(os.path.join(self.output_dir, "hash_value_mapping.pickle"), "wb") as handle:
            pickle.dump(self.hash_orig_map, handle)

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

    processor = DataProcessor(
        file_path=args.file_path,
        output_dir=args.output_dir,
        chunksize=args.chunksize,
        nrows=args.nrows,
    )
    processor.memory_map(file_lines)

if __name__ == "__main__":
    main()
