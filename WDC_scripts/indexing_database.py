import sqlite3
import os
import logging

# import hashlib
import argparse
from tqdm import tqdm
import pandas as pd
import numpy as np
import sys
import time
import functools
import psutil
import traceback
import bisect
import pickle

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# def string)
#     return hashlib.sha256(string.encode("utf-8")).hexdigest()


def timeit(func):
    @functools.wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        logging.info(
            f"{func.__name__} took {total_time:.4f} seconds | Current Memory Usage {psutil.Process(os.getpid()).memory_info().rss / (1024 ** 3): .5f} GB"
        )
        return result

    return timeit_wrapper


def create_connection(db_file):
    """create a database connection to a SQLite database"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception as e:
        logging.error(e)
    return conn


def create_table(conn, create_table_sql):
    """create a table from the create_table_sql statement"""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Exception as e:
        logging.error(e)


def insert_into_db(conn, table, key, value):
    sql = f""" INSERT OR IGNORE INTO {table}(hash, id)
              VALUES(?, ?) """
    cur = conn.cursor()
    cur.execute(sql, (key, value))
    conn.commit()

@timeit
def fetch_index(conn, hash_value):
    """Fetch index from entities or relations table using hash."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM entities WHERE hash = ?", (hash_value,))
    result = cur.fetchone()
    return result[0] if result else None

@timeit
def transform_and_store_data(chunk, conn):
    """Transform data using the indexed entities and relations, and store in SQLite."""
    transformed_data = []
    for _, row in chunk.iterrows():
        subject = fetch_index(conn, row["subject"])
        relation = fetch_index(conn, row["relation"])
        object = fetch_index(conn, row["object"])

        if subject is not None and relation is not None and object is not None:
            transformed_data.append((subject, relation, object))

    # Insert transformed data into database
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO transformed_data (subject, relation, object) VALUES (?, ?, ?)",
        transformed_data,
    )
    conn.commit()


@timeit
def count_rows(conn, table_name):
    """Function to count rows in a given table."""
    logging.info(f"Counting rows from {table_name}")
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        return count
    except Exception as e:
        logging.error(f"Failed to count rows in {table_name}: {str(e)}")
        return None


@timeit
def index_entities(conn, unique_entities, next_entity_id):
    logging.info("Indexing entities...")
    if next_entity_id == 0:
            entity_list = [(index, value) for index, value in enumerate(tqdm(unique_entities, desc="Indexing entities"))]
    else:
            entity_list = [(index + next_entity_id, value) for index, value in enumerate(tqdm(unique_entities, desc="Indexing entities"))]
    cur = conn.cursor()
    cur.executemany("INSERT OR IGNORE INTO entities (hash, id) VALUES (?, ?)", entity_list)
    conn.commit()
    
    next_entity_id += len(unique_entities)
    del entity_list
    return next_entity_id


@timeit
def index_relations(conn, unique_relations, next_relation_id):
    logging.info("Indexing relations...")
    for relation in tqdm(unique_relations, desc="Indexing relations"):
        cur = conn.cursor()
        cur.execute("SELECT id FROM relations WHERE hash=?", (relation,))
        data = cur.fetchone()
        if data is None:
            cur.execute("INSERT INTO relations (hash) VALUES (?)", (relation,))
            conn.commit()


@timeit
def save_numpy_ndarray(data: np.ndarray, file_path: str):
    with open(file_path, "wb") as f:
        np.save(f, data)


@timeit
def memory_map(file_path, output_dir, conn, file_lines, chunksize, nrows=None):
    path_parts = file_path.split(os.sep)
    input_filename = path_parts[-1].split(".")[-2]

    logging.info(f"Processing: {input_filename}")

    logging.info("Creating indexing of unique relations and entities...")

    start_time = time.perf_counter()
    reader = pd.read_csv(
        file_path,
        sep="\s+",
        header=None,
        names=["subject", "relation", "object"],
        usecols=[0, 1, 2],
        dtype=str,
        chunksize=chunksize,
        memory_map=True,
        engine="python",
        nrows=None if nrows is None else nrows,
    )
    end_time = time.perf_counter()
    logging.info(
        f"Reading took {end_time - start_time:.6f} seconds"
        f"| Current Memory Usage {psutil.Process(os.getpid()).memory_info().rss / (1024 ** 3): .5} in GB"
    )
    with tqdm(
        total=file_lines if nrows is None else nrows, desc="Processing file"
    ) as pbar:

        next_entity_id = 0
        next_relation_id = 0
        for chunk in reader:
            try:
                start_time = time.perf_counter()
                ordered_list = pd.unique(chunk["relation"].values.ravel("K"))
                end_time = time.perf_counter()
                logging.info(
                    f"Time taken to calculate unique relation: {end_time - start_time:.6f} seconds"
                    f"| Current Memory Usage {psutil.Process(os.getpid()).memory_info().rss / (1024 ** 3): .5} in GB"
                )
                logging.info(f"Unique relations: {len(ordered_list)}")
                next_relation_id = index_relations(conn, ordered_list, next_relation_id=next_relation_id)

                start_time = time.perf_counter()
                ordered_list = pd.unique(chunk[["subject", "object"]].values.ravel("K"))
                end_time = time.perf_counter()
                logging.info(
                    f"Time taken to calculate unique entities: {end_time - start_time:.6f} seconds"
                    f"| Current Memory Usage {psutil.Process(os.getpid()).memory_info().rss / (1024 ** 3): .5} in GB"
                )
                logging.info(f"Unique entities: {len(ordered_list)}")
                next_entity_id = index_entities(conn, ordered_list, next_entity_id=next_entity_id)
                del ordered_list
            except Exception as e:
                logging.error(f"{traceback.print_exc()}")
                logging.error(
                    f"Memory size of unique_entities: {sys.getsizeof(ordered_list)}"
                )
                logging.info(
                    f"Current Memory Usage {psutil.Process(os.getpid()).memory_info().rss / (1024 ** 3): .5} in GB"
                )

            pbar.update(chunksize)

    logging.info("Index mapping done...")

    hash_orig_map = dict(sorted(hash_orig_map.items(), key=lambda item: item[1]))

    total_entities = count_rows(conn, "entities")
    total_relations = count_rows(conn, "relations")
    logging.info(f"Total unique entities: {total_entities}")
    logging.info(f"Total unique relations: {total_relations}")

    all_chunks = None

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
    
    logging.info("Transforming dataset to indexing")
    for chunk in reader:
        try:
            transform_and_store_data(chunk, conn)
        except Exception as e:
            logging.error(f"{traceback.print_exc()}")
            logging.error(f"Length of chunks array: {len(all_chunks)}")
            logging.error(f"Memory size of all_chunks: {sys.getsizeof(all_chunks)}")

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

    db_path = os.path.join(args.output_dir, "indexing.db")
    conn = create_connection(db_path)

    # SQL for creating entities table
    sql_create_entities_table = """
    CREATE TABLE IF NOT EXISTS entities (
        entity TEXT PRIMARY KEY,
        "index" INTEGER NOT NULL
    );
    """
    # SQL for creating relations table
    sql_create_relations_table = """
    CREATE TABLE IF NOT EXISTS relations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hash TEXT NOT NULL UNIQUE
    );
    """
    # SQL for creating transformed data table
    sql_create_transformed_table = """
    CREATE TABLE IF NOT EXISTS transformed_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        subject_id INTEGER,
        relation_id INTEGER,
        object_id INTEGER
    );
    """
    create_table(conn, sql_create_entities_table)
    create_table(conn, sql_create_relations_table)
    create_table(conn, sql_create_transformed_table)

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
        conn=conn,
        file_lines=file_lines,
        chunksize=args.chunksize,
        nrows=args.nrows,
    )

    if conn:
        conn.close()


if __name__ == "__main__":
    main()
