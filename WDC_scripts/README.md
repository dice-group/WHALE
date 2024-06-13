# Triple Counter

This script processes large RDF dataset files to count RDF triples, unique entities, and relations. It supports processing via three different libraries: Pandas, Dask, and NumPy.

## Features

- **Pandas**: Efficient processing of dataset chunks using Pandas.
- **Dask**: Parallel processing with Dask for large datasets that do not fit in memory.
- **NumPy**: Memory mapping and chunk processing with NumPy for handling large files.

## Requirements

- Python 3.x
- pandas
- [dask](https://docs.dask.org/en/stable/)
- numpy
- tqdm
- argparse

## Installation

Install the required libraries using pip:

```bash
pip install pandas dask numpy tqdm
```

## Usage

The script can be run from the command line. It requires the file path to the dataset and optionally the chunk size and library to use for processing.

### Command Line Arguments

- `file_path`: Path to the dataset file.
- `--chunksize`: Number of lines per chunk for processing (default: 10000).
- `--library`: Library to use for processing (`pandas`, `dask`, or `numpy`; default: `pandas`).

### Example

```sh
python large_dataset_processor.py path/to/dataset.txt --chunksize 5000 --library dask
```

## Code Overview

The script is structured as follows:

- **`process_chunk` Function**: Processes a chunk of data, extracting triples and calculating memory usage.
- **`process_large_dataset` Function**: Handles the dataset processing using the specified library.

## Experiment Idea

To see which method is more suitable to read the [whole dataset](https://files.dice-research.org/datasets/WHALE/WDC/wdc_oct_23.nq-all.tar.gz) of 20TB efficiently. We used `dask`, which uses distributed system and runs `pandas` under the hood for reading huge datasets, [`numpy.memmap`](https://numpy.org/doc/stable/reference/generated/numpy.memmap.html) approach for indexing the data, and `pandas.read_csv` with [`memory_map`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html#:~:text=memory_mapbool%2C%20default%20False), and [`chunksize`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html#:~:text=chunksizeint%2C%20optional) attributes to use memory efficiently.

## Experiment Setup

All the experiments were run on Noctua 2 CPU environment with the following specs:

| **System**                    | Atos BullSequana XH2000                                                                                      |
|-------------------------------|--------------------------------------------------------------------------------------------------------------|
| **Processor Cores**           | 143,872                                                                                                      |
| **Total Main Memory**         | 347.5 TiB                                                                                                     |
| **Floating-Point Performance**| CPU: 5.4 PFLOPS DP Peak (4.19 PFlop/s Linpack) <br> GPU: 2.49 PFLOPS DP Tensor Core Peak (ca. 1.7 PFlop/s Linpack) |
| **Compute Nodes**             | 990 nodes, each with <br> <li> 2x AMD Milan 7763, 2.45 GHz, up to 3.5 GHz <br> <li> 2x 64 cores <br> <li>256 GiB main memory|


## Running the Script

To run the script, navigate to the directory containing the script and execute the following command:

```bash
python large_dataset_processor.py <file_path> [--chunksize <chunksize>] [--library <library>]
```

Replace `<file_path>` with the path to your dataset file, `<chunksize>` with the desired chunk size, and `<library>` with the library you want to use (`pandas`, `dask`, or `numpy`).

## Hyperparameter

`--chunksize <chunksize>` is one of the hyperparameter for the `--library [pandas, numpy]` parameters. In the experiment, we used `chunksize = [100_000_000, 1_000_000, 100_00]` to see which one is feasible.

## Results

For subset dataset of [RDFa](https://files.dice-research.org/datasets/WHALE/WDC/rdfa/transformed_rdfa_train.txt) format with $152.6$ GB filesize, all the three approaches were successfully after to calculate the following data:

```
Total number of triples: 865539636
Number of unique entities: 297819718
Number of unique relations: 2490
```

However, when coming to the whole 20TB dataset, `dask` was thrown a buffer overflow error.
```bash
ERROR - An error occurred: Error tokenizing data. C error: Buffer overflow caught - possible malformed input file.

slurmstepd: error: Detected 1 oom_kill event in StepId=7676243.batch. Some of the step tasks have been OOM Killed.
```

For `chunksize = 100M`, 
- `numpy.memmap` threw the similar `oom_kill event` error
- `pandas` gave the following error: 
    ```
    ERROR - An error occurred: Unable to allocate 15.7 TiB for an array with shape (100000000, 21625) and data type object
    ```

For `chunksize = 10M`,
- `numpy.memmap` is still running but the time per iteration is very large $\approx 5$ minutes on average.
- `pandas` again stopped and gave a similar error
    ```
    ERROR - An error occurred: Unable to allocate 646. GiB for an array with shape (10000000, 8675) and data type object
    ```

For `chunksize = 100K`,
- Both `numpy.memmap` and `pandas` are still running but the time per iteration is very large $\approx 5$ minutes on average.
