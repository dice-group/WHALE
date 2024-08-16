import os
from tqdm import tqdm
import argparse

def count_file_rows(directory):
    """Count the number of rows in each text file in the specified directory."""
    file_row_counts = {}

    for filename in tqdm(os.listdir(directory), desc="Creating domain logs"):
        file_path = os.path.join(directory, filename)

        if os.path.isfile(file_path) and filename.endswith('.txt'):
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                num_rows = sum(1 for _ in file)
                file_row_counts[os.path.splitext(filename)[0]] = num_rows

    return file_row_counts

def write_log_file(log_file_path, file_row_counts):
    """Write the row counts to a log file."""
    with open(log_file_path, 'w', encoding='utf-8') as log_file:
        for filename, row_count in file_row_counts.items():
            log_file.write(f'{filename}: {row_count}\n')
    print(f'***** Logs written at: {log_file_path} *****')

def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description="Process dataset folders and count file rows")
    parser.add_argument("dataset", type=str, help="Name of the dataset to process")

    args = parser.parse_args()
    dataset = args.dataset

    # Define directories and paths
    directory = f"domain_specific/domain_dataset/{dataset}_dataset"
    log_file_path = f'domain_specific/domain_logs/{dataset}_domains.log'

    # Count file rows
    file_row_counts = count_file_rows(directory)

    # Write the log file
    write_log_file(log_file_path, file_row_counts)

if __name__ == "__main__":
    main()
