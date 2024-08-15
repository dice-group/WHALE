import os
import logging
import shutil
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename="linking_dataset_logs.log", filemode="a")

dataset_folder = "hcard"
link_dir = f'/scratch/hpc-prf-dsg/WHALE-data/domain_specific/linking_dataset/{dataset_folder}'
log_file = f'/scratch/hpc-prf-dsg/WHALE-data/domain_specific/domain_logs/{dataset_folder}_domains.log'
domain_dir = f'/scratch/hpc-prf-dsg/WHALE-data/domain_specific/domain_dataset/{dataset_folder}_dataset'
link_file = f"{dataset_folder}_link.txt"

log_dict = {}
with open(log_file, 'r') as f:
    for line in f:
        line = line.strip()
        if ': ' in line:
            key, value = line.split(': ', 1)
            log_dict[f'{key.strip()}'] = int(value.strip())

# Calculate the threshold as 99% of the number of keys in log_dict
threshold = int(0.99 * len(log_dict))

truncate_list = []
approval_list = []

log_items = log_dict.items()

# Convert values to floats for sorting
def get_value(item):
    return float(item[1])

log_items_with_float_values = [(key, float(value)) for key, value in log_items]

# Sort the items by their values using the get_value function
sorted_log_items = sorted(log_items_with_float_values, key=get_value)

# Add threshold number of keys from bottom to truncate_list
truncate_list = [key for key, value in sorted_log_items[:threshold]]
approval_list = [key for key, value in sorted_log_items[threshold:]]

truncate_list = set(truncate_list)
approval_list = set(approval_list)

logging.info(f"Threshold set to {threshold}")
logging.info(f"Truncate list: {len(truncate_list)}")
logging.info(f"Approval list: {len(approval_list)}")

print(f"Threshold set to {threshold}")
print(f"Truncate list: {len(truncate_list)}")
print(f"Approval list: {len(approval_list)}")

if not os.path.exists(link_dir):
    os.makedirs(link_dir)
    logging.info(f"Created directory {link_dir}")

with open(os.path.join(link_dir, link_file), 'w', encoding='utf-8') as outfile:
    for file in tqdm(truncate_list, desc="Joining files"):
        file_path = os.path.join(domain_dir, file)
        if os.path.isfile(file_path):
            with open(file_path, 'r', encoding='utf-8') as infile:
                outfile.write(infile.read())
                logging.info(f"Wrote contents of {file_path} to {link_file}")
        else:
            logging.warning(f"File {file_path} does not exist and will be skipped.")

copied = 0
skipped = 0
for file in tqdm(approval_list, desc="Coping files"):
    source = os.path.join(domain_dir, file)
    if os.path.isfile(source):
        destination = os.path.join(link_dir, file)
        shutil.copy2(source, destination)
        copied += 1
        logging.info(f"Copied {source} to {destination}")
    else:
        skipped += 1
        logging.warning(f"File {source} does not exist and will be skipped.")

logging.info(f"Copied {copied} files and skipped {skipped} files")
print(f"Copied {copied} files and skipped {skipped} files")
