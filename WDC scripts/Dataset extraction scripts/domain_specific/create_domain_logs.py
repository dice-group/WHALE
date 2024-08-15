import os
from tqdm import tqdm
file_row_counts = {}
directory = "/scratch/hpc-prf-dsg/WHALE-data/domain_specific/domain_dataset/hcard_dataset"
for filename in tqdm(os.listdir(directory), desc="Creating domain logs"):
    # Construct full file path
    file_path = os.path.join(directory, filename)

    if os.path.isfile(file_path) and filename.endswith('.txt'):
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            num_rows = sum(1 for _ in file)
            file_row_counts[os.path.splitext(filename)[0]] = num_rows
            
with open('/scratch/hpc-prf-dsg/WHALE-data/domain_specific/domain_logs/hcard_domains.log', 'w', encoding='utf-8') as log_file:
    for filename, row_count in file_row_counts.items():
        log_file.write(f'{filename}: {row_count}\n')
        
print(f'***** Logs written at: domain_logs/hcard_domains.log *****')