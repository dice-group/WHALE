import logging
from tqdm import tqdm
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def lowercase_file_with_progress(input_file, output_file):
    file_size = os.path.getsize(input_file)
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile, \
         tqdm(total=file_size, unit='B', unit_scale=True, desc="Processing") as progress_bar:

        for line in infile:
            lowercased_line = line.lower()
            outfile.write(lowercased_line)
            progress_bar.update(len(line.encode('utf-8')))
    
    logging.info("File processing completed.")

input_filename = "wdc_oct_23.nq-all"
output_filename = "wdc_oct_23_preprocessed.nq-all"
lowercase_file_with_progress(input_filename, output_filename)
