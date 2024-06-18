import argparse
import glob
import os
import logging
import gzip
from urllib.parse import urlparse

from tqdm import tqdm
from joblib import Parallel, delayed

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DomainProcessor:
    def __init__(self, domain_file, output_dir):
        self.domain_file = domain_file
        self.output_dir = output_dir
        self.domains = set()
        self.no_domain_lines = []
        
        base_name = os.path.basename(self.domain_file)  # Extract the filename from the path
        format_name, _ = os.path.splitext(base_name)  # Remove the extension from the filename
        log_dir = 'domain_logs'
        os.makedirs(log_dir, exist_ok=True)
        self.log_file = os.path.join(log_dir, f'{format_name}.log')

    def read_domains(self):
        try:
            with open(self.domain_file, 'r') as f:
                self.domains.update(line.strip() for line in f)
            logging.info(f"Loaded domains from {self.domain_file}")
        except Exception as e:
            logging.error(f"Failed to read domain file: {e}")
            raise

    def get_base_url(self, url):
        url = url.strip('<>')
        parsed_url = urlparse(url)
        base_url = parsed_url.netloc
        # Remove 'www.' if present
        if base_url.startswith('www.'):
            base_url = base_url[4:]  # Skip the first 4 characters which are 'www.'
        return base_url

    def process_data(self, file_path):
        process_id = os.getpid()  # Get the current process ID
        logging.info(f"Process ID {process_id} is processing {file_path}") 
        local_output_dict = {}          
        with gzip.open(file_path, 'rt') as file:
            progress = tqdm(total=1_084_475_392, unit='B', unit_scale=True, desc=f"Processing {os.path.basename(file_path)}")
            for line in file:
                line_size = len(line.encode('utf-8'))
                parts = line.split()
                part = parts[-2]
                found_domain = False
                for domain in self.domains:
                    if domain in part:
                        # logging.debug(f'Domain {domain} found in {parts}')
                        if domain not in local_output_dict: # TODO: if there are multiple compressed files, check if there is already .txt files in the output dir, if there are, create the dictionary based on the base_url in the file titles and save the lines in the files.
                            local_output_dict[domain] = []
                        local_output_dict[domain].append(line) # TODO: change it to directly write in the file for larger files.
                        # logging.debug("Dictionary:")
                        # for domain, lines in local_output_dict.items():
                        #     logging.debug(f'{domain} : {len([line for line in lines])}')
                        found_domain = True
                        break
                if not found_domain:
                    self.no_domain_lines.append(part)
                    base_url = self.get_base_url(part)
                    if base_url not in local_output_dict: # TODO: if there are multiple compressed files, check if there is already .txt files in the output dir, if there are, create the dictionary based on the base_url in the file titles and save the lines in the files.
                        local_output_dict[base_url] = []
                    local_output_dict[base_url].append(line)
                progress.update(line_size)
        self.save_results(local_output_dict)  # Save after processing each file
        logging.info(f"Finished processing and saving {file_path}")
        
    def save_results(self, local_output_dict):
        for domain, lines in local_output_dict.items():
            file_path = os.path.join(self.output_dir, f"{domain}.txt")
            mode = 'a' if os.path.exists(file_path) else 'w'
            with open(file_path, mode, encoding='utf-8') as f:
                f.writelines(lines)
        logging.info("Files have been written in the output directory.")

    def display_counts(self):
        domain_counts = {}
        for filename in glob.glob(f"{self.output_dir}/*.txt"):
            with open(filename, 'r', encoding='utf-8') as file:
                count = sum(1 for _ in file)
                domain = os.path.basename(filename).replace('.txt', '')
                domain_counts[domain] = count
        
        # Writing counts to the log file
        with open(self.log_file, 'w') as log_file:
            for domain, count in sorted(domain_counts.items(), key=lambda item: item[1], reverse=True):
                log_file.write(f"{domain}: {count}\n")
        logging.info(f"Domain counts written to {self.log_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_path", type=str, help="Path to the directory containing data files.")
    parser.add_argument("domain_file", type=str, help="Path to the domain file.")
    parser.add_argument("output_dir", type=str, help="Path to the output directory.")
    
    args = parser.parse_args()
    data_path, domain_file, output_dir = args.data_path, args.domain_file, args.output_dir

# datasets = [
#     ("raw_data/rdfa", "domain_files/rdfa_domains.txt", "domain_dataset/rdfa_dataset"),
#     ("raw_data/microdata", "domain_files/microdata_domains.txt", "domain_dataset/microdata_dataset"),
#     ("raw_data/hcard", "domain_files/hcard_domains.txt", "domain_dataset/hcard_dataset"),
#     ("raw_data/jsonld", "domain_files/jsonld_domains.txt", "dprocess_fileomain_dataset/jsonld_dataset")
# ]
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Output directory '{output_dir}' created because it did not exist.")
    else:
        logging.info(f"Output directory '{output_dir}' already exists.")
    processor = DomainProcessor(domain_file, output_dir)
    processor.read_domains()
    
    file_paths = [os.path.join(data_path, f) for f in os.listdir(data_path) if f.endswith('.gz')]
    Parallel(n_jobs=-1)(delayed(processor.process_data)(file) for file in file_paths)
    processor.display_counts()

