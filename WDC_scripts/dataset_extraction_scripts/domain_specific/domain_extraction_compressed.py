import argparse
import glob
import os
import logging
import gzip
from urllib.parse import urlparse
import psutil
from tqdm import tqdm
from joblib import Parallel, delayed

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DomainProcessor:
    def __init__(self, domain_file, output_dir):
        self.domain_file = domain_file
        self.output_dir = output_dir
        self.domains = set()
        self.no_domain_lines = []

        base_name = os.path.basename(self.domain_file)
        format_name, _ = os.path.splitext(base_name)
        log_dir = "domain_logs"
        os.makedirs(log_dir, exist_ok=True)
        self.log_file = os.path.join(log_dir, f"{format_name}.log")

    def read_domains(self):
        try:
            with open(self.domain_file, "r") as f:
                self.domains.update(line.strip() for line in f)
            logging.info(f"Loaded domains from {self.domain_file}")
        except Exception as e:
            logging.error(f"Failed to read domain file: {e}")
            raise

    def get_base_url(self, url):
        url = url.strip("<>")
        parsed_url = urlparse(url)
        base_url = parsed_url.netloc

        if base_url.startswith("www."):
            base_url = base_url[4:]
        return base_url

    def process_data(self, file_path):
        process_id = os.getpid()
        logging.info(f"Process ID {process_id} is processing {file_path}")
        local_output_dict = {}
        with gzip.open(file_path, "rt") as file:
            progress = tqdm(
                total=1_510_000_000,
                unit="B",
                unit_scale=True,
                desc=f"Processing {os.path.basename(file_path)}",
            )
            for line in file:
                line_size = len(line.encode("utf-8"))
                parts = line.split()
                part = parts[-2]
                found_domain = False
                for domain in self.domains:
                    if domain in part:

                        if domain not in local_output_dict:
                            local_output_dict[domain] = []
                        local_output_dict[domain].append(line)
                        found_domain = True
                        break
                if not found_domain:
                    self.no_domain_lines.append(part)
                    base_url = self.get_base_url(part)
                    if base_url not in local_output_dict:
                        local_output_dict[base_url] = []
                    local_output_dict[base_url].append(line)
                progress.update(line_size)
            logging.info(f"Current Memory Usage {psutil.Process(os.getpid()).memory_info().rss / 1000000: .5} in MB")
            self.save_results(local_output_dict, os.path.basename(file_path))
            del local_output_dict
        logging.info(f"Finished processing and saving {file_path}")

    def save_results(self, local_output_dict, base_file_name):
        logging.info(f'Saving data from {base_file_name}')
        for domain, lines in tqdm(local_output_dict.items(), desc="Saving results", unit="domain"):
            file_path = os.path.join(self.output_dir, f"{domain}.txt")
            existing_lines = set()
            
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8', errors="ignore") as f:
                    existing_lines = set(f.readlines())
                
            with open(file_path, 'a', encoding='utf-8') as f:
                for line in lines:
                    if line not in existing_lines:
                        f.write(line)
            del existing_lines
        logging.info("Files have been written in the output directory.")

    def display_counts(self):
        domain_counts = {}
        for filename in glob.glob(f"{self.output_dir}/*.txt"):
            with open(filename, "r", encoding="utf-8") as file:
                count = sum(1 for _ in file)
                domain = os.path.basename(filename).replace(".txt", "")
                domain_counts[domain] = count

        with open(self.log_file, "w") as log_file:
            for domain, count in sorted(
                domain_counts.items(), key=lambda item: item[1], reverse=True
            ):
                log_file.write(f"{domain}: {count}\n")
        logging.info(f"Domain counts written to {self.log_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "file_path", type=str, help="Path to the directory containing data files."
    )
    parser.add_argument("domain_file", type=str, help="Path to the domain file.")
    parser.add_argument("output_dir", type=str, help="Path to the output directory.")

    args = parser.parse_args()
    data_path, domain_file, output_dir = (
        args.file_path,
        args.domain_file,
        args.output_dir,
    )

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(
            f"Output directory '{output_dir}' created because it did not exist."
        )
    else:
        logging.info(f"Output directory '{output_dir}' already exists.")
    processor = DomainProcessor(domain_file, output_dir)
    processor.read_domains()

    file_paths = sorted([os.path.join(data_path, f) for f in os.listdir(data_path) if f.endswith('.gz')])
    Parallel(n_jobs=-1)(delayed(processor.process_data)(file) for file in file_paths)
    # for file in tqdm(file_paths, desc="Files processed"):
        # processor.process_data(file)
    # processor.process_data(data_path)
    processor.display_counts()
