import glob
import os
import logging
from urllib.parse import urlparse

from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DomainProcessor:
    def __init__(self, data_path, domain_file, output_dir):
        self.data_path = data_path
        self.domain_file = domain_file
        self.output_dir = output_dir
        self.output_dict = {}
        self.no_domain_lines = []
        log_dir = "domain_logs"

        base_name = os.path.basename(self.domain_file)
        format_name, _ = os.path.splitext(base_name)
        log_file = os.path.join(log_dir, f"{format_name}.log")
        self.count_logger = logging.getLogger("DomainCountLogger")
        handler = logging.FileHandler(log_file, mode="w")
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        self.count_logger.addHandler(handler)
        self.parameters = self.count_logger.propagate = False

    def read_domains(self):
        try:
            with open(self.domain_file, "r") as f:
                self.domains = [line.strip() for line in f]
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

    def process_data(self):
        try:
            total_size = os.path.getsize(self.data_path)
            logging.info(f"Start processing file of size {total_size} bytes.")

            with open(self.data_path, "r", encoding="utf-8") as file, tqdm(
                total=total_size, unit="B", unit_scale=True, desc="Processing File"
            ) as progress_bar:
                for line in file:
                    line_size = len(line.encode("utf-8"))
                    parts = line.split()
                    part = parts[-2]
                    found_domain = False
                    for domain in self.domains:
                        if domain in part:

                            if domain not in self.output_dict:
                                self.output_dict[domain] = []
                            self.output_dict[domain].append(line)

                            found_domain = True
                            break
                    if not found_domain:
                        logging.debug(f"Domain not found: {part} in line str: {line}")
                        self.no_domain_lines.append(part)
                        base_url = self.get_base_url(part)
                        if base_url not in self.output_dict:
                            self.output_dict[base_url] = []
                        self.output_dict[base_url].append(line)
                    progress_bar.update(line_size)
            logging.info("Data processing completed successfully.")
        except Exception as e:
            logging.error(f"Error during data processing: {e}")
            raise

    def save_results(self):
        for key, lines in self.output_dict.items():
            filename = f"{key.replace(':', '').replace('/', '_')}.txt"
            filepath = os.path.join(self.output_dir, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                for line in lines:
                    f.write(line)
        print(f"Files have been created in the '{self.output_dir}' directory.")

    def display_counts(self):
        domain_counts = {
            domain: len(lines) for domain, lines in self.output_dict.items()
        }
        for domain, count in sorted(
            domain_counts.items(), key=lambda item: item[1], reverse=True
        ):
            self.count_logger.info(f"{domain}: {count}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 4:
        print("Usage: python3 script.py <data_path> <domain_file> <output_dir>")
    else:
        data_dir = sys.argv[1]

        data_path, domain_file, output_dir = sys.argv[1], sys.argv[2], sys.argv[3]
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(
            f"Output directory '{output_dir}' created because it did not exist."
        )
    else:
        logging.info(f"Output directory '{output_dir}' already exists.")
    processor = DomainProcessor(data_path, domain_file, output_dir)
    processor.read_domains()
    processor.process_data()
    processor.save_results()
    processor.display_counts()
