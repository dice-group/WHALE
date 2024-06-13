import os
import pandas as pd
from urllib.parse import urlparse

class DomainProcessor:
    def __init__(self, data_path, domain_file, output_dir):
        self.data_path = data_path
        self.domain_file = domain_file
        self.output_dir = output_dir
        self.output_dict = {}
        self.no_domain_lines = []

    def read_domains(self):
        with open(self.domain_file, 'r') as f:
            self.domains = [line.strip() for line in f]

    def get_base_url(self, url):
        url = url.strip('<>')
        parsed_url = urlparse(url)
        return f"{parsed_url.netloc}"

    def process_data(self):
        with open(self.data_path, 'r', encoding='utf-8') as file:
            for line in file:
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
                    self.no_domain_lines.append(part)
                    base_url = self.get_base_url(part)
                    if base_url not in self.output_dict:
                        self.output_dict[base_url] = []
                    self.output_dict[base_url].append(line)

    def save_results(self):
        os.makedirs(self.output_dir, exist_ok=True)
        for key, lines in self.output_dict.items():
            filename = f"{key.replace(':', '').replace('/', '_')}.txt"
            filepath = os.path.join(self.output_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                for line in lines:
                    f.write(line)
        print(f"Files have been created in the '{self.output_message}' directory.")

    def display_counts(self):
        domain_counts = {domain: len(lines) for domain, lines in self.output_dict.items()}
        for domain, count in sorted(domain_counts.items(), key=lambda item: item[1], reverse=True):
            print(f"{domain}: {count}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: python script.py <data_path> <domain_file> <output_dir>")
    else:
        data_path, domain_file, output_dir = sys.argv[1], sys.argv[2], sys.argv[3]
        processor = DomainProcessor(data_path, domain_file, output_dir)
        processor.read_domains()
        processor.process_data()
        processor.save_results()
        processor.display_counts()