import csv
import os
import time
import logging
import json
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.error import HTTPError

from tqdm import tqdm

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

class SPARQLQueryExecutor:
    def __init__(self, endpoint_url, csv_file_path, language_file, limit=300, offset=6000):
        self.sparql = SPARQLWrapper(endpoint_url)
        self.csv_file_path = csv_file_path
        self.failed_class_exist = False
        self.languages = self.load_languages(language_file)
        self.limit = limit
        self.offset = offset
        self.total_classes = 0
        if os.path.exists(self.csv_file_path):
            self.total_classes = sum(1 for _ in open(self.csv_file_path, 'r', encoding='utf-8')) - 1
            self.existing_data = self.load_existing_data()
        else:
            self.total_classes = 0
            with open(self.csv_file_path, mode='w', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['Lang Code', 'Language', 'Class', 'Class Label'])
    
    def load_existing_data(self):
        data_set = set()
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                next(reader, None)  # Skip header
                for row in reader:
                    data_set.add((row[0], row[2]))
        except FileNotFoundError as e:
            raise e
        return data_set
    
    def load_languages(self, filepath):
        languages = {}
        if os.path.exists("limes/raw_data/wikidata_failed_lang.log"):
            with open("limes/raw_data/wikidata_failed_lang.log", "r") as f:
                failed_langs = [line.rstrip() for line in f]
            self.failed_class_exist = True
        with open(filepath, "r") as file:
            for line in file:
                if ':' in line:
                    key, value = line.split(':', 1)
                    if self.failed_class_exist:
                        if key.strip() in failed_langs:
                            languages[key.strip()] = value.strip()    
                    else:
                        languages[key.strip()] = value.strip()
        return languages

    def execute_safe_query(self):
        base_delay = 1 
        for attempt in range(5):
            try:
                response = self.sparql.query().response
                raw_data = response.read().decode('utf-8')
                results = json.loads(raw_data)
                return results
            except json.JSONDecodeError as e:
                logging.error(f"JSON decode failed on attempt {attempt + 1}: {e}")
                if "Invalid control character" in str(e):
                    continue
            except HTTPError as e:
                logging.error(f"HTTP error on attempt {attempt + 1}: {e}")
                if e.code == 429:  # Handle rate limiting
                    sleep_time = base_delay * 2 ** attempt
                    logging.info(f"Rate limit exceeded. Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    raise e
            except Exception as e:
                logging.error(f"Unhandled exception: {e}")
                return {}
    
    def csv_sanity_check(self, target_string, lang_code):
        return (lang_code, target_string) in self.existing_data

    def query_and_write(self):
        total_written = 0
        for lang_code in tqdm(self.languages, desc="Fetching classes"):
            consecutive_empty_batches = 0
            while True:
                batch_written = 0
                with open(self.csv_file_path, mode='a', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    self.sparql.setQuery(self._construct_query(lang_code))
                    self.sparql.setTimeout(600)
                    self.sparql.setReturnFormat(JSON)
                    results = self.execute_safe_query()
                    
                    if not results or "results" not in results or not results["results"]["bindings"]:
                        consecutive_empty_batches += 1
                    else:
                        consecutive_empty_batches = 0  # Reset if we get results
                        for result in results["results"]["bindings"]:
                            class_uri = result["class"]["value"]
                            class_label = result["classLabel"]["value"] if "classLabel" in result else ""
                            if self.csv_sanity_check(target_string=class_uri, lang_code=lang_code):
                                logging.debug(f"{(lang_code, class_uri)} already present. Skipping...")
                                continue
                            writer.writerow([lang_code, self.languages[lang_code], class_uri, class_label])
                            batch_written += 1
                            total_written += 1
                if consecutive_empty_batches >= 5:  # Break if two consecutive empty results
                    logging.info(f"No more data found for {lang_code} after 5 consecutive empty batches. Breaking loop.")
                    break
                self.offset += self.limit  # Increase offset for the next batch
                self.total_classes += total_written
                if self.failed_class_exist and self.offset > 9000:
                    logging.info(f"Failed language code: {lang_code} received till {self.offset - self.limit}. Breaking loop.")
                    break
                logging.info(f"Processed {batch_written} classes for language {self.languages[lang_code]} at offset {self.offset}.")
        logging.info(f"All languages processed. Total classes in the CSV file after updating: {self.total_classes}")

    def _construct_query(self, lang_code):
        return f"""
        SELECT ?class ?classLabel
        WHERE {{
            {{
                SELECT DISTINCT ?class WHERE {{
                    ?s wdt:P31 ?class .
                }} OFFSET {self.offset} LIMIT {self.limit}
            }}
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang_code}". }}
            OPTIONAL {{ ?class rdfs:label ?label }}
            BIND(COALESCE(?label, "No label") AS ?classLabel)
            FILTER (BOUND(?classLabel))
        }}
        """

if __name__ == "__main__":
    executor = SPARQLQueryExecutor(
        endpoint_url="https://query.wikidata.org/sparql",
        csv_file_path='limes/raw_data/wikidata_classes_by_language.csv',
        language_file="limes/raw_data/WDC_class_language.txt"
    )
    executor.query_and_write()
    logging.info("CSV file has been written successfully.")
