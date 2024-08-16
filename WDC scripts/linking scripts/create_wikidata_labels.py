import pandas as pd
from tqdm import tqdm
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the file paths
csv_file = 'limes/raw_data/wikidata_classes_by_language.csv'
output_file = 'limes/raw_data/wikidata_classes.nt'

# Read the CSV file
df = pd.read_csv(csv_file)

# Open the output file in write mode
with open(output_file, 'w', encoding='utf-8') as f:
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing rows"):
        s = row['Class']
        o = row['Class Label']
        triple = f'<{s}> <http://www.w3.org/2000/01/rdf-schema#label> "{o}" .\n'
        f.write(triple)
        # logging.info(f"Processed row {index + 1}/{df.shape[0]}: {triple.strip()}")

print(f"N-triples have been written to {output_file}")
logging.info(f"N-triples have been written to {output_file}")
