import os
import logging
import argparse
from tqdm import tqdm

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Convert N-Quads to modified triples format and delete the original file.")
parser.add_argument("-d", "--directory", type=str, help="Specify the directory to process. Processes all default directories if not specified.")
args = parser.parse_args()

# Define the predicate to be used for linking subjects to their sources
new_predicate = "<http://whale.data.dice-research.org/ontology/hasQuadObject>"

# Function to parse a line into its components, considering objects with whitespaces
def parse_line(line):
    parts = []
    temp = ""
    in_quotes = False
    escape_next_char = False

    for char in line:
        if escape_next_char:
            temp += char
            escape_next_char = False
        elif char == "\\":
            escape_next_char = True
            temp += char
        elif char == '"' and not in_quotes:
            in_quotes = True
            temp += char
        elif char == '"' and in_quotes:
            in_quotes = False
            temp += char
        elif char == ' ' and not in_quotes:
            if temp:
                parts.append(temp)
                temp = ''
            continue
        else:
            temp += char

    if temp:
        parts.append(temp)

    if len(parts) >= 4:
        subject, predicate, object, source = parts[:4]
        # Clean up the object and source components to remove trailing characters
        object = object.strip(" .")
        source = source.strip(" .")
        return subject, predicate, object, source
    else:
        raise ValueError("Line does not conform to N-Quads format: " + line)


# Default list of directories to process, if none specified
default_directories = ["adr", "geo", "hcalendar", "hcard", "hlisting", "hrecipe", "hresume", "hreview", "hspecies", "jsonId", "jsonld", "microdata", "xfn"]

# Determine directories to process based on command-line argument
directories = [args.directory] if args.directory else default_directories

for directory_name in directories:
    input_filename = f'dpef.html-{directory_name}.nq-all'
    output_filename = f'dpef.html-{directory_name}.triples-all'

    input_path = os.path.join(directory_name, input_filename)
    output_path = os.path.join(directory_name, output_filename)

    # Keep track of subject-source pairs to avoid duplicating triples
    processed_subject_sources = set()

    if os.path.exists(input_path):
        # Open the file and wrap it with tqdm for a progress bar
        with open(input_path, 'r') as infile:
            # Get the file size for tqdm's total parameter, providing progress relative to the file size
            total_size = os.stat(input_path).st_size
            with tqdm(desc=f"Processing {directory_name}", total=total_size, unit='B', unit_scale=True, unit_divisor=1024) as pbar:
                with open(output_path, 'w') as outfile:
                    for line in infile:
                        try:
                            subject, predicate, object, source = parse_line(line)
                            # Write the original triple
                            outfile.write(f"{subject} {predicate} {object} .\n")
                            # Check for new triple
                            subject_source_pair = (subject, source)
                            if subject_source_pair not in processed_subject_sources:
                                outfile.write(f"{subject} {new_predicate} {source} .\n")
                                processed_subject_sources.add(subject_source_pair)
                        except Exception as e:
                            logging.error(f"Error processing line: {e}")
                        # Update tqdm progress by the length of the processed line
                        pbar.update(len(line.encode('utf-8')))
                
        logging.info(f"Completed processing for {directory_name}")
        # # After successful processing, delete the input file
        # try:
        #     os.remove(input_path)
        #     logging.info(f"Deleted original file: {input_path}")
        # except Exception as e:
        #     logging.error(f"Failed to delete original file {input_path}: {e}")
    else:
        logging.error(f"File {input_path} does not exist. Skipping.")

logging.info("Conversion and cleanup completed.")
