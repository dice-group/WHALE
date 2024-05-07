import os
import logging
import sys
from tqdm import tqdm

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Default list of directories to process
default_directories = ["adr", "geo", "hcalendar", "hcard", "hlisting", "hrecipe", "hresume", "hreview", "hspecies", "jsonId", "jsonld", "microdata", "xfn"]

# Check for command-line arguments
if len(sys.argv) > 1:
    # Directories specified by the user
    directories = sys.argv[1:]  # Takes all arguments except the script name
else:
    # No specific directories specified; process all default directories
    directories = default_directories

for directory_name in directories:
    # Validate if directory exists
    if not os.path.exists(directory_name) or not os.path.isdir(directory_name):
        logging.error(f"Directory {directory_name} does not exist or is not a directory.")
        continue

    directory_path = directory_name  # Assuming these directories are at the specified path
    output_filename = f'dpef.html-{directory_name}.nq-all'
    processed_files_record = f'processed_files_{directory_name}.txt'

    # Paths to important files
    output_path = os.path.join(directory_path, output_filename)
    processed_files_path = os.path.join(directory_path, processed_files_record)

    # Attempt to load the set of already processed files, if the record exists
    if os.path.exists(processed_files_path):
        with open(processed_files_path, 'r') as file:
            processed_files = set(file.read().splitlines())
    else:
        processed_files = set()

    # Adjust file pattern based on directory
    if directory_name == 'jsonld':
        file_pattern = 'dpef.html-embedded-jsonld.nq-'
    elif directory_name == 'microdata':
        file_pattern = 'dpef.html-microdata.nq-'
    else:
        file_pattern = f'dpef.html-mf-{directory_name}.nq-'

    files = [f for f in os.listdir(directory_path) if f.startswith(file_pattern)]
    total_files = len(files)
    
    progress_bar = tqdm(total=total_files, desc=f"Combining Files in {directory_name}", unit="file")

    # Open the output file in append mode once to avoid overwriting content
    with open(output_path, 'a') as outfile:
        # Loop through each file in the directory
        for filename in files:
            if filename in processed_files:
                logging.info(f'{filename} has already been processed; skipping.')
                progress_bar.update(1)
                continue
            
            # Path to the current file
            path = os.path.join(directory_path, filename)
            try:
                # Open the current file in read mode
                with open(path, 'r') as infile:
                    content = infile.read()
                    outfile.write(content)
                # Add the filename to the processed files set
                processed_files.add(filename)
                logging.info(f'Successfully processed {filename}')
            except Exception as e:
                logging.error(f'Error processing {filename}: {e}')
            progress_bar.update(1)

    # After all files are processed, update the record of processed files
    with open(processed_files_path, 'w') as file:
        for filename in processed_files:
            file.write(f"{filename}\n")

    progress_bar.close()
    logging.info(f"All files in {directory_name} have been combined into {output_path}")

    # Deleting the processed files
    for filename in files:
        path = os.path.join(directory_path, filename)
        try:
            os.remove(path)
            logging.info(f"Deleted {filename}")
        except Exception as e:
            logging.error(f"Error deleting {filename}: {e}")
