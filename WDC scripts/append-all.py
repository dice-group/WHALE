import os
import logging
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_processed_files_info(processed_log):
    processed_files_info = {}
    if os.path.exists(processed_log):
        with open(processed_log, 'r') as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) == 2:
                    file_path, offset = parts
                    processed_files_info[file_path] = int(offset)
                else:
                    logging.error(f"Malformed log entry: {line}")
    return processed_files_info

def update_processed_file_log(processed_log, file_path, offset, error_occurred=False):
    mode = 'a' if not error_occurred else 'r+'
    with open(processed_log, mode) as log:
        if error_occurred:
            # Read current log, filter out the current file's previous entries
            lines = [line for line in log if not line.startswith(f"{file_path},")]
            log.seek(0)
            log.writelines(lines)
            log.truncate()
        log.write(f"{file_path},{offset}\n")

def combine_nq_files(directories, output_file, processed_log):
    processed_files_info = get_processed_files_info(processed_log)
    total_size = sum([os.path.getsize(os.path.join(dir, f)) for dir in directories 
                      for f in os.listdir(dir) if f.endswith('.nq-all') and os.path.join(dir, f) not in processed_files_info])

    with tqdm(total=total_size, desc="Combining .nq-all files", unit='B', unit_scale=True) as pbar:
        for directory in directories:
            dir_path = os.path.join('.', directory)
            if not os.path.exists(dir_path) or not os.path.isdir(dir_path):
                logging.warning(f"Directory does not exist: {dir_path}")
                continue

            for file in os.listdir(dir_path):
                if not file.endswith('.nq-all'):
                    continue

                full_path = os.path.join(dir_path, file)
                start_offset = processed_files_info.get(full_path, 0)

                try:
                    with open(full_path, 'r') as infile:
                        infile.seek(start_offset)
                        with open(output_file, 'a') as outfile:
                            while True:
                                line = infile.readline()
                                if not line:
                                    break
                                outfile.write(line)
                                pbar.update(len(line.encode('utf-8')))
                            # Update log to mark the file as fully processed
                            update_processed_file_log(processed_log, full_path, infile.tell())
                    # Delete the file after successful processing
                    os.remove(full_path)
                    logging.info(f"Deleted processed file: {full_path}")
                except Exception as e:
                    logging.error(f"Failed to process {full_path}: {e}")
                    # Log progress for partial completion
                    update_processed_file_log(processed_log, full_path, start_offset + pbar.n, error_occurred=True)
                    break

    logging.info("All .nq-all files have been combined and appended.")

directories = ["adr", "geo", "hcalendar", "hcard", "hlisting", "hrecipe", "hresume", "hreview", "jsonld", "microdata", "rdfa", "species", "xfn"]
output_file_path = 'wdc_oct_23.nq-all'
processed_files_log = 'processed_files.log'

combine_nq_files(directories, output_file_path, processed_files_log)

logging.info(f"Combined file created at: {output_file_path}")
