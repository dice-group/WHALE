import re
from tqdm import tqdm
import logging
import os
import argparse

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_line(line):
    # Check for literal objects and skip the line if found
    if '"' in line:
        return None
    line = re.sub(r'_:([a-zA-Z0-9]+)', r'<http://whale.data.dice-research.org/resource#\1>', line) # Replace blank node identifiers
    line = re.sub(r' <[^>]+>\s*\.$', ' .', line) # Remove context URI
    return line

def count_output_lines(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return sum(1 for _ in file)

def write_progress(progress_file, line_number):
    with open(progress_file, 'w') as pfile:
        pfile.write(str(line_number))

def process_file(input_file, output_file, progress_file):
    try:
        last_line_number = count_output_lines(output_file)
        write_progress(progress_file, last_line_number)  # Initialize progress log with the starting line

        # if os.path.exists(progress_file):
        #     with open(progress_file, 'r') as pfile:
        #         last_line_number = int(pfile.read().strip() or 0)

        # Get the size of the file in bytes for the progress bar
        file_size = os.path.getsize(input_file)
        progress_bar = tqdm(desc="Materializing blank nodes", total=file_size, unit='B', unit_scale=True, unit_divisor=1024, initial=last_line_number)

        with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'a' if last_line_number > 0 else 'w', encoding='utf-8') as outfile:
            for line_number, line in enumerate(infile, 1):
                if line_number <= last_line_number:
                    progress_bar.update(len(line.encode('utf-8')))
                    continue
                transformed_line = transform_line(line)
                if transformed_line:
                    outfile.write(transformed_line)
                progress_bar.update(len(line.encode('utf-8')))
                write_progress(progress_file, line_number)  # Update progress log
        
        progress_bar.close()
        logging.info(f'File processed successfully: {input_file} -> {output_file}')
    except Exception as e:
        logging.error(f'Error processing file {input_file}: {str(e)}')
        progress_bar.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process N-Quads files.')
    parser.add_argument('--i', type=str, required=True, help='Input file path')
    parser.add_argument('--o', type=str, required=True, help='Output file path')
    parser.add_argument('--p', type=str, help='Progress log file path (optional)')
    args = parser.parse_args()

    if not args.p:
        args.p = os.path.join(os.path.dirname(args.o), 'progress.log')
        
    input_file_path = args.i
    output_file_path = args.o
    progress_log_path = args.p

    if not os.path.exists(output_file_path):
        open(output_file_path, 'w').close()  # Create the file if it does not exist

    process_file(input_file_path, output_file_path, progress_log_path)

