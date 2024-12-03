import os
import sys

if len(sys.argv) != 2:
    print("Usage: python check_triples.py <directory>")
    sys.exit(1)

directory = sys.argv[1]
deleted_files_count = 0

for filename in os.listdir(directory):
    if filename.endswith(".nt"):
        file_path = os.path.join(directory, filename)
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read().strip()
            if not content:
                os.remove(file_path)
                deleted_files_count += 1
                
print(f'Triples are checked. {deleted_files_count} empty files deleted.')
