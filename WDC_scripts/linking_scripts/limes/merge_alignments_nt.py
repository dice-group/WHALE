import sys
import os

def clean_relation(relation):
    return relation.replace("https://", "http://").rstrip('/')

def merge_nt_files(input_dir, output_dir):
    unique_triples = set()

    if not os.listdir(input_dir):
        print("No files found in the directory.")
        return 

    for filename in os.listdir(input_dir):
        if '_near' in filename:
            continue

        file_path = os.path.join(input_dir, filename)
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    parts = line.split(' ')
                    s, r, o = parts[0], parts[1], ' '.join(parts[2:-1])
                    cleaned_relation = clean_relation(r)
                    unique_triples.add((s, cleaned_relation, o))

    if unique_triples:
        with open(output_file, 'w') as f_out:
            for s, r, o in unique_triples:
                f_out.write(f'{s} {r} {o} .\n')
        print(f'Merged N_Triples file created at: {output_file}')
    else:
        print('No triples found in the files.')

if len(sys.argv) != 3:
    print("Usage: python merge_alignments_nt.py <input_directory> <output_nt_file>")
    sys.exit(1)

input_dir = sys.argv[1]
output_file = sys.argv[2]

merge_nt_files(input_dir, output_file)
