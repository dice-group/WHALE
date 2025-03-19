import os
import logging

def clean_relation(relation: str) -> str:
    return relation.replace("https://", "http://").rstrip('/')

def detect_format(line: str) -> str:
    parts = line.split()
    if len(parts) == 3 and parts[2].replace('.', '', 1).isdigit():
        return 'similarity'
    elif len(parts) == 4 and parts[1] == '<http://www.w3.org/2002/07/owl#sameAs>':
        return 'nt'
    return None

def merge_alignments(input_dir: str) -> None:
    unique_triples = set()

    files = [ 
        f for f in os.listdir(input_dir)
        if os.path.isfile(os.path.join(input_dir, f))
    ]
    if not files:
        logging.error('No files found in the directory')
        return 

    file_format = None
    for filename in os.listdir(input_dir):
        if not 'same_as' in filename:
            continue

        file_path = os.path.join(input_dir, filename)
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    if file_format is None:
                        file_format = detect_format(line)
                    
                    parts = line.split()
                    if file_format == 'nt' and len(parts) >= 3: 
                        s, r, o = parts[0], parts[1], ' '.join(parts[2:-1])
                        cleaned_relation = clean_relation(r)
                        unique_triples.add((s, cleaned_relation, o))
                    elif file_format == 'similarity' and len(parts) == 3:
                        s, o, conf = parts[0], parts[1], parts[2]
                        unique_triples.add((s, o, conf))

    output_file = os.path.join(input_dir, "same_as_total.nt")

    if unique_triples:
        with open(output_file, 'w') as f_out:
            for triple in unique_triples:
                if file_format == 'nt':
                    s, r, o = triple
                    f_out.write(f'{s} {r} {o} .\n')
                elif file_format == 'similarity':
                    s, o, conf = triple
                    f_out.write(f'{s}\t{o}\t{conf}\n')
        logging.info(f'Merged N_Triples file created at: {output_file}')
    else:
        logging.error('No triples found in the files.')
