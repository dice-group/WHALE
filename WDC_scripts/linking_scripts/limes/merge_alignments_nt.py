import sys

def clean_relation(relation):
    return relation.replace("https://", "http://").rstrip('/')

def merge_nt_files(file1, file2, output_file):
    unique_triples = set()

    with open(file1, 'r') as f1:
        for line in f1:
            line = line.strip()
            if line:
                parts = line.split(' ')
                s, r, o = parts[0], parts[1], ' '.join(parts[2:-1])
                cleaned_relation = clean_relation(r)
                unique_triples.add((s, cleaned_relation, o))
    
    with open(file2, 'r') as f2:
        for line in f2:
            line = line.strip()
            if line:
                parts = line.split(' ')
                s, r, o = parts[0], parts[1], ' '.join(parts[2:-1])
                cleaned_relation = clean_relation(r)
                unique_triples.add((s, cleaned_relation, o))

    with open(output_file, 'w') as f_out:
        for s, r, o in unique_triples:
            f_out.write(f'{s} {r} {o} .\n')

if len(sys.argv) != 4:
    print("Usage: python merge_alignments_nt.py <input_nt_file1> <input_nt_file2> <output_nt_file>")
    sys.exit(1)

file1 = sys.argv[1]
file2 = sys.argv[2]
output_file = sys.argv[3]

merge_nt_files(file1, file2, output_file)

print(f"Merged N-Triples file created at: {output_file}")
