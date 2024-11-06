import chardet

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(10000))
        return result['encoding']

def sanitize(input_path, output):
    encoding = detect_encoding(input_path)
    print(f"Detected encoding: {encoding}")

    with open(input_path, 'r', encoding=encoding, errors='replace') as infile, \
         open(output, 'w', encoding='utf-8') as outfile:

        for line in infile:
            if not line.startswith('<'):
                continue
            parts = line.split(' ', 2)
            label = parts[2]

            if label.startswith('"'):
                sanitized = label.strip().strip('"').replace('"', '').rstrip(' .')
                outfile.write(f'{parts[0]} {parts[1]} "{sanitized}" .\n')
            else:
                outfile.write(f'{parts[0]} {parts[1]} {label}')

sanitize('WDC_scripts/linking_scripts/limes/datasets/bio2rdf/merged_bioportal.nq', 
         'WDC_scripts/linking_scripts/limes/datasets/bio2rdf/merged_bioportal_clean.nq')
