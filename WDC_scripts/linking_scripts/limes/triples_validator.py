import re

def is_valid_uri(uri):
    if not re.match(r'^<([^:]+:[^\s"<>[\](){}]*)>$', uri):
        return False

    if uri.count('#') > 1:
        return False

    return True

def is_roman_script(text):
    return bool(re.match(r'^[\x00-\x7F]+$', text))

def sanitize_literal(literal):
    return literal.replace('"', '').replace('\\', '')

def sanitize(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
        for line in infile:
            parts = line.split(' ')
            if len(parts) < 3:
                continue

            subject = parts[0]
            predicate = parts[1]
            label = ' '.join(parts[2:]).strip()
            label = label[:-2].strip()

            # if not is_roman_script(label):
            #     continue

            if not is_valid_uri(subject) or not is_valid_uri(predicate):
                continue

            if label.startswith('"') and label.endswith('"'):
                label = sanitize_literal(label[1:-1])
                label = f'"{label}"'
            elif not is_valid_uri(label):
                continue

            outfile.write(f'{subject} {predicate} {label} .\n')

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sanitize an N-Triples file.")
    parser.add_argument("input_file", help="Path to the input N-Triples file.")
    parser.add_argument("output_file", help="Path to the sanitized N-Triples file.")
    args = parser.parse_args()

    sanitize(args.input_file, args.output_file)
