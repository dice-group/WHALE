import os
import sys

if len(sys.argv) != 2:
    print("Usage: python txt_to_nt.py <directory>")
    sys.exit(1)

directory = sys.argv[1]

if not os.path.isdir(directory):
    print(f"The directory '{directory}' does not exist.")
    sys.exit(1)

for filename in os.listdir(directory):
    if filename.endswith('.txt'):
        txt_file = os.path.join(directory, filename)

        nt_file = os.path.join(directory, filename[:-4] + '.nt')

        with open(txt_file, 'r') as file:
            content = file.read()

        with open(nt_file, 'w') as file:
            file.write(content)

        os.remove(txt_file)

print("Conversion completed!")
