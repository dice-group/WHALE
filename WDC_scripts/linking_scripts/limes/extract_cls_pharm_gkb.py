import os
import xml.etree.ElementTree as etree

input_directory = "/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/datasets/pharm_gkb/pathways-biopax"
output_file = "/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/datasets/pharm_gkb/clinical_annotations_classes.nt"

namespaces = {
    "bp": "http://www.biopax.org/release/biopax-level3.owl#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
}

unique_triples = set()

for filename in os.listdir(input_directory):
    if filename.endswith(".owl"):
        file_path = os.path.join(input_directory, filename)

        try:
            tree = etree.parse(file_path)
            root = tree.getroot()

            for element in root.findall(".//", namespaces):
                standard_name = element.find("bp:standardName", namespaces)
                xref = element.find("bp:xref", namespaces)

                if xref is not None and standard_name is not None:
                    xref_resource = xref.attrib.get(f"{{{namespaces['rdf']}}}resource")
                    label = standard_name.text

                    if (
                        xref_resource and
                        "pathwayProcesses" not in xref_resource and
                        xref_resource.startswith("https://") and
                        "api" not in xref_resource
                    ):
                        triple = f'<{xref_resource}> <http://www.w3.org/2002/07/owl#Class> "{label}" .'
                        unique_triples.add(triple)
        
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

with open(output_file, "w") as nt_file:
    for triple in unique_triples:
        nt_file.write(triple + "\n")

print(f"Classes extracted and saved to {output_file}")
