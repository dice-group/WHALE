import csv
import sys

csv.field_size_limit(sys.maxsize)

input_file = "/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/datasets/pharm_gkb/chemicals/chemicals.tsv"
output_file = "/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/datasets/pharm_gkb/chemicals_classes.nt"

with open(input_file, 'r', encoding='utf-8') as tsv_file, open(output_file, 'w', encoding='utf-8') as nt_file:
    tsv_reader = csv.DictReader(tsv_file, delimiter='\t')
    triples_set = set()

    for row in tsv_reader:
        entity2_id = row['PharmGKB Accession Id']
        entity2_name = row['Name'].replace('"', '')
        # entity2_symbol = row['Generic Names']
        # entity2_trade = row['Trade Names']
        # entity2_symbol_cleaned = entity2_symbol.replace('"', '')
        entity2_type = 'chemical'

        subject = f"<https://www.pharmgkb.org/{entity2_type}/{entity2_id}>"
        predicate = "<http://www.w3.org/2002/07/owl#Class>"
        object_ = f"{entity2_name}"

        # parts_object = [entity2_name, entity2_trade]
        # non_empty_parts = [part for part in parts_object if part]
        # object_ = "; ".join(non_empty_parts)

        triple = f"{subject} {predicate} \"{object_}\" ."

        if triple not in triples_set:
            triples_set.add(triple)

    for triple in triples_set:
        nt_file.write(triple + '\n')

print(f"Conversion completed. The triples have been written to {output_file}")
