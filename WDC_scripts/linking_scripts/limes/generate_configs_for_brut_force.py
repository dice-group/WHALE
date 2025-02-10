import pandas as pd
import os
from urllib.parse import urlparse

CONFIG_TEMPLATE = '''<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<!DOCTYPE LIMES SYSTEM "limes.dtd">
<LIMES>
  <PREFIX>
    <NAMESPACE>http://www.w3.org/1999/02/22-rdf-syntax-ns#</NAMESPACE>
    <LABEL>rdf</LABEL>
  </PREFIX>
  <PREFIX>
    <NAMESPACE>http://www.w3.org/2000/01/rdf-schema#</NAMESPACE>
    <LABEL>rdfs</LABEL>
  </PREFIX>
  <PREFIX>
    <NAMESPACE>http://www.w3.org/2002/07/owl#</NAMESPACE>
    <LABEL>owl</LABEL>
  </PREFIX>
  <PREFIX>
    <NAMESPACE>http://dbpedia.org/ontology/</NAMESPACE>
    <LABEL>dbo</LABEL>
  </PREFIX>
  
  <SOURCE>
    <ID>source</ID>
    <ENDPOINT>http://n2cn0301:3030/dbpedia_en</ENDPOINT>
    <VAR>?s</VAR>
    <PAGESIZE>-1</PAGESIZE>
    <RESTRICTION>?s a dbo:{class_name}</RESTRICTION>
    <PROPERTY>rdfs:label AS nolang->lowercase</PROPERTY>
  </SOURCE>
  
  <TARGET>
    <ID>target</ID>
    <ENDPOINT>http://n2cn0301:3030/dbpedia_de</ENDPOINT>
    <VAR>?t</VAR>
    <PAGESIZE>-1</PAGESIZE>
    <RESTRICTION>?t a dbo:{class_name}</RESTRICTION>
    <PROPERTY>rdfs:label AS nolang->lowercase</PROPERTY>
  </TARGET>

  <MLALGORITHM>
    <NAME>wombat simple</NAME>
    <TYPE>unsupervised</TYPE>
    <PARAMETER>
        <NAME>atomic measures</NAME>
        <VALUE>jaccard, trigrams, cosine</VALUE>
    </PARAMETER>
  </MLALGORITHM>
  
  <ACCEPTANCE>
    <THRESHOLD>0.7</THRESHOLD>
    <FILE>/scratch/hpc-prf-whale/albert/WHALE/LIMES/output/leaf_classes_dbpedia_de/{class_name}_same_as.nt</FILE>
    <RELATION>owl:sameAs</RELATION>
  </ACCEPTANCE>
  
  <REVIEW>
    <THRESHOLD>0.0</THRESHOLD>
    <FILE>/scratch/hpc-prf-whale/albert/WHALE/LIMES/output/leaf_classes_dbpedia_de/{class_name}_near.nt</FILE>
    <RELATION>owl:near</RELATION>
  </REVIEW>
  
  <EXECUTION>
    <REWRITER>default</REWRITER>
    <PLANNER>default</PLANNER>
    <ENGINE>default</ENGINE>
  </EXECUTION>
  
  <OUTPUT>CSV</OUTPUT>
</LIMES>
'''

def extract_class_name(uri):
    return uri.rstrip('/').split('/')[-1]

def generate_config(class_uri, output_dir):
    # class_name = extract_class_name(class_uri)
    class_name = class_uri
    config_content = CONFIG_TEMPLATE.format(class_name=class_name)

    filename = f"{class_name}_config.xml"
    file_path = os.path.join(output_dir, filename)

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(config_content)
    print(f"Generated config for class '{class_name}': {file_path}")

def main():
    csv_file = '/scratch/hpc-prf-whale/DBpedia/raw_data/en/tdb/leaf_classes.csv'

    output_directory = '/scratch/hpc-prf-whale/albert/WHALE/LIMES/configs/leaf_classes_dbpedia_de'

    df = pd.read_csv(csv_file)

    for index, row in df.iterrows():
        class_uri = row['class'].strip()
        generate_config(class_uri, output_directory)

if __name__ == "__main__":
    main()
