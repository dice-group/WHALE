
# RDF Processor Script

This script processes RDF data based on user input and generates configuration files for the LIMES link discovery framework. It supports processing all subdirectories or specific source and target graphs, and can utilize property and class mapping files.

## Prerequisites

- Python 3.6 or higher
- The following Python libraries:
  - `rdflib`
  - `tqdm`
  - `SPARQLWrapper`
  - `pandas`

You can install the required libraries using `pip`:

```bash
pip install rdflib tqdm SPARQLWrapper pandas
```

Firstly, if you cannot find `all_prefix.csv` in `/WHALE/WDC_scripts/linking_scripts/limes/raw_data/`, directory, you need to download it first and move it to `/WHALE/WDC_scripts/linking_scripts/limes/raw_data/` path:

```
wget https://files.dice-research.org/datasets/WHALE/WDC/linking_data/all_prefix.csv
```

## Usage

```bash
python rdf_processor.py [action] [options]
```

### Actions

- `all`: Process all subdirectories in the base directory.
- `specific`: Process specific source and target graphs.

### Options

- `-p`, `--path`:  
  Specify the path to an input directory or input KG file when using the `specific` action.

- `-c`, `--config_output_path` (required):  
  Specify the path to a directory for storing the configuration files.

- `-o`, `--output_path` (required):  
  Specify the path to a directory for output files.

- `-pm`, `--property_mapping`:  
  Path to the property mapping file.

- `-cm`, `--class_mapping`:  
  Path to the class mapping file.

- `--use_endpoint`:  
  Use the Wikidata endpoint for the target graph when action is `specific`.

- `--source_graph`:  
  Path to the source graph when action is `specific`.

- `--target_graph`:  
  Path to the target graph when action is `specific`.

## Examples

### Processing All Subdirectories
To process all subdirectories in the base directory:

```bash
python rdf_processor.py all -c path/to/config/output -o path/to/output
```

### Processing Specific Graphs with Target Graph File
To process specific source and target graphs using files:

```bash
python rdf_processor.py specific -c path/to/config/output -o path/to/output --source_graph path/to/source_graph.nt --target_graph path/to/target_graph.nt
```

### Processing Specific Graphs Using Endpoint
To process a specific source graph and use the Wikidata endpoint as the target graph:

```bash
python rdf_processor.py specific -c path/to/config/output -o path/to/output --source_graph path/to/source_graph.nt --use_endpoint
```

### Using Property and Class Mappings
If you have property and class mapping files, you can include them as follows:

```bash
python rdf_processor.py specific -c path/to/config/output -o path/to/output --source_graph path/to/source_graph.nt --target_graph path/to/target_graph.nt -pm path/to/property_mapping.nt -cm path/to/class_mapping.nt
```

## Notes

- The script expects RDF files in N-Quads (.nq), N-Triples (.nt), or plain text (.txt) format.
- When using the `specific` action, you must provide at least the `--source_graph` or `--path` option.
- If neither `--target_graph` nor `--use_endpoint` is specified for the `specific` action, the script will throw an error.
- The class and property mapping files should contain triples in the form:

```turtle
<source_class_or_property> <owl:equivalentClass_or_equivalentProperty> <target_class_or_property> .
```

## Output

- **Configuration Files**: Generated in the directory specified by `--config_output_path`.
- **Output Files**: Results of the processing are saved in the directory specified by `--output_path`.
