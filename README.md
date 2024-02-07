# create_kg.py Execution Guide
`create_kg.py` is a Python script used for converting CSV data into a knowledge graph (KG) format. The script processes CSV files, cleans and converts data, and then generates RDF triples to form a KG serialized in Turtle format.

## Prerequisites
- Python 3.x
- Libraries: rdflib, pandas, tqdm
- CSV data files placed in a specified folder

## How to Run the Script
Execute the script from the command line:

```bash
python create_kg.py
```

If you want to specify the dataset folder:

```bash
python create_kg.py --dataset <folder_name>
```

Replace `<folder_name>` with the name of the folder containing your CSV files. This folder should be located in the same directory as the script.

For example, to run the script with CSV files in the default **Data** folder, simply execute:

```bash
python create_kg.py --dataset Data
```


### Arguments

- `--dataset`: Specifies the name of the folder containing the dataset. If not provided, the default folder used is **Data**. This folder should be located in the same directory as the script.

## Output

The script outputs a human readable Turtle file (`knowledge_graph.ttl`) and an ontology file (`knowledge_ontology.owl`) containing the knowledge graph generated from the CSV files. These files are stored in the same directory as the script.

## Challenges Addressed
### 1. Data Cleaning
Some CSV files in the dataset were empty and needed to be excluded from parsing to avoid errors. The script identifies and skips these empty files to ensure smooth processing.

### 2. Data Conversion
The data in the CSV files was initially in object form. Each entry in specific columns was converted to its corresponding datatype for proper handling. For instance:

- Column "name" values were converted to strings.
- Columns "ratings", "actual_price", and "discount_price" were converted to floats.
### 3. Data Preprocessing
Values in "discount_price" and "actual_price" had complex formats (e.g., "₹32,999"). The script was designed to:

- Extract the "₹" symbol for the `wo:hasSymbol` property in the KG.
- Convert the numerical part of these values to floats for accurate representation in the KG.
### 4. Triple Generation
Many entries contained empty values, especially in "ratings", "no_of_ratings", "actual_price", and "discount_price" columns. The script was tailored to avoid creating triples for entities lacking values in these critical fields. This approach ensured the KG contained only meaningful and complete data.
### 5. Execution time
Since data is very large, processing all of it takes a lot of time. The execution time to parse all the file took ~$1$ hour and the graph serialization to a `.ttl` and `.owl` file took ~$5$ hours in total.
