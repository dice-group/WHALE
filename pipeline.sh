# This pipeline will do the following:

# (1) Get the datasets (individual paths/directory path) from the user
# (2) Give this path to dice-embeddings to create embeddings

# - From here the paths diverge

# Path A: (3) Use evaluation mode directly on the dataset which has `dataset_1` in their name to get the evaluation results from query generator
# Path B: (3) Create class and property alignment from AML tool.
#         (4) Create config files for the dataset for Limes.
#         (5) Create sameAs links from Limes.
#         (6) Give the embeddings results and Limes result to Procrustes algorithm.
#         (7) Provide the input to query generator.

#!/bin/bash

# Function to display usage
usage() {
    echo "Usage: $0 [path1] [path2] ..."
    echo "Provide individual dataset paths or directory containing datasets."
    exit 1
} # TODO: Might change based on the input of AML, Limes or Procrustes

# Ensure at least one path is provided
if [ $# -eq 0 ]; then
    usage
fi

# Get the dataset paths from the user
dataset_paths=("$@")

# Iterate over each dataset path
for dataset_path in "${dataset_paths[@]}"; do
    echo "Processing dataset: $dataset_path"

    # Check if the dataset exists
    if [ ! -e "$dataset_path" ]; then
        echo "Dataset path $dataset_path does not exist. Skipping."
        continue
    fi

    # Step 2: Generate embeddings with dice-embeddings
    echo "Installing dice-embeddings..."
    git clone https://github.com/dice-group/dice-embeddings.git # Comment it out if using on Noctua 2. Only to be used if running on local machine
    pip3 install -e .["dev"] # Comment it out if using on Noctua 2. Only to be used if running on local machine
    echo "Generating embeddings for $dataset_path..."
    
    # TODO: SHIVAM
    # - Add command to initiate dice embeddings
    # dicee --path_single_kg "$dataset_path" --model Keci --num_epochs 500 --p 0 --q 1 --embedding_dim 256 --scoring_technique NegSample --batch_size 100_000 --optim Adopt --disable_checkpointing --num_folds_for_cv 10 --trainer PL

    # Check if the dataset is for Path A or Path B
    if [[ $(basename "$dataset_path") == *"dataset_1"* ]]; then
        # Path A: Use evaluation mode directly
        echo "Running evaluation mode for $dataset_path..."
        # TODO: SHIVAM
        # - Add query generator evaluation
    else
        # Path B: Full pipeline
        echo "Running Path B pipeline for $dataset_path..."

        # Step 3: Create class and property alignment with AML tool
        echo "Running AML tool for class and property alignment..."
        # TODO: ALBERT
        # - Add downloading AML tool
        # - Run it to create class and property alignments

        # Step 4: Create config files for Limes
        echo "Creating config files for Limes..."
        # TODO: SHIVAM 
        # - Add command to run /WHALE/WDC_scripts/linking_scripts/limes/limes_config_extractor.py

        # Step 5: Generate sameAs links using Limes
        echo "Running Limes to generate sameAs links..."
        # TODO: ALBERT
        # - Add command to download limes 
        # - Run Limes on the config files.

        # Step 6: Run Procrustes algorithm
        echo "Running Procrustes algorithm..."
        # TODO: DUGUYE
        # - Add command to run procrastes algorithm
        # - Generate the model.pt from dicee KGE class

        # Step 7: Provide input to query generator
        echo "Running query generator..."
        # TODO: SHIVAM
        # - Add query generator evaluation
    fi

    echo "Processing completed for $dataset_path."
done

echo "Pipeline execution complete."
