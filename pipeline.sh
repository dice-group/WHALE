# This pipeline will do the following:

# (1) Get the datasets (individual paths/directory path) from the user
# (2) Give this path to dice-embeddings to create embeddings

# - From here the paths diverge

# Path A: (3) Use evaluation mode directly on the dataset which has `dataset_1` in their name to get the evaluation results from link prediction
# Path B: (3) Create class and property alignment from AML tool.
#         (4) Create config files for the dataset for Limes.
#         (5) Create sameAs links from Limes.
#         (6) Give the embeddings results and Limes result to Procrustes algorithm.
#         (7) Provide the input to link prediction.

#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
CONFIG_DIR=$"$SCRIPT_DIR/LIMES/configs"
LIMES_OUTPUT=$"$SCRIPT_DIR/Alignment/procrustes/limes"
AML_INPUT=$"$SCRIPT_DIR/AML_v3.2/data/input"

# Function to display usage
usage() {
    echo "Usage: $0 [path1] [path2] ..."
    echo "Provide individual dataset paths or directory containing datasets."
    exit 1
} # TODO: Might change based on the input of AML (class, property lists), Limes or Procrustes

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
    git checkout whale-pipeline-evaluation
    pip3 install -e .["dev"] # Comment it out if using on Noctua 2. Only to be used if running on local machine
    echo "Generating embeddings for $dataset_path..."
    
    # TODO: SHIVAM
    # - Add command to initiate dice embeddings
    dicee --path_single_kg "$dataset_path" --model Keci --num_epochs 500 --p 0 --q 1 --embedding_dim 256 --scoring_technique NegSample --batch_size 100_000 --optim Adopt --disable_checkpointing --trainer PL --backend rdflib
    
    echo "Training embeddings completed for $dataset_path."
done

# Step 5: Generate sameAs links using Limes
echo "Running Limes to generate sameAs links..."
# TODO: should be ucomment for local testing
# - Add command to download limes 
# if [ ! -d "$SCRIPT_DIR/LIMES" ]; then
#     echo "Cloning LIMES repository..."
#     git clone https://github.com/dice-group/LIMES.git
# else
#     echo "LIMES repository already exists."
# fi

# cd $SCRIPT_DIR/LIMES

# echo "Building the LIMES project using Maven..."

# if ! command -v mvn &> /dev/null
# then
#     echo "Maven is not installed. Installing Maven..."
#     sudo apt-get update
#     sudo apt-get install maven -y

#     if command -v mvn &> /dev/null
#     then
#         echo "Maven has been installed successfully."
#     else   
#         echo "Maven installation failed. Exiting the script."
#         exit 1
#     fi
# else
#     echo "Maven is already installed."
# fi

# mvn clean package shade:shade -Dmaven.test.skip=true

# # - Run Limes on the config files.
# if [ $? -eq 0]; then
#     echo "LIMES built successfully."

python $SCRIPT_DIR/WDC_scripts/linking_scripts/limes/linking/src/main.py \
--source_endpoint ${dataset_paths[0]} \
--target_endpoint ${dataset_paths[1]}

# else
#     echo "LIMES build failed."
#     exit 1
# fi

# Step 6: Run Procrustes algorithm
echo "Running Procrustes algorithm..."
# Define paths for the process
embedding_folder="./Alignment/procrustes/embeddings_folder"
alignment_dict_path="./Alignment/procrustes/pre_aligned"
alignmentlimes_dict_path="./Alignment/procrustes/limes"
output_folder="./Alignment/procrustes/output"

# Run the Python script using relative paths and initialize the model
python3 ./Alignment/procrustes/procrustes.py \
    --embedding_folder "$embedding_folder" \
    --alignment_dict_path "$alignment_dict_path" \
    --alignmentlimes_dict_path "$alignmentlimes_dict_path" \
    --output_folder "$output_folder"

# Step 7: Provide input to link prediction
echo "Running Evaluation on Procrustes..."
# Update the evaluation script based on the input from procrustes
python3 evaluation.py


echo "Pipeline execution complete."
