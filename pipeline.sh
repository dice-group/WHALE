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

SCRIPT_DIR=$(dirname "$(realpath "$0")")
CONFIG_DIR=$"$SCRIPT_DIR/LIMES/configs"

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
    git clone https://github.com/dice-group/dice-embeddings.git
    pip3 install -e .["dev"]
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
        # downloading AML tool
        if [ ! -d "$SCRIPT_DIR/AML_v3.2" ]; then
            wget https://github.com/AgreementMakerLight/AML-Project/releases/download/v3.2/AML_v3.2.zip \
            -O $SCRIPT_DIR/AML_v3.2.zip && unzip $SCRIPT_DIR/AML_v3.2.zip -d $SCRIPT_DIR
        else
            echo "AML_v3.2 is already installed. Skipping download and extraction."
        fi

        # - Run it to create class and property alignments using 
        # classes and properties from both datasets as an input
        python $SCRIPT_DIR/WDC_scripts/linking_scripts/limes/create_alignment.py \
        -c1 /scratch/hpc-prf-whale/albert/WHALE/WDC_scripts/linking_scripts/AML/AML_v3.2/data/test/test_cls_1.nt \
        -c2 /scratch/hpc-prf-whale/albert/WHALE/WDC_scripts/linking_scripts/AML/AML_v3.2/data/test/test_cls_2.nt \
        -p1 /scratch/hpc-prf-whale/albert/WHALE/WDC_scripts/linking_scripts/AML/AML_v3.2/data/test/test_props_1.nt \
        -p2 /scratch/hpc-prf-whale/albert/WHALE/WDC_scripts/linking_scripts/AML/AML_v3.2/data/test/test_props_2.nt

        # Step 4: Create config files for Limes
        echo "Creating config files for Limes..."
        # TODO: SHIVAM 
        # - Add command to run /WHALE/WDC_scripts/linking_scripts/limes/limes_config_extractor.py

        # Step 5: Generate sameAs links using Limes
        echo "Running Limes to generate sameAs links..."
        # TODO: ALBERT        
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

        for config_file in "$CONFIG_DIR"/*.xml; do
            if [ -f "$config_file" ]; then
                echo "Running LIMES with config file: $config_file"
                java -jar /scratch-n1/hpc-prf-dsg/WHALE-output/limes/limes-core-1.8.1-WORDNET.jar "$config_file"
            else 
                echo "No config files found in $CONFIG_DIR."
                break
            fi
        done
        # else
        #     echo "LIMES build failed."
        #     exit 1
        # fi

        # Step 6: Run Procrustes algorithm
        echo "Running Procrustes algorithm..."
        # TODO: DUYGU
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