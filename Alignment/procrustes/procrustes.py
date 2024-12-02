"""
Procrustes Alignment Script

This script aligns embeddings from two datasets into a shared vector space using Procrustes alignment. 
It facilitates downstream tasks like link prediction, graph integration, and cross-graph analyses.

### Key Steps:
1. Load embeddings and mapping files from dataset folders.
2. Parse alignment dictionaries (e.g., LIMES or pre-aligned).
3. Train and test Procrustes alignment in both forward and reverse directions.
4. Merge aligned and non-aligned embeddings.
5. Save aligned embeddings and models for downstream tasks.

### Inputs:
- **Embedding Folder**: Contains subfolders for datasets and embedding files.
- **Alignment Dictionaries**: Provide known entity alignments (e.g., LIMES `sameAs` links).

### Outputs:
- Aligned embeddings saved in the output folder.
- Processed embeddings for tasks like link prediction.

Designed for scalability and interpretability, this script is optimized for large-scale graph embeddings.
"""

# Import external libraries
import torch
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from scipy.linalg import orthogonal_procrustes

# Import standard Python libraries
import os
import pickle
import argparse
import logging
from rdflib import Graph

# Import project-specific modules
from dicee import KGE, Keci, intialize_model

# Set up logging for better error handling
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def find_dataset_folders(embedding_folder):
    """Find and return paths for dataset folders in the embedding folder."""
    all_items = os.listdir(embedding_folder)

    # Filter for directories that represent datasets
    dataset_folders = [item for item in all_items if os.path.isdir(
        os.path.join(embedding_folder, item))]

    # Check if there are exactly 2 datasets
    if len(dataset_folders) != 2:
        raise ValueError(
            f"Expected exactly 2 datasets in {embedding_folder}, found {len(dataset_folders)}.")

    # Return the paths of the dataset folders
    dataset1_path = os.path.join(embedding_folder, dataset_folders[0])
    dataset2_path = os.path.join(embedding_folder, dataset_folders[1])

    return dataset1_path, dataset2_path


def extract_files_from_directory(directory):
    """Retrieve required files (model, entity_to_idx, relation_to_idx) from a directory."""
    model_path = os.path.join(directory, "model.pt")
    entity_to_id_path = os.path.join(directory, "entity_to_idx.p")
    relation_to_id_path = os.path.join(directory, "relation_to_idx.p")

    # Check if all required files exist
    for path in [model_path, entity_to_id_path, relation_to_id_path]:
        if not os.path.exists(path):
            logging.error(f"File not found: {path}")
            raise FileNotFoundError(f"Missing file: {path}")

    return model_path, entity_to_id_path, relation_to_id_path


def load_embeddings(model_path, entity_to_id_path, relation_to_id_path):
    """Load embeddings and mappings for entities and relations."""
    model_weights = torch.load(
        model_path, map_location='cpu', weights_only=True)
    entity_embeddings = model_weights['entity_embeddings.weight'].cpu(
    ).detach().tolist()
    relation_embeddings = model_weights['relation_embeddings.weight'].cpu(
    ).detach().tolist()

    with open(entity_to_id_path, 'rb') as f:
        ent_ids = pickle.load(f)

    with open(relation_to_id_path, 'rb') as f:
        rel_ids = pickle.load(f)

    sorted_ent_ids = sorted(ent_ids.items(), key=lambda x: x[1])
    sorted_entities = [item[0] for item in sorted_ent_ids]
    entity_embeddings_df = pd.DataFrame(
        entity_embeddings, index=sorted_entities)

    sorted_rel_ids = sorted(rel_ids.items(), key=lambda x: x[1])
    sorted_relations = [item[0] for item in sorted_rel_ids]
    relation_embeddings_df = pd.DataFrame(
        relation_embeddings, index=sorted_relations)

    return entity_embeddings_df, relation_embeddings_df


def remove_brackets_from_indices(embeddings_df):
    """Remove < and > from each index in the DataFrame."""
    cleaned_index = [uri.strip('<>') for uri in embeddings_df.index]
    embeddings_df.index = cleaned_index
    return embeddings_df


def build_alignment_dict(folder_path):
    """
    Build an alignment dictionary from all files in a folder using only subject and object URIs.

    Args:
        folder_path (str): Path to the folder containing alignment files (.nt, .ttl, .txt).

    Returns:
        dict: A dictionary mapping subject URIs to object URIs.
    """
    alignment_dict = {}

    # Check if the folder exists
    if not os.path.exists(folder_path):
        logging.error(f"Folder '{folder_path}' does not exist.")
        return alignment_dict

    if not os.listdir(folder_path):  # Folder is empty
        logging.warning(
            f"Alignment folder '{folder_path}' is empty. Skipping processing.")
        return alignment_dict

    file_paths = [os.path.join(folder_path, file)
                  for file in os.listdir(folder_path)]

    for file_path in file_paths:
        extension = os.path.splitext(file_path)[1].lower()

        try:
            if extension in ['.nt', '.ttl']:
                # Parse RDF-based files
                g = Graph()
                g.parse(file_path, format='nt' if extension == '.nt' else 'ttl')
                for subj, _, obj in g:  # Ignore the predicate
                    alignment_dict[str(subj).strip(
                        '<>')] = str(obj).strip('<>')
            elif extension == '.txt':
                # Parse custom text-based alignment files
                with open(file_path, 'r') as f:
                    for line in f:
                        parts = line.strip().split('\t')
                        if len(parts) == 2:
                            entity1, entity2 = parts
                            alignment_dict[entity1.strip(
                                '<>')] = entity2.strip('<>')
                        else:
                            logging.warning(
                                f"Skipping line (unexpected format): {line.strip()}")
            else:
                logging.warning(f"Unsupported file type: {file_path}")
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")

    return alignment_dict


def clean_alignment_dict(alignment_dict):
    """Cleans the alignment dictionary by removing any trailing '> .' characters in target URIs."""
    return {k: v.rstrip('> .') for k, v in alignment_dict.items()}


def create_train_test_matrices(alignment_dict, entity_embeddings1, entity_embeddings2, test_size=0.1):
    """Generates train and test matrices based on alignment dictionary."""
    filtered_alignment_dict = {
        k: v for k, v in alignment_dict.items() if k in entity_embeddings1.index and v in entity_embeddings2.index
    }

    if not filtered_alignment_dict:
        logging.warning(
            "No valid entries found in the alignment dictionary. Skipping train-test split.")
        return None, None, None, None

    # Perform train-test split
    train_ents, test_ents = train_test_split(
        list(filtered_alignment_dict.keys()), test_size=test_size, random_state=42)

    # Retrieve embeddings
    S_train = entity_embeddings1.loc[train_ents].values
    T_train = entity_embeddings2.loc[list(
        map(filtered_alignment_dict.get, train_ents))].values
    S_test = entity_embeddings1.loc[test_ents].values if test_ents else None
    T_test = entity_embeddings2.loc[list(
        map(filtered_alignment_dict.get, test_ents))].values if test_ents else None

    return S_train, T_train, S_test, T_test


def normalize_and_scale(data, reference_data=None):
    """Normalize and scale data using its mean and standard deviation, or those of a reference dataset."""
    if reference_data is None:
        reference_data = data

    mean = reference_data.mean(axis=0)
    scale = np.sqrt(((reference_data - mean) ** 2).sum() /
                    reference_data.shape[0])
    normalized_data = (data - mean) / scale

    return normalized_data, mean, scale


def apply_procrustes_both_directions(S_train, T_train, S_test=None, T_test=None, reverse=False):
    """
    Performs Procrustes alignment for embeddings in both forward and reverse directions.

    Args:
        S_train (np.ndarray): Source training embeddings.
        T_train (np.ndarray): Target training embeddings.
        S_test (np.ndarray, optional): Source test embeddings.
        T_test (np.ndarray, optional): Target test embeddings.
        reverse (bool, optional): If True, aligns T to S. Defaults to False.

    Returns:
        tuple: Scaled and aligned embeddings for train and test sets, along with the rotation matrix (R).
    """
    # Normalize and scale training embeddings
    scaled_S_train, mean_S, scale_S = normalize_and_scale(S_train)
    scaled_T_train, mean_T, scale_T = normalize_and_scale(T_train)

    # Compute Procrustes alignment
    if not reverse:
        R, _ = orthogonal_procrustes(scaled_S_train, scaled_T_train)
    else:
        R, _ = orthogonal_procrustes(scaled_T_train, scaled_S_train)

    # Normalize and scale test embeddings if provided
    scaled_S_test = None
    scaled_T_test = None
    if S_test is not None and T_test is not None:
        scaled_S_test, _, _ = normalize_and_scale(
            S_test, reference_data=S_train)
        scaled_T_test, _, _ = normalize_and_scale(
            T_test, reference_data=T_train)

    if not reverse:
        S_test_aligned = scaled_S_test @ R if scaled_S_test is not None else None
        S_train_aligned = scaled_S_train @ R
        return scaled_S_train, S_train_aligned, scaled_T_train, S_test_aligned, scaled_S_test, scaled_T_test, R
    else:
        T_test_aligned = scaled_T_test @ R if scaled_T_test is not None else None
        T_train_aligned = scaled_T_train @ R
        return scaled_T_train, T_train_aligned, scaled_S_train, T_test_aligned, scaled_T_test, scaled_S_test, R


def extract_non_aligned_embeddings(alignment_dict, embeddings1, embeddings2,
                                   S_train=None, T_train=None, use_test_scaling=False,
                                   S_test=None, T_test=None):
    """
    Extracts and normalizes embeddings of non-aligned entities or relations, with optional transformation.

    Args:
        alignment_dict (dict): Alignment dictionary for entities (use None for relations).
        embeddings1 (pd.DataFrame): Embeddings for the first dataset (entities or relations).
        embeddings2 (pd.DataFrame): Embeddings for the second dataset (entities or relations).
        S_train (np.ndarray): Training embeddings for dataset 1.
        T_train (np.ndarray): Training embeddings for dataset 2.
        use_test_scaling (bool): Whether to use test statistics for scaling.
        S_test (np.ndarray): Test embeddings for dataset 1.
        T_test (np.ndarray): Test embeddings for dataset 2.
        transform (tuple): Transformation matrices (R, R_reverse) for relations. None for entities.

    Returns:
        tuple: Normalized and optionally transformed embeddings for both datasets.
    """
    if S_train is None or T_train is None:
        raise ValueError(
            "Training data (S_train and T_train) must be provided.")

    # Use training or test statistics for normalization
    reference_data_S = S_test if use_test_scaling and S_test is not None else S_train
    reference_data_T = T_test if use_test_scaling and T_test is not None else T_train

    _, mean_S, scale_S = normalize_and_scale(
        S_train, reference_data=reference_data_S)
    _, mean_T, scale_T = normalize_and_scale(
        T_train, reference_data=reference_data_T)

    if alignment_dict:
        # For entities: Filter non-aligned embeddings
        embeddings1_non_aligned = list(
            set(embeddings1.index) - set(alignment_dict.keys()))
        embeddings2_non_aligned = list(
            set(embeddings2.index) - set(alignment_dict.values()))
    else:
        # For relations: Use all embeddings
        embeddings1_non_aligned = embeddings1.index
        embeddings2_non_aligned = embeddings2.index

    S_non_aligned = embeddings1.loc[embeddings1_non_aligned].values
    T_non_aligned = embeddings2.loc[embeddings2_non_aligned].values

    # Normalize embeddings
    S_normalized = (S_non_aligned - mean_S) / scale_S
    T_normalized = (T_non_aligned - mean_T) / scale_T

    return S_normalized, T_normalized


def merge_embeddings(S_normalized, T_normalized, R, R_reverse,
                     S_train_aligned, S_test_aligned,
                     scaled_S_train, scaled_S_test,
                     T_train_aligned, T_test_aligned,
                     scaled_T_train, scaled_T_test):
    """
    Merges aligned and non-aligned embeddings for two datasets into combined embeddings.
    Returns:
        tuple: Combined embeddings for both datasets:
            - all_combined_embeddings_1 (np.ndarray)
            - all_combined_embeddings_2 (np.ndarray)
    """
    S_na_tranformed = S_normalized @ R  # Apply R to non-aligned embeddings for dataset 1
    # Apply R_reverse to non-aligned embeddings for dataset 2
    T_na_tranformed = T_normalized @ R_reverse

    S_aligned_full = np.concatenate((S_train_aligned, S_test_aligned), axis=0)
    S_nonaligned_full = np.concatenate((scaled_S_train, scaled_S_test), axis=0)
    T_aligned_full = np.concatenate((T_train_aligned, T_test_aligned), axis=0)
    T_nonaligned_full = np.concatenate((scaled_T_train, scaled_T_test), axis=0)

    averaged_embeddings_full = ((S_aligned_full + T_nonaligned_full) / 2 +
                                (T_aligned_full + S_nonaligned_full) / 2) / 2

    # Option 1
    S_combined_1 = S_na_tranformed
    T_combined_1 = np.concatenate(
        (T_normalized, averaged_embeddings_full), axis=0)

    # Option 2
    S_combined_2 = S_normalized
    T_combined_2 = np.concatenate(
        (T_na_tranformed, averaged_embeddings_full), axis=0)

    all_entitiy_embeddings1 = np.concatenate(
        (S_combined_1, T_combined_1), axis=0)
    all_entitiy_embeddings2 = np.concatenate(
        (S_combined_2, T_combined_2), axis=0)

    return all_entitiy_embeddings1, all_entitiy_embeddings2


def transform_merge_relation_embeddings(
    relation_embeddings1, relation_embeddings2, R, R_reverse
):
    """Transforms and merges relation embeddings using rotation matrices."""
    relations_1_transformed = relation_embeddings1 @ R  # Apply R to Dataset 1 relations
    # Apply R_reverse to Dataset 2 relations
    relations_2_transformed = relation_embeddings2 @ R_reverse

    merged_relations1 = np.concatenate(
        (relations_1_transformed, relation_embeddings2), axis=0)
    merged_relations2 = np.concatenate(
        (relations_2_transformed, relation_embeddings1), axis=0)

    return merged_relations1, merged_relations2


# Model Initialization
def extract_model_parameters(entity_embeddings, relation_embeddings):
    """Extracts dynamic model parameters from the given embeddings."""
    if entity_embeddings.shape[1] != relation_embeddings.shape[1]:
        raise ValueError(
            "Entity and relation embeddings must have the same embedding dimension.")

    num_entities = entity_embeddings.shape[0]
    num_relations = relation_embeddings.shape[0]
    embedding_dim = entity_embeddings.shape[1]

    return {
        "num_entities": num_entities,
        "num_relations": num_relations,
        "embedding_dim": embedding_dim
    }


def initialize_models_and_update_embeddings(entity_embeddings_1, relation_embeddings_1, entity_embeddings_2, relation_embeddings_2):
    """ Initializes two models with dynamically extracted parameters and updates their embeddings."""
    def extract_parameters(entity_embeddings, relation_embeddings):
        num_entities = entity_embeddings.shape[0]
        num_relations = relation_embeddings.shape[0]
        embedding_dim = entity_embeddings.shape[1]
        return {"num_entities": num_entities, "num_relations": num_relations, "embedding_dim": embedding_dim}

    params_1 = extract_parameters(entity_embeddings_1, relation_embeddings_1)
    params_2 = extract_parameters(entity_embeddings_2, relation_embeddings_2)

    model_1, _ = intialize_model(args={
        "model": "Keci",
        "num_entities": params_1["num_entities"],
        "num_relations": params_1["num_relations"],
        "embedding_dim": params_1["embedding_dim"]
    })

    model_2, _ = intialize_model(args={
        "model": "Keci",
        "num_entities": params_2["num_entities"],
        "num_relations": params_2["num_relations"],
        "embedding_dim": params_2["embedding_dim"]
    })

    entity_matrix_1 = torch.tensor(entity_embeddings_1, dtype=torch.float32)
    relation_matrix_1 = torch.tensor(
        relation_embeddings_1, dtype=torch.float32)

    entity_matrix_2 = torch.tensor(entity_embeddings_2, dtype=torch.float32)
    relation_matrix_2 = torch.tensor(
        relation_embeddings_2, dtype=torch.float32)

    # Validate dimensions for model 1
    if entity_matrix_1.shape != (params_1["num_entities"], params_1["embedding_dim"]):
        raise ValueError(
            f"Model 1: Entity embeddings shape mismatch: expected ({params_1['num_entities']}, {params_1['embedding_dim']}), got {entity_matrix_1.shape}")
    if relation_matrix_1.shape != (params_1["num_relations"], params_1["embedding_dim"]):
        raise ValueError(
            f"Model 1: Relation embeddings shape mismatch: expected ({params_1['num_relations']}, {params_1['embedding_dim']}), got {relation_matrix_1.shape}")

    # Validate dimensions for model 2
    if entity_matrix_2.shape != (params_2["num_entities"], params_2["embedding_dim"]):
        raise ValueError(
            f"Model 2: Entity embeddings shape mismatch: expected ({params_2['num_entities']}, {params_2['embedding_dim']}), got {entity_matrix_2.shape}")
    if relation_matrix_2.shape != (params_2["num_relations"], params_2["embedding_dim"]):
        raise ValueError(
            f"Model 2: Relation embeddings shape mismatch: expected ({params_2['num_relations']}, {params_2['embedding_dim']}), got {relation_matrix_2.shape}")

    # Update embeddings for model 1
    with torch.no_grad():
        model_1.entity_embeddings.data = entity_matrix_1
        model_1.relation_embeddings.data = relation_matrix_1

    # Update embeddings for model 2
    with torch.no_grad():
        model_2.entity_embeddings.data = entity_matrix_2
        model_2.relation_embeddings.data = relation_matrix_2

    return model_1, model_2


def procrustes_alignment_pipeline(
    alignment_dict, entity_embeddings1, entity_embeddings2,
    relation_embeddings1, relation_embeddings2,
    output_folder, alignment_type
):
    """
    Handles Procrustes alignment for a given alignment dictionary.

    Args:
        alignment_dict (dict): Alignment dictionary.
        entity_embeddings1 (pd.DataFrame): Entity embeddings for the first dataset.
        entity_embeddings2 (pd.DataFrame): Entity embeddings for the second dataset.
        relation_embeddings1 (pd.DataFrame): Relation embeddings for the first dataset.
        relation_embeddings2 (pd.DataFrame): Relation embeddings for the second dataset.
        output_folder (str): Directory to save aligned models and embeddings.
        alignment_type (str): Type of alignment, e.g., "pre_aligned" or "limes".

    Returns:
        None
    """
    print(f"Starting Procrustes alignment for {alignment_type}...")

    # Train-test split
    S_train, T_train, S_test, T_test = create_train_test_matrices(
        alignment_dict, entity_embeddings1, entity_embeddings2, test_size=0.1
    )

    if S_train is None or T_train is None:
        print(
            f"Skipping {alignment_type} alignment due to empty train-test matrices.")
        return

    # Apply Procrustes alignment (forward and reverse directions)
    print(f"Applying forward Procrustes alignment for {alignment_type}...")
    scaled_S_train, S_train_aligned, scaled_T_train, S_test_aligned, scaled_S_test, scaled_T_test, R = apply_procrustes_both_directions(
        S_train, T_train, S_test, T_test
    )

    print(f"Applying reverse Procrustes alignment for {alignment_type}...")
    scaled_T_train, T_train_aligned, scaled_S_train, T_test_aligned, scaled_T_test, scaled_S_test, R_reverse = apply_procrustes_both_directions(
        S_train, T_train, S_test, T_test, reverse=True
    )
    # Handle non-aligned embeddings
    print(f"Extracting non-aligned embeddings for {alignment_type}...")
    S_normalized, T_normalized = extract_non_aligned_embeddings(
        alignment_dict=alignment_dict,
        embeddings1=entity_embeddings1,
        embeddings2=entity_embeddings2,
        S_train=scaled_S_train,
        T_train=scaled_T_train,
        S_test=scaled_S_test,
        T_test=scaled_T_test
    )

    Sr_normalized, Tr_normalized = extract_non_aligned_embeddings(
        alignment_dict=None,  # No alignment dictionary for relations
        embeddings1=relation_embeddings1,
        embeddings2=relation_embeddings2,
        S_train=scaled_S_train,
        T_train=scaled_T_train,
        S_test=scaled_S_test,
        T_test=scaled_T_test
    )

    # Merge embeddings
    print(
        f"Merging aligned and non-aligned embeddings for {alignment_type}...")
    all_entitiy_embeddings1, all_entitiy_embeddings2 = merge_embeddings(
        S_normalized, T_normalized, R, R_reverse, S_train_aligned, S_test_aligned,
        scaled_S_train, scaled_S_test, T_train_aligned, T_test_aligned,
        scaled_T_train, scaled_T_test
    )

    all_relation_embeddings1, all_relation_embeddings2 = transform_merge_relation_embeddings(
        Sr_normalized, Tr_normalized, R, R_reverse
    )

    # Initialize and save models for alignment
    print(f"Initializing and saving models for {alignment_type}...")
    model_1, model_2 = initialize_models_and_update_embeddings(
        all_entitiy_embeddings1, all_relation_embeddings1,
        all_entitiy_embeddings2, all_relation_embeddings2
    )

    torch.save(model_1.state_dict(), os.path.join(
        output_folder, f"{alignment_type}_model_1.pt"))
    torch.save(model_2.state_dict(), os.path.join(
        output_folder, f"{alignment_type}_model_2.pt"))

    print(f"{alignment_type} alignment completed successfully.")


def main():
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Process datasets for embedding extraction."
    )
    parser.add_argument("--embedding_folder", required=True,
                        help="Path to the embedding folder containing dataset subfolders.")
    parser.add_argument("--alignment_dict_path", required=True,
                        help="Path to pre-aligned alignment dictionary file.")
    parser.add_argument("--alignmentlimes_dict_path", required=True,
                        help="Path to LIMES alignment dictionary file.")
    parser.add_argument("--output_folder", required=True,
                        help="Folder to save processed outputs.")
    args = parser.parse_args()

    # Find dataset paths dynamically
    dataset1_path, dataset2_path = find_dataset_folders(args.embedding_folder)
    print(f"Dataset 1 Path: {dataset1_path}")
    print(f"Dataset 2 Path: {dataset2_path}")

    # Define output folder
    output_folder = os.path.join(
        args.output_folder, f"{os.path.basename(dataset1_path)}_{os.path.basename(dataset2_path)}"
    )
    os.makedirs(output_folder, exist_ok=True)

    # Load embeddings and related files
    dataset1_model, dataset1_entity_to_id, dataset1_relation_to_id = extract_files_from_directory(
        dataset1_path)
    dataset2_model, dataset2_entity_to_id, dataset2_relation_to_id = extract_files_from_directory(
        dataset2_path)

    print("Loading embeddings for both datasets...")
    entity_embeddings1, relation_embeddings1 = load_embeddings(
        dataset1_model, dataset1_entity_to_id, dataset1_relation_to_id)
    entity_embeddings2, relation_embeddings2 = load_embeddings(
        dataset2_model, dataset2_entity_to_id, dataset2_relation_to_id)

    entity_embeddings1 = remove_brackets_from_indices(entity_embeddings1)
    entity_embeddings2 = remove_brackets_from_indices(entity_embeddings2)

    # Handle pre-aligned alignment dictionary
    pre_aligned_dict = build_alignment_dict(
        args.alignment_dict_path)
    if pre_aligned_dict:
        cleaned_pre_aligned_dict = clean_alignment_dict(pre_aligned_dict)
        print(
            f"Processing pre-aligned alignment dictionary with {len(cleaned_pre_aligned_dict)} entries...")
        procrustes_alignment_pipeline(
            cleaned_pre_aligned_dict, entity_embeddings1, entity_embeddings2,
            relation_embeddings1, relation_embeddings2, output_folder, "pre_aligned"
        )
    else:
        print("No valid pre-aligned alignment dictionary found. Skipping...")

    # Handle LIMES alignment dictionary
    limes_dict = build_alignment_dict(args.alignmentlimes_dict_path)
    if limes_dict:
        cleaned_limes_dict = clean_alignment_dict(limes_dict)
        print(
            f"Processing LIMES alignment dictionary with {len(cleaned_limes_dict)} entries...")
        procrustes_alignment_pipeline(
            cleaned_limes_dict, entity_embeddings1, entity_embeddings2,
            relation_embeddings1, relation_embeddings2, output_folder, "limes"
        )
    else:
        print("No valid LIMES alignment dictionary found. Skipping...")

    print("Processing completed successfully.")


if __name__ == "__main__":
    main()
