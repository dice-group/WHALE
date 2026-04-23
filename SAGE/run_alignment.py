# run_alignment.py
import os, sys
SAGE_PARENT = "/scratch/hpc-prf-whale/duygu"
sys.path.insert(0, SAGE_PARENT)

import numpy as np
import torch
from sage.modules.data.loader import load_all
from sage.modules.data.graph import build_merged_graph, build_embedding_matrix
from sage.modules.data.labse import LaBSEEncoder
from sage.modules.train import train_sage

FOLDER_KG1  = "/scratch/hpc-prf-whale/duygu/alignment/embeddings/EN_DE_TransE_15K/AEN_TransE_15K"
FOLDER_KG2  = "/scratch/hpc-prf-whale/duygu/alignment/embeddings/EN_DE_TransE_15K/DE_TransE_15K"
OUTPUT_DIR  = "/scratch/hpc-prf-whale/duygu/sage/output/EN_DE_TransE_15K"
LABSE_CACHE = "/scratch/hpc-prf-whale/duygu/sage/cache/labse_EN_DE_15K.npy"
TRAIN_LINKS = "/scratch/hpc-prf-whale/duygu/alignment/pre_aligned_fold0/train_links"
VAL_LINKS   = "/scratch/hpc-prf-whale/duygu/alignment/pre_aligned_valid_fold0/valid_links"
TEST_LINKS  = "/scratch/hpc-prf-whale/duygu/alignment/pre_aligned_test_fold0/test_links"

# Load data
data = load_all(
    folder_kg1  = FOLDER_KG1,
    folder_kg2  = FOLDER_KG2,
    train_links = TRAIN_LINKS,
    val_links   = VAL_LINKS,
    test_links  = TEST_LINKS,
)

G = build_merged_graph(
    triples1    = data["triples1"],
    triples2    = data["triples2"],
    train_pairs = data["train_pairs"],
    emb1        = data["emb1"],
    emb2        = data["emb2"],
)
E = build_embedding_matrix(G, data["emb1"], data["emb2"])

encoder = LaBSEEncoder(model_name="LaBSE", device="auto")
P = encoder.encode_graph(
    G          = G,
    names1     = data["names1"],
    names2     = data["names2"],
    batch_size = 512,
    cache_path = LABSE_CACHE,
)

results, gat, fusion, projector, A = train_sage(
    data             = data,
    G                = G,
    E                = E,
    P                = P,
    output_dir       = OUTPUT_DIR,
    hidden_dim       = 256,
    n_heads          = 4,
    dropout          = 0.1,
    epochs           = 100,
    batch_size       = 512,
    lr               = 1e-3,
    temperature      = 0.07,
    lambda_tc        = 2.0,
    lambda_div       = 0.01,
    expand_every     = 10,
    expand_threshold = 0.95,
    expand_min_drop  = 0.01,
    eval_every       = 1,
    device_str       = "auto",
)

print(f"\nAlignment done. Results saved to {OUTPUT_DIR}")
print(f"EA Hits@1 : {results['test_results']['Hits@1']:.4f}")
print(f"EA MRR    : {results['test_results']['MRR']:.4f}")
print(f"\naligned_kg1.csv and aligned_kg2.csv saved.")
print(f"Now run run_link_prediction.py to evaluate LP.")