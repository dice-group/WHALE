import os
import sys
import json
import random
import pandas as pd
import numpy as np
import torch

SEED = 42
random.seed(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)
if torch.cuda.is_available():
    torch.cuda.manual_seed_all(SEED)
torch.backends.cudnn.deterministic = True
torch.backends.cudnn.benchmark     = False

print(f"Random seed fixed to {SEED}")

SAGE_PARENT = "/scratch/hpc-prf-whale/duygu"
sys.path.insert(0, SAGE_PARENT)

from sage.modules.data.loader import load_all
from sage_scale.modules.data.graph import (
    build_kg_graph,
    build_embedding_matrix_single,
)
from sage_scale.modules.data.labse import LaBSEEncoder, build_dbpedia_names, build_wikidata_names
from sage_scale.modules.train import train_sage
from sage.modules.eval.link_prediction import (
    run_link_prediction_pipeline,
)

# ─────────────────────────────────────────────
# ALL PATHS
# ─────────────────────────────────────────────

FOLDER_KG1  = "/scratch/hpc-prf-whale/duygu/alignment/embeddings/EN_DE_TransE_15K/AEN_TransE_15K"
FOLDER_KG2  = "/scratch/hpc-prf-whale/duygu/alignment/embeddings/EN_DE_TransE_15K/DE_TransE_15K"
OUTPUT_DIR  = "/scratch/hpc-prf-whale/duygu/sage_scale/output/EN_DE_TransE_15K"
LABSE_CACHE_KG1 = "/scratch/hpc-prf-whale/duygu/sage_scale/cache/labse_EN_15K.npy"
LABSE_CACHE_KG2 = "/scratch/hpc-prf-whale/duygu/sage_scale/cache/labse_DE_15K.npy"

TRAIN_LINKS = "/scratch/hpc-prf-whale/duygu/alignment/pre_aligned_fold0/train_links"
VAL_LINKS   = "/scratch/hpc-prf-whale/duygu/alignment/pre_aligned_valid_fold0/valid_links"
TEST_LINKS  = "/scratch/hpc-prf-whale/duygu/alignment/pre_aligned_test_fold0/test_links"

DATA_DIR      = "/scratch/hpc-prf-whale/duygu/alignment/data/OpenEA_dataset_v1.1/EN_DE_15K_V1"
TRAIN_TRIPLES = os.path.join(DATA_DIR, "train_merged.txt")
TEST_TRIPLES  = os.path.join(DATA_DIR, "test_merged.txt")
REL_TRIPLES_1 = os.path.join(DATA_DIR, "rel_triples_1")
REL_TRIPLES_2 = os.path.join(DATA_DIR, "rel_triples_2")
ATTR_TRIPLES_1  = os.path.join(DATA_DIR, "attr_triples_1")  
ATTR_TRIPLES_2  = os.path.join(DATA_DIR, "attr_triples_2")  

def load_triples_tsv(path: str):
    df = pd.read_csv(
        path, sep="\t", header=None,
        names=["subject", "relation", "object"],
        dtype=str, engine="python",
    ).dropna()
    return df.values.tolist()


# ─────────────────────────────────────────────
# STEP 1: LOAD DATA
# ─────────────────────────────────────────────

print("\n" + "="*55)
print("  STEP 1: LOADING DATA")
print("="*55)

data = load_all(
    folder_kg1  = FOLDER_KG1,
    folder_kg2  = FOLDER_KG2,
    train_links = TRAIN_LINKS,
    val_links   = VAL_LINKS,
    test_links  = TEST_LINKS,
)

# ─────────────────────────────────────────────
# STEP 2: BUILD SEPARATE KG GRAPHS
# ─────────────────────────────────────────────

print("\n" + "="*55)
print("  STEP 2: BUILDING SEPARATE KG GRAPHS")
print("  (No cross-KG edges — truly post-hoc)")
print("="*55)

G1 = build_kg_graph(data["triples1"], data["emb1"], name="KG1 (DBpedia)")
G2 = build_kg_graph(data["triples2"], data["emb2"], name="KG2 (Wikidata)")

E1 = build_embedding_matrix_single(G1, data["emb1"])
E2 = build_embedding_matrix_single(G2, data["emb2"])

# ─────────────────────────────────────────────
# STEP 3: LABSE EMBEDDINGS (separate per KG)
# ─────────────────────────────────────────────

print("\n" + "="*55)
print("  STEP 3: LABSE EMBEDDINGS")
print("="*55)

encoder = LaBSEEncoder(model_name="LaBSE", device="auto")

names1_dbpedia = build_dbpedia_names(ATTR_TRIPLES_1)
P1 = encoder.encode_kg(
    G          = G1,
    names      = names1_dbpedia,
    batch_size = 512,
    cache_path = LABSE_CACHE_KG1,
)

names2_wiki = build_wikidata_names(ATTR_TRIPLES_2)
P2 = encoder.encode_kg(
    G          = G2,
    names      = names2_wiki,
    batch_size = 512,
    cache_path = LABSE_CACHE_KG2,
)

# ─────────────────────────────────────────────
# STEP 4: TRAIN SAGE (ENTITY ALIGNMENT)
# ─────────────────────────────────────────────

print("\n" + "="*55)
print("  STEP 4: TRAINING SAGE")
print("="*55)

results, gat1, gat2, fusion, projector, A1, A2 = train_sage(
    data             = data,
    G1               = G1,
    G2               = G2,
    E1               = E1,
    E2               = E2,
    P1               = P1,
    P2               = P2,
    output_dir       = OUTPUT_DIR,
    hidden_dim       = 256,
    n_heads          = 4,
    dropout          = 0.1,
    epochs           = 20,
    batch_size       = 512,
    lr               = 1e-3,
    temperature      = 0.07,
    lambda_tc        = 2,
    lambda_div       = 0.01,
    expand_every     = 10,
    expand_threshold = 0.95,
    expand_min_drop  = 0.01,
    eval_every       = 1,
    device_str       = "auto",
)

print(f"\n[EA Results]")
print(f"  Hits@1  : {results['test_results']['Hits@1']:.4f}")
print(f"  Hits@10 : {results['test_results']['Hits@10']:.4f}")
print(f"  MRR     : {results['test_results']['MRR']:.4f}")

# ─────────────────────────────────────────────
# STEP 5: PREPARE TRIPLES FOR LINK PREDICTION
# ─────────────────────────────────────────────

print("\n" + "="*55)
print("  STEP 5: PREPARING TRIPLES")
print("="*55)

all_train = load_triples_tsv(TRAIN_TRIPLES)
print(f"Total train triples: {len(all_train)}")

random.seed(42)
random.shuffle(all_train)
n_val            = max(1000, int(len(all_train) * 0.1))
val_triples_lp   = all_train[:n_val]
train_triples_lp = all_train[n_val:]

print(f"Fine-tune train : {len(train_triples_lp)}")
print(f"Fine-tune val   : {len(val_triples_lp)}")

# ─────────────────────────────────────────────
# STEP 6: LINK PREDICTION
# ─────────────────────────────────────────────

print("\n" + "="*55)
print("  STEP 6: LINK PREDICTION")
print("="*55)

lp_results = run_link_prediction_pipeline(
    aligned_kg1_csv       = os.path.join(OUTPUT_DIR, "aligned_kg1.csv"),
    aligned_kg2_csv       = os.path.join(OUTPUT_DIR, "aligned_kg2.csv"),
    folder_kg1            = FOLDER_KG1,
    folder_kg2            = FOLDER_KG2,
    train_triples         = train_triples_lp,
    val_triples           = val_triples_lp,
    test_triples_path     = TEST_TRIPLES,
    output_dir            = OUTPUT_DIR,
    fine_tune_epochs      = 2,
    fine_tune_lr          = 0.001,
    rel_triples_1_path    = REL_TRIPLES_1,
    rel_triples_2_path    = REL_TRIPLES_2,
)

# ─────────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────────

print("\n" + "="*55)
print("  FINAL RESULTS SUMMARY")
print("="*55)
print(f"\n  Entity Alignment:")
print(f"    Hits@1  : {results['test_results']['Hits@1']:.4f}")
print(f"    Hits@10 : {results['test_results']['Hits@10']:.4f}")
print(f"    MRR     : {results['test_results']['MRR']:.4f}")
print(f"\n  Link Prediction:")
print(f"    H@1     : {lp_results['H@1']:.4f}")
print(f"    H@3     : {lp_results['H@3']:.4f}")
print(f"    H@10    : {lp_results['H@10']:.4f}")
print(f"    MRR     : {lp_results['MRR']:.4f}")
print("="*55)

combined = {
    "entity_alignment" : results["test_results"],
    "link_prediction"  : lp_results,
    "best_epoch"       : results["best_epoch"],
    "best_val_mrr_ea"  : results["best_val_mrr"],
    "mode"             : "separate_gat_per_kg",
}
results_path = os.path.join(OUTPUT_DIR, "final_results.json")
with open(results_path, "w") as f:
    json.dump(combined, f, indent=2)

print(f"\nAll results saved to {results_path}")
