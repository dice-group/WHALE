import os, sys, json, random
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from collections import defaultdict

SEED = 42
random.seed(SEED); np.random.seed(SEED); torch.manual_seed(SEED)
if torch.cuda.is_available(): torch.cuda.manual_seed_all(SEED)

SAGE_PARENT = "/scratch/hpc-prf-whale/duygu"
sys.path.insert(0, SAGE_PARENT)

from sage.modules.data.loader import load_all
from sage_scale.modules.data.graph import build_kg_graph, build_embedding_matrix_single
from sage_scale.modules.data.labse import LaBSEEncoder
from sage_scale.modules.data.graph import get_pair_indices_separate
from sage.modules.models.gat import RelationalGATEncoder
from sage.modules.models.fusion import AdaptiveFusion
from sage.modules.models.projector import DualProjector
from sage.modules.models.loss import infonce_loss, triple_coherence_loss, sample_triples_batch
from sage_scale.modules.train import evaluate_alignment, build_rel_emb_tensor

FOLDER_KG1      = "/scratch/hpc-prf-whale/duygu/alignment/embeddings/EN_DE_TransE_15K/AEN_TransE_15K"
FOLDER_KG2      = "/scratch/hpc-prf-whale/duygu/alignment/embeddings/EN_DE_TransE_15K/DE_TransE_15K"
TRAIN_LINKS     = "/scratch/hpc-prf-whale/duygu/alignment/pre_aligned_fold0/train_links"
VAL_LINKS       = "/scratch/hpc-prf-whale/duygu/alignment/pre_aligned_valid_fold0/valid_links"
TEST_LINKS      = "/scratch/hpc-prf-whale/duygu/alignment/pre_aligned_test_fold0/test_links"
LABSE_CACHE_KG1 = "/scratch/hpc-prf-whale/duygu/sage_scale/cache/labse_EN_15K.npy"
LABSE_CACHE_KG2 = "/scratch/hpc-prf-whale/duygu/sage_scale/cache/labse_DE_15K.npy"
OUTPUT_DIR      = "/scratch/hpc-prf-whale/duygu/sage_scale/output/ablation"
os.makedirs(OUTPUT_DIR, exist_ok=True)

EPOCHS     = 5
BATCH_SIZE = 512
LR         = 1e-3
TEMP       = 0.07
LAMBDA_TC  = 2.0
HIDDEN_DIM = 256
N_HEADS    = 4
DROPOUT    = 0.1

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Device: {device}")

print("\n" + "="*55)
print("  LOADING DATA")
print("="*55)

data = load_all(
    folder_kg1  = FOLDER_KG1,
    folder_kg2  = FOLDER_KG2,
    train_links = TRAIN_LINKS,
    val_links   = VAL_LINKS,
    test_links  = TEST_LINKS,
)

G1 = build_kg_graph(data["triples1"], data["emb1"], name="KG1 (EN)")
G2 = build_kg_graph(data["triples2"], data["emb2"], name="KG2 (DE)")
E1 = build_embedding_matrix_single(G1, data["emb1"])
E2 = build_embedding_matrix_single(G2, data["emb2"])

encoder = LaBSEEncoder(model_name="LaBSE", device="auto")
P1 = encoder.encode_kg(G1, data["names1"], batch_size=512, cache_path=LABSE_CACHE_KG1)
P2 = encoder.encode_kg(G2, data["names2"], batch_size=512, cache_path=LABSE_CACHE_KG2)

E1_t = torch.tensor(E1, dtype=torch.float32).to(device)
E2_t = torch.tensor(E2, dtype=torch.float32).to(device)
P1_t = torch.tensor(P1, dtype=torch.float32).to(device)
P2_t = torch.tensor(P2, dtype=torch.float32).to(device)

train_src_np, train_tgt_np = get_pair_indices_separate(data["train_pairs"], G1, G2)
val_src_np,   val_tgt_np   = get_pair_indices_separate(data["val_pairs"],   G1, G2)
test_src_np,  test_tgt_np  = get_pair_indices_separate(data["test_pairs"],  G1, G2)

train_src = torch.tensor(train_src_np, dtype=torch.long)
train_tgt = torch.tensor(train_tgt_np, dtype=torch.long)
val_src   = torch.tensor(val_src_np,   dtype=torch.long)
val_tgt   = torch.tensor(val_tgt_np,   dtype=torch.long)
test_src  = torch.tensor(test_src_np,  dtype=torch.long)
test_tgt  = torch.tensor(test_tgt_np,  dtype=torch.long)

rel_emb1_t = build_rel_emb_tensor(G1, data["rel_emb1"], data["dim"], device)
rel_emb2_t = build_rel_emb_tensor(G2, data["rel_emb2"], data["dim"], device)

all_results = {}

# ════════════════════════════════════════════════════════════
# VARIANT 1: LaBSE only — zero training, pure cosine sim
# ════════════════════════════════════════════════════════════

print("\n" + "="*55)
print("  VARIANT 1: LaBSE ONLY (no training)")
print("="*55)

A1_labse = F.normalize(P1_t, dim=1)
A2_labse = F.normalize(P2_t, dim=1)
v1_labse = evaluate_alignment(A1_labse, A2_labse, test_src, test_tgt)
print(f"  H@1={v1_labse['Hits@1']:.4f}  H@10={v1_labse['Hits@10']:.4f}  MRR={v1_labse['MRR']:.4f}")
all_results["1_labse_only"] = v1_labse


# ════════════════════════════════════════════════════════════
# VARIANT 2: DICE + LaBSE, no GAT — train projector only
# ════════════════════════════════════════════════════════════

print("\n" + "="*55)
print("  VARIANT 2: DICE + LaBSE, no GAT")
print("="*55)

class NaiveFusionProjector(nn.Module):
    def __init__(self, dice_dim, labse_dim, hidden_dim):
        super().__init__()
        self.proj1 = nn.Sequential(
            nn.Linear(dice_dim + labse_dim, hidden_dim),
            nn.LayerNorm(hidden_dim), nn.ELU(),
        )
        self.proj2 = nn.Sequential(
            nn.Linear(dice_dim + labse_dim, hidden_dim),
            nn.LayerNorm(hidden_dim), nn.ELU(),
        )
        self.head1 = nn.Linear(hidden_dim, hidden_dim)
        self.head2 = nn.Linear(hidden_dim, hidden_dim)

    def forward_kg1(self, E, P):
        z = self.proj1(torch.cat([E, P], dim=1))
        return F.normalize(self.head1(z), dim=1)

    def forward_kg2(self, E, P):
        z = self.proj2(torch.cat([E, P], dim=1))
        return F.normalize(self.head2(z), dim=1)

model_v2 = NaiveFusionProjector(data["dim"], 768, HIDDEN_DIM).to(device)
opt_v2   = torch.optim.Adam(model_v2.parameters(), lr=LR)
best_mrr_v2, best_results_v2 = -1, {}

for epoch in range(EPOCHS):
    model_v2.train()
    perm = torch.randperm(len(train_src))
    ts, tt = train_src[perm], train_tgt[perm]
    for bs in range(0, len(ts), BATCH_SIZE):
        sb, tb = ts[bs:bs+BATCH_SIZE], tt[bs:bs+BATCH_SIZE]
        A1 = model_v2.forward_kg1(E1_t, P1_t)
        A2 = model_v2.forward_kg2(E2_t, P2_t)
        loss, _, _ = infonce_loss(A1[sb], A2[tb], TEMP)
        opt_v2.zero_grad(); loss.backward(); opt_v2.step()

    model_v2.eval()
    with torch.no_grad():
        A1v = model_v2.forward_kg1(E1_t, P1_t)
        A2v = model_v2.forward_kg2(E2_t, P2_t)
    vr = evaluate_alignment(A1v, A2v, val_src, val_tgt)
    print(f"  [Epoch {epoch}] val H@1={vr['Hits@1']:.4f} MRR={vr['MRR']:.4f}")
    if vr["MRR"] > best_mrr_v2:
        best_mrr_v2 = vr["MRR"]
        with torch.no_grad():
            A1t = model_v2.forward_kg1(E1_t, P1_t)
            A2t = model_v2.forward_kg2(E2_t, P2_t)
        best_results_v2 = evaluate_alignment(A1t, A2t, test_src, test_tgt)

print(f"  BEST H@1={best_results_v2['Hits@1']:.4f}  H@10={best_results_v2['Hits@10']:.4f}  MRR={best_results_v2['MRR']:.4f}")
all_results["2_dice_labse_no_gat"] = best_results_v2


# ════════════════════════════════════════════════════════════
# VARIANT 3: DICE + LaBSE + GAT, no adaptive fusion (equal weights)
# ════════════════════════════════════════════════════════════

print("\n" + "="*55)
print("  VARIANT 3: + GAT, no adaptive fusion (equal weights)")
print("="*55)

gat1_v3 = RelationalGATEncoder(
    dice_dim=data["dim"], labse_dim=768, hidden_dim=HIDDEN_DIM,
    n_relations=G1.n_relations, n_heads=N_HEADS, dropout=DROPOUT, align_rel_id=None,
).to(device)
gat2_v3 = RelationalGATEncoder(
    dice_dim=data["dim"], labse_dim=768, hidden_dim=HIDDEN_DIM,
    n_relations=G2.n_relations, n_heads=N_HEADS, dropout=DROPOUT, align_rel_id=None,
).to(device)

class EqualFusion(nn.Module):
    def __init__(self, dice_dim, labse_dim, hidden_dim):
        super().__init__()
        self.proj_e = nn.Sequential(nn.Linear(dice_dim,  hidden_dim), nn.LayerNorm(hidden_dim), nn.ELU())
        self.proj_p = nn.Sequential(nn.Linear(labse_dim, hidden_dim), nn.LayerNorm(hidden_dim), nn.ELU())
        self.proj_h = nn.Sequential(nn.LayerNorm(hidden_dim), nn.ELU())
        self.out    = nn.Sequential(nn.Linear(hidden_dim, hidden_dim), nn.LayerNorm(hidden_dim))

    def forward(self, E, P, H):
        z = (self.proj_e(E) + self.proj_p(P) + self.proj_h(H)) / 3.0
        return self.out(z)

efusion_v3 = EqualFusion(data["dim"], 768, HIDDEN_DIM).to(device)
proj_v3    = DualProjector(hidden_dim=HIDDEN_DIM, dropout=DROPOUT).to(device)
opt_v3     = torch.optim.Adam(
    list(gat1_v3.parameters()) + list(gat2_v3.parameters()) +
    list(efusion_v3.parameters()) + list(proj_v3.parameters()), lr=LR
)
best_mrr_v3, best_results_v3 = -1, {}

for epoch in range(EPOCHS):
    gat1_v3.train(); gat2_v3.train(); efusion_v3.train(); proj_v3.train()
    perm = torch.randperm(len(train_src))
    ts, tt = train_src[perm], train_tgt[perm]
    for bs in range(0, len(ts), BATCH_SIZE):
        sb, tb = ts[bs:bs+BATCH_SIZE], tt[bs:bs+BATCH_SIZE]
        H1 = gat1_v3(E1_t, P1_t, G1.adj_lists, device)
        H2 = gat2_v3(E2_t, P2_t, G2.adj_lists, device)
        Z1 = efusion_v3(E1_t, P1_t, H1)
        Z2 = efusion_v3(E2_t, P2_t, H2)
        A1 = proj_v3.forward_kg1(Z1)
        A2 = proj_v3.forward_kg2(Z2)
        loss, _, _ = infonce_loss(A1[sb], A2[tb], TEMP)
        opt_v3.zero_grad(); loss.backward(); opt_v3.step()

    gat1_v3.eval(); gat2_v3.eval(); efusion_v3.eval(); proj_v3.eval()
    with torch.no_grad():
        H1v = gat1_v3(E1_t, P1_t, G1.adj_lists, device)
        H2v = gat2_v3(E2_t, P2_t, G2.adj_lists, device)
        Z1v = efusion_v3(E1_t, P1_t, H1v)
        Z2v = efusion_v3(E2_t, P2_t, H2v)
        A1v = proj_v3.forward_kg1(Z1v)
        A2v = proj_v3.forward_kg2(Z2v)
    vr = evaluate_alignment(A1v, A2v, val_src, val_tgt)
    print(f"  [Epoch {epoch}] val H@1={vr['Hits@1']:.4f} MRR={vr['MRR']:.4f}")
    if vr["MRR"] > best_mrr_v3:
        best_mrr_v3 = vr["MRR"]
        best_results_v3 = evaluate_alignment(A1v, A2v, test_src, test_tgt)

print(f"  BEST H@1={best_results_v3['Hits@1']:.4f}  H@10={best_results_v3['Hits@10']:.4f}  MRR={best_results_v3['MRR']:.4f}")
all_results["3_gat_no_adaptive_fusion"] = best_results_v3


# ════════════════════════════════════════════════════════════
# VARIANT 4: Full SAGE (GAT + adaptive fusion + dual projector)
# ════════════════════════════════════════════════════════════

print("\n" + "="*55)
print("  VARIANT 4: FULL SAGE")
print("="*55)

gat1_v4 = RelationalGATEncoder(
    dice_dim=data["dim"], labse_dim=768, hidden_dim=HIDDEN_DIM,
    n_relations=G1.n_relations, n_heads=N_HEADS, dropout=DROPOUT, align_rel_id=None,
).to(device)
gat2_v4 = RelationalGATEncoder(
    dice_dim=data["dim"], labse_dim=768, hidden_dim=HIDDEN_DIM,
    n_relations=G2.n_relations, n_heads=N_HEADS, dropout=DROPOUT, align_rel_id=None,
).to(device)
fusion_v4 = AdaptiveFusion(
    dice_dim=data["dim"], labse_dim=768, hidden_dim=HIDDEN_DIM, dropout=DROPOUT,
).to(device)
proj_v4 = DualProjector(hidden_dim=HIDDEN_DIM, dropout=DROPOUT).to(device)
opt_v4  = torch.optim.Adam(
    list(gat1_v4.parameters()) + list(gat2_v4.parameters()) +
    list(fusion_v4.parameters()) + list(proj_v4.parameters()), lr=LR,
    weight_decay=1e-5,
)
best_mrr_v4, best_results_v4 = -1, {}

for epoch in range(EPOCHS):
    gat1_v4.train(); gat2_v4.train(); fusion_v4.train(); proj_v4.train()
    perm = torch.randperm(len(train_src))
    ts, tt = train_src[perm], train_tgt[perm]
    for bs in range(0, len(ts), BATCH_SIZE):
        sb, tb = ts[bs:bs+BATCH_SIZE], tt[bs:bs+BATCH_SIZE]
        H1 = gat1_v4(E1_t, P1_t, G1.adj_lists, device)
        H2 = gat2_v4(E2_t, P2_t, G2.adj_lists, device)
        Z1, gw1 = fusion_v4(E1_t, P1_t, H1)
        Z2, gw2 = fusion_v4(E2_t, P2_t, H2)
        A1 = proj_v4.forward_kg1(Z1)
        A2 = proj_v4.forward_kg2(Z2)
        align_loss, _, _ = infonce_loss(A1[sb], A2[tb], TEMP)
        tc1 = sample_triples_batch(data["triples1"], G1.entity2id, G1.relation2id, 256, device)
        tc2 = sample_triples_batch(data["triples2"], G2.entity2id, G2.relation2id, 256, device)
        tc_loss = torch.tensor(0.0, device=device)
        if tc1 is not None: tc_loss = tc_loss + triple_coherence_loss(A1, rel_emb1_t, tc1, data["model_type"])
        if tc2 is not None: tc_loss = tc_loss + triple_coherence_loss(A2, rel_emb2_t, tc2, data["model_type"])
        tc_loss = tc_loss / 2
        loss = align_loss + LAMBDA_TC * tc_loss
        opt_v4.zero_grad(); loss.backward(); opt_v4.step()

    gat1_v4.eval(); gat2_v4.eval(); fusion_v4.eval(); proj_v4.eval()
    with torch.no_grad():
        H1v = gat1_v4(E1_t, P1_t, G1.adj_lists, device)
        H2v = gat2_v4(E2_t, P2_t, G2.adj_lists, device)
        Z1v, _ = fusion_v4(E1_t, P1_t, H1v)
        Z2v, _ = fusion_v4(E2_t, P2_t, H2v)
        A1v = proj_v4.forward_kg1(Z1v)
        A2v = proj_v4.forward_kg2(Z2v)
    vr = evaluate_alignment(A1v, A2v, val_src, val_tgt)
    print(f"  [Epoch {epoch}] val H@1={vr['Hits@1']:.4f} MRR={vr['MRR']:.4f}")
    if vr["MRR"] > best_mrr_v4:
        best_mrr_v4 = vr["MRR"]
        best_results_v4 = evaluate_alignment(A1v, A2v, test_src, test_tgt)

print(f"  BEST H@1={best_results_v4['Hits@1']:.4f}  H@10={best_results_v4['Hits@10']:.4f}  MRR={best_results_v4['MRR']:.4f}")
all_results["4_full_sage"] = best_results_v4


# ════════════════════════════════════════════════════════════
# VARIANT 5: DICE + GAT only — no LaBSE (name signal zeroed out)
# ════════════════════════════════════════════════════════════

print("\n" + "="*55)
print("  VARIANT 5: DICE + GAT only (no LaBSE)")
print("="*55)


P1_zero = torch.zeros_like(P1_t)
P2_zero = torch.zeros_like(P2_t)

gat1_v5 = RelationalGATEncoder(
    dice_dim=data["dim"], labse_dim=768, hidden_dim=HIDDEN_DIM,
    n_relations=G1.n_relations, n_heads=N_HEADS, dropout=DROPOUT, align_rel_id=None,
).to(device)
gat2_v5 = RelationalGATEncoder(
    dice_dim=data["dim"], labse_dim=768, hidden_dim=HIDDEN_DIM,
    n_relations=G2.n_relations, n_heads=N_HEADS, dropout=DROPOUT, align_rel_id=None,
).to(device)

efusion_v5 = EqualFusion(data["dim"], 768, HIDDEN_DIM).to(device)
proj_v5    = DualProjector(hidden_dim=HIDDEN_DIM, dropout=DROPOUT).to(device)
opt_v5     = torch.optim.Adam(
    list(gat1_v5.parameters()) + list(gat2_v5.parameters()) +
    list(efusion_v5.parameters()) + list(proj_v5.parameters()), lr=LR
)
best_mrr_v5, best_results_v5 = -1, {}

for epoch in range(EPOCHS):
    gat1_v5.train(); gat2_v5.train(); efusion_v5.train(); proj_v5.train()
    perm = torch.randperm(len(train_src))
    ts, tt = train_src[perm], train_tgt[perm]
    for bs in range(0, len(ts), BATCH_SIZE):
        sb, tb = ts[bs:bs+BATCH_SIZE], tt[bs:bs+BATCH_SIZE]
        H1 = gat1_v5(E1_t, P1_zero, G1.adj_lists, device)
        H2 = gat2_v5(E2_t, P2_zero, G2.adj_lists, device)
        Z1 = efusion_v5(E1_t, P1_zero, H1)
        Z2 = efusion_v5(E2_t, P2_zero, H2)
        A1 = proj_v5.forward_kg1(Z1)
        A2 = proj_v5.forward_kg2(Z2)
        tc1 = sample_triples_batch(data["triples1"], G1.entity2id, G1.relation2id, 256, device)
        tc2 = sample_triples_batch(data["triples2"], G2.entity2id, G2.relation2id, 256, device)
        tc_loss = torch.tensor(0.0, device=device)
        if tc1 is not None: tc_loss = tc_loss + triple_coherence_loss(A1, rel_emb1_t, tc1, data["model_type"])
        if tc2 is not None: tc_loss = tc_loss + triple_coherence_loss(A2, rel_emb2_t, tc2, data["model_type"])
        tc_loss = tc_loss / 2
        align_loss, _, _ = infonce_loss(A1[sb], A2[tb], TEMP)
        loss = align_loss + LAMBDA_TC * tc_loss
        opt_v5.zero_grad(); loss.backward(); opt_v5.step()

    gat1_v5.eval(); gat2_v5.eval(); efusion_v5.eval(); proj_v5.eval()
    with torch.no_grad():
        H1v = gat1_v5(E1_t, P1_zero, G1.adj_lists, device)
        H2v = gat2_v5(E2_t, P2_zero, G2.adj_lists, device)
        Z1v = efusion_v5(E1_t, P1_zero, H1v)
        Z2v = efusion_v5(E2_t, P2_zero, H2v)
        A1v = proj_v5.forward_kg1(Z1v)
        A2v = proj_v5.forward_kg2(Z2v)
    vr = evaluate_alignment(A1v, A2v, val_src, val_tgt)
    print(f"  [Epoch {epoch}] val H@1={vr['Hits@1']:.4f} MRR={vr['MRR']:.4f}")
    if vr["MRR"] > best_mrr_v5:
        best_mrr_v5 = vr["MRR"]
        best_results_v5 = evaluate_alignment(A1v, A2v, test_src, test_tgt)

print(f"  BEST H@1={best_results_v5['Hits@1']:.4f}  H@10={best_results_v5['Hits@10']:.4f}  MRR={best_results_v5['MRR']:.4f}")
all_results["5_dice_gat_no_labse"] = best_results_v5


# ════════════════════════════════════════════════════════════
# SUMMARY
# ════════════════════════════════════════════════════════════

print("\n" + "="*55)
print("  ABLATION SUMMARY")
print("="*55)
print(f"{'Variant':<35} {'H@1':>6} {'H@5':>6} {'H@10':>6} {'MRR':>6}")
print("-"*55)

labels = {
    "1_labse_only":              "1. LaBSE only (no training)",
    "2_dice_labse_no_gat":       "2. DICE + LaBSE (no GAT)",
    "3_gat_no_adaptive_fusion":  "3. + GAT, equal fusion",
    "4_full_sage":               "4. Full SAGE (adaptive fusion)",
    "5_dice_gat_no_labse":       "5. DICE + GAT (no LaBSE)",
}

for key, label in labels.items():
    r = all_results[key]
    print(f"{label:<35} {r['Hits@1']:>6.4f} {r['Hits@5']:>6.4f} {r['Hits@10']:>6.4f} {r['MRR']:>6.4f}")

print("="*55)

with open(os.path.join(OUTPUT_DIR, "ablation_results.json"), "w") as f:
    json.dump(all_results, f, indent=2)

print(f"\nSaved to {OUTPUT_DIR}/ablation_results.json")
