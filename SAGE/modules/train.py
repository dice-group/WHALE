import os
import json
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

from sage_scale.modules.data.graph import KGGraph, get_pair_indices_separate
from sage.modules.models.gat import RelationalGATEncoder
from sage.modules.models.fusion import AdaptiveFusion
from sage.modules.models.projector import DualProjector
from sage.modules.models.loss import (
    infonce_loss,
    triple_coherence_loss,
    sample_triples_batch,
)


# ─────────────────────────────────────────────
# EVALUATION FUNCTION
# ─────────────────────────────────────────────

def evaluate_alignment(
    A1     : torch.Tensor,
    A2     : torch.Tensor,
    src_ids,
    tgt_ids,
    top_k  : List[int] = [1, 5, 10, 50],
) -> Dict:
    """
    Evaluate entity alignment.
    A1: KG1 aligned embeddings [N1, dim]
    A2: KG2 aligned embeddings [N2, dim]
    src_ids: KG1-local indices of test pairs
    tgt_ids: KG2-local indices of test pairs
    """
    from sage.modules.eval.eval import test

    A_src = A1[src_ids].cpu().numpy()
    A_tgt = A2[tgt_ids].cpu().numpy()

    _, hits, mr, mrr = test(
        embeds1    = A_src,
        embeds2    = A_tgt,
        mapping    = None,
        top_k      = top_k,
        threads_num= 4,
        metric     = 'cosine',
        normalize  = True,
        accurate   = True,
    )

    results = {}
    for i, k in enumerate(top_k):
        results[f"Hits@{k}"] = hits[i]
    results["MRR"] = mrr
    results["MR"]  = mr
    return results


# ─────────────────────────────────────────────
# PSEUDO LABEL EXPANSION
# ─────────────────────────────────────────────

def expand_pseudo_labels(
    A1          : torch.Tensor,
    A2          : torch.Tensor,
    G1          : KGGraph,
    G2          : KGGraph,
    current_src : List[str],
    current_tgt : List[str],
    emb1        : pd.DataFrame,
    emb2        : pd.DataFrame,
    threshold   : float = 0.92,
) -> Tuple[List[Tuple[str, str]], int]:
    """
    Find new confident pairs via mutual nearest neighbor.
    A1/A2 are already the full KG1/KG2 aligned embeddings.
    """
    with torch.no_grad():
        sim = torch.matmul(A1, A2.t())  # [N1, N2]

    nn_1to2 = sim.argmax(dim=1).cpu().numpy()
    nn_2to1 = sim.argmax(dim=0).cpu().numpy()

    current_pairs_set = set(zip(current_src, current_tgt))
    new_pairs = []

    for i in range(A1.shape[0]):
        j = int(nn_1to2[i])
        if int(nn_2to1[j]) != i:
            continue
        if sim[i, j].item() < threshold:
            continue
        uri1 = G1.id2entity[i]
        uri2 = G2.id2entity[j]
        if (uri1, uri2) in current_pairs_set:
            continue
        if uri1 not in emb1.index or uri2 not in emb2.index:
            continue
        new_pairs.append((uri1, uri2))

    return new_pairs, len(new_pairs)


# ─────────────────────────────────────────────
# BUILD PER-KG RELATION EMBEDDING TENSOR
# ─────────────────────────────────────────────

def build_rel_emb_tensor(
    G       : KGGraph,
    rel_emb : pd.DataFrame,
    dim     : int,
    device  : torch.device,
) -> torch.Tensor:
    """Build relation embedding tensor indexed by G.relation2id."""
    R = torch.zeros(G.n_relations, dim, device=device, dtype=torch.float32)
    for r_uri, r_id in G.relation2id.items():
        if r_uri in rel_emb.index:
            R[r_id] = torch.tensor(
                rel_emb.loc[r_uri].values, dtype=torch.float32
            ).to(device)
    return R


# ─────────────────────────────────────────────
# MAIN TRAINING FUNCTION
# ─────────────────────────────────────────────

def train_sage(
    data            : Dict,
    G1              : KGGraph,
    G2              : KGGraph,
    E1              : np.ndarray,
    E2              : np.ndarray,
    P1              : np.ndarray,
    P2              : np.ndarray,
    output_dir      : str,
    # Model hyperparameters
    hidden_dim      : int   = 256,
    n_heads         : int   = 4,
    dropout         : float = 0.1,
    # Training hyperparameters
    epochs          : int   = 10,
    batch_size      : int   = 512,
    lr              : float = 1e-3,
    # Loss hyperparameters
    temperature     : float = 0.07,
    lambda_tc       : float = 0.1,
    lambda_div      : float = 0.01,
    # Pseudo-label expansion
    expand_every    : int   = 5,
    expand_threshold: float = 0.92,
    expand_min_drop : float = 0.03,
    # Evaluation
    eval_every      : int   = 1,
    # Device
    device_str      : str   = "auto",
) -> Dict:
    """
    Train SAGE with GAT running SEPARATELY per KG.
    No cross-KG graph edges — alignment happens only via loss.

    Args:
        data   : output of load_all()
        G1, G2 : separate KGGraph objects (no ALIGN edges)
        E1, E2 : DICE embedding matrices [N1/N2, dim]
        P1, P2 : LaBSE embedding matrices [N1/N2, 768]
    """
    os.makedirs(output_dir, exist_ok=True)

    if device_str == "auto":
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    else:
        device = torch.device(device_str)

    print(f"\n{'='*55}")
    print(f"  SAGE TRAINING (separate GAT per KG)")
    print(f"{'='*55}")
    print(f"  Device       : {device}")
    print(f"  Epochs       : {epochs}")
    print(f"  Batch size   : {batch_size}")
    print(f"  LR           : {lr}")
    print(f"  Temperature  : {temperature}")
    print(f"  Lambda TC    : {lambda_tc}")
    print(f"  Lambda Div   : {lambda_div}")
    print(f"  Hidden dim   : {hidden_dim}")
    print(f"  N heads      : {n_heads}")
    print(f"{'='*55}\n")

    E1_t = torch.tensor(E1, dtype=torch.float32).to(device)
    E2_t = torch.tensor(E2, dtype=torch.float32).to(device)
    P1_t = torch.tensor(P1, dtype=torch.float32).to(device)
    P2_t = torch.tensor(P2, dtype=torch.float32).to(device)

    rel_emb1_t = build_rel_emb_tensor(G1, data["rel_emb1"], data["dim"], device)
    rel_emb2_t = build_rel_emb_tensor(G2, data["rel_emb2"], data["dim"], device)

    gat1 = RelationalGATEncoder(
        dice_dim     = data["dim"],
        labse_dim    = 768,
        hidden_dim   = hidden_dim,
        n_relations  = G1.n_relations,
        n_heads      = n_heads,
        dropout      = dropout,
        align_rel_id = None,
    ).to(device)

    gat2 = RelationalGATEncoder(
        dice_dim     = data["dim"],
        labse_dim    = 768,
        hidden_dim   = hidden_dim,
        n_relations  = G2.n_relations,
        n_heads      = n_heads,
        dropout      = dropout,
        align_rel_id = None,
    ).to(device)

    fusion = AdaptiveFusion(
        dice_dim   = data["dim"],
        labse_dim  = 768,
        hidden_dim = hidden_dim,
        dropout    = dropout,
    ).to(device)

    projector = DualProjector(
        hidden_dim = hidden_dim,
        dropout    = dropout,
    ).to(device)

    optimizer = torch.optim.Adam(
        list(gat1.parameters()) +
        list(gat2.parameters()) +
        list(fusion.parameters()) +
        list(projector.parameters()),
        lr           = lr,
        weight_decay = 1e-5,
    )

    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
        optimizer, T_max=epochs, eta_min=lr * 0.1
    )

    train_src_np, train_tgt_np = get_pair_indices_separate(data["train_pairs"], G1, G2)
    val_src_np,   val_tgt_np   = get_pair_indices_separate(data["val_pairs"],   G1, G2)
    test_src_np,  test_tgt_np  = get_pair_indices_separate(data["test_pairs"],  G1, G2)

    train_src = torch.tensor(train_src_np, dtype=torch.long)
    train_tgt = torch.tensor(train_tgt_np, dtype=torch.long)
    val_src   = torch.tensor(val_src_np,   dtype=torch.long)
    val_tgt   = torch.tensor(val_tgt_np,   dtype=torch.long)
    test_src  = torch.tensor(test_src_np,  dtype=torch.long)
    test_tgt  = torch.tensor(test_tgt_np,  dtype=torch.long)

    current_train_pairs = list(data["train_pairs"])

    best_val_mrr          = -1.0
    best_epoch            = 0
    val_history           = []
    expand_threshold_curr = expand_threshold
    patience              = 2
    epochs_no_improve     = 0

    total_params = (
        sum(p.numel() for p in gat1.parameters()) +
        sum(p.numel() for p in gat2.parameters()) +
        sum(p.numel() for p in fusion.parameters()) +
        sum(p.numel() for p in projector.parameters())
    )
    print(f"[train] Total parameters: {total_params:,}")
    print(f"[train] Train pairs : {len(train_src)}")
    print(f"[train] Val pairs   : {len(val_src)}")
    print(f"[train] Test pairs  : {len(test_src)}")
    print()

    for epoch in range(epochs):

        gat1.train(); gat2.train()
        fusion.train(); projector.train()

        perm           = torch.randperm(len(train_src))
        train_src_shuf = train_src[perm]
        train_tgt_shuf = train_tgt[perm]

        epoch_losses = defaultdict(float)
        n_batches    = 0

        for batch_start in range(0, len(train_src_shuf), batch_size):
            batch_end = min(batch_start + batch_size, len(train_src_shuf))
            src_batch = train_src_shuf[batch_start:batch_end]
            tgt_batch = train_tgt_shuf[batch_start:batch_end]

            H1 = gat1(E1_t, P1_t, G1.adj_lists, device) 
            H2 = gat2(E2_t, P2_t, G2.adj_lists, device) 

            Z1, gw1 = fusion(E1_t, P1_t, H1)
            Z2, gw2 = fusion(E2_t, P2_t, H2)

            A1 = projector.forward_kg1(Z1) 
            A2 = projector.forward_kg2(Z2)  

            A_src = A1[src_batch] 
            A_tgt = A2[tgt_batch]  

    
            align_loss, pos_sim, neg_sim = infonce_loss(A_src, A_tgt, temperature)

    
            tc_loss = torch.tensor(0.0, device=device)
            if lambda_tc > 0:
                triples1_idx = sample_triples_batch(
                    triples     = data["triples1"],
                    entity2id   = G1.entity2id,
                    relation2id = G1.relation2id,
                    batch_size  = 256,
                    device      = device,
                )
                triples2_idx = sample_triples_batch(
                    triples     = data["triples2"],
                    entity2id   = G2.entity2id,
                    relation2id = G2.relation2id,
                    batch_size  = 256,
                    device      = device,
                )
                if triples1_idx is not None:
                    tc_loss = tc_loss + triple_coherence_loss(
                        A1, rel_emb1_t, triples1_idx, data["model_type"]
                    )
                if triples2_idx is not None:
                    tc_loss = tc_loss + triple_coherence_loss(
                        A2, rel_emb2_t, triples2_idx, data["model_type"]
                    )
                tc_loss = tc_loss / 2


            div_loss = torch.tensor(0.0, device=device)
            if lambda_div > 0:
                gw1_b = gw1[src_batch]
                gw2_b = gw2[tgt_batch]
                ent1  = -(gw1_b * torch.log(gw1_b.clamp(1e-8))).sum(-1).mean()
                ent2  = -(gw2_b * torch.log(gw2_b.clamp(1e-8))).sum(-1).mean()
                div_loss = -(ent1 + ent2) / 2

            total_loss = align_loss + lambda_tc * tc_loss + lambda_div * div_loss

            optimizer.zero_grad()
            total_loss.backward()
            torch.nn.utils.clip_grad_norm_(
                list(gat1.parameters()) + list(gat2.parameters()) +
                list(fusion.parameters()) + list(projector.parameters()),
                max_norm=1.0,
            )
            optimizer.step()

            epoch_losses["total_loss"] += total_loss.item()
            epoch_losses["align_loss"] += align_loss.item()
            epoch_losses["tc_loss"]    += tc_loss.item()
            epoch_losses["div_loss"]   += div_loss.item()
            epoch_losses["pos_sim"]    += pos_sim
            epoch_losses["neg_sim"]    += neg_sim
            n_batches += 1

        avg_losses = {k: v / n_batches for k, v in epoch_losses.items()}
        scheduler.step()

        if epoch % eval_every == 0:
            gat1.eval(); gat2.eval()
            fusion.eval(); projector.eval()

            with torch.no_grad():
                H1_v = gat1(E1_t, P1_t, G1.adj_lists, device)
                H2_v = gat2(E2_t, P2_t, G2.adj_lists, device)
                Z1_v, gw1_v = fusion(E1_t, P1_t, H1_v)
                Z2_v, gw2_v = fusion(E2_t, P2_t, H2_v)
                A1_v = projector.forward_kg1(Z1_v)
                A2_v = projector.forward_kg2(Z2_v)

            val_results = evaluate_alignment(A1_v, A2_v, val_src, val_tgt)
            val_mrr   = val_results["MRR"]
            val_hits1 = val_results["Hits@1"]

            avg_gw1 = gw1_v.mean(dim=0).cpu().numpy()
            avg_gw2 = gw2_v.mean(dim=0).cpu().numpy()

            print(
                f"[Epoch {epoch:03d}] "
                f"loss={avg_losses['total_loss']:.4f} "
                f"align={avg_losses['align_loss']:.4f} "
                f"tc={avg_losses['tc_loss']:.4f} "
                f"| val H@1={val_hits1:.4f} MRR={val_mrr:.4f} "
                f"| gate1=[{avg_gw1[0]:.2f},{avg_gw1[1]:.2f},{avg_gw1[2]:.2f}] "
                f"gate2=[{avg_gw2[0]:.2f},{avg_gw2[1]:.2f},{avg_gw2[2]:.2f}]"
            )

            val_history.append({
                "epoch": epoch, "val_mrr": val_mrr,
                "val_hits1": val_hits1, **avg_losses,
            })

            if val_mrr > best_val_mrr:
                best_val_mrr      = val_mrr
                best_epoch        = epoch
                epochs_no_improve = 0
                torch.save({
                    "epoch"     : epoch,
                    "val_mrr"   : val_mrr,
                    "gat1"      : gat1.state_dict(),
                    "gat2"      : gat2.state_dict(),
                    "fusion"    : fusion.state_dict(),
                    "projector" : projector.state_dict(),
                }, os.path.join(output_dir, "best_model.pt"))
                print(f"  ✓ New best model saved (val MRR={val_mrr:.4f})")
            else:
                epochs_no_improve += 1
                print(f"  No improvement ({epochs_no_improve}/{patience})")
                if epochs_no_improve >= patience:
                    print(f"\n[train] Early stopping at epoch {epoch}")
                    break

        if epoch > 0 and epoch % expand_every == 0:
            gat1.eval(); gat2.eval()
            fusion.eval(); projector.eval()

            with torch.no_grad():
                H1_e = gat1(E1_t, P1_t, G1.adj_lists, device)
                H2_e = gat2(E2_t, P2_t, G2.adj_lists, device)
                Z1_e, _ = fusion(E1_t, P1_t, H1_e)
                Z2_e, _ = fusion(E2_t, P2_t, H2_e)
                A1_e = projector.forward_kg1(Z1_e)
                A2_e = projector.forward_kg2(Z2_e)

            new_pairs, n_new = expand_pseudo_labels(
                A1          = A1_e,
                A2          = A2_e,
                G1          = G1,
                G2          = G2,
                current_src = [p[0] for p in current_train_pairs],
                current_tgt = [p[1] for p in current_train_pairs],
                emb1        = data["emb1"],
                emb2        = data["emb2"],
                threshold   = expand_threshold_curr,
            )

            if n_new > 0:
                print(f"[Expand epoch {epoch}] Added {n_new} pseudo pairs "
                      f"(threshold={expand_threshold_curr:.3f})")
                current_train_pairs.extend(new_pairs)

                new_src, new_tgt = [], []
                for u1, u2 in current_train_pairs:
                    i1 = G1.entity2id.get(u1)
                    i2 = G2.entity2id.get(u2)
                    if i1 is not None and i2 is not None:
                        new_src.append(i1)
                        new_tgt.append(i2)

                train_src = torch.tensor(new_src, dtype=torch.long)
                train_tgt = torch.tensor(new_tgt, dtype=torch.long)
                expand_threshold_curr = max(
                    0.80, expand_threshold_curr - expand_min_drop
                )
            else:
                print(f"[Expand epoch {epoch}] No new pairs found "
                      f"(threshold={expand_threshold_curr:.3f})")

    print(f"\n[train] Loading best model from epoch {best_epoch}")
    checkpoint = torch.load(
        os.path.join(output_dir, "best_model.pt"), map_location=device
    )
    gat1.load_state_dict(checkpoint["gat1"])
    gat2.load_state_dict(checkpoint["gat2"])
    fusion.load_state_dict(checkpoint["fusion"])
    projector.load_state_dict(checkpoint["projector"])

    gat1.eval(); gat2.eval()
    fusion.eval(); projector.eval()

    with torch.no_grad():
        H1_f = gat1(E1_t, P1_t, G1.adj_lists, device)
        H2_f = gat2(E2_t, P2_t, G2.adj_lists, device)
        Z1_f, _ = fusion(E1_t, P1_t, H1_f)
        Z2_f, _ = fusion(E2_t, P2_t, H2_f)
        A1_final = projector.forward_kg1(Z1_f)
        A2_final = projector.forward_kg2(Z2_f)

    test_results = evaluate_alignment(A1_final, A2_final, test_src, test_tgt)

    print(f"\n{'='*55}")
    print(f"  FINAL TEST RESULTS (separate GAT)")
    print(f"{'='*55}")
    print(f"  Hits@1  : {test_results['Hits@1']:.4f}")
    print(f"  Hits@5  : {test_results['Hits@5']:.4f}")
    print(f"  Hits@10 : {test_results['Hits@10']:.4f}")
    print(f"  Hits@50 : {test_results['Hits@50']:.4f}")
    print(f"  MRR     : {test_results['MRR']:.4f}")
    print(f"  MR      : {test_results['MR']:.2f}")
    print(f"  Best epoch: {best_epoch}")
    print(f"{'='*55}")

    results = {
        "test_results" : test_results,
        "best_epoch"   : best_epoch,
        "best_val_mrr" : best_val_mrr,
        "val_history"  : val_history,
        "hyperparams"  : {
            "hidden_dim"  : hidden_dim,
            "n_heads"     : n_heads,
            "epochs"      : epochs,
            "batch_size"  : batch_size,
            "lr"          : lr,
            "temperature" : temperature,
            "lambda_tc"   : lambda_tc,
            "lambda_div"  : lambda_div,
        },
        "mode": "separate_gat_per_kg",
    }

    with open(os.path.join(output_dir, "results.json"), "w") as f:
        json.dump(results, f, indent=2)

    kg1_uris = [G1.id2entity[i] for i in range(G1.n_entities)]
    kg2_uris = [G2.id2entity[i] for i in range(G2.n_entities)]

    A_kg1_df = pd.DataFrame(A1_final.cpu().numpy(), index=kg1_uris)
    A_kg2_df = pd.DataFrame(A2_final.cpu().numpy(), index=kg2_uris)

    A_kg1_df.to_csv(os.path.join(output_dir, "aligned_kg1.csv"))
    A_kg2_df.to_csv(os.path.join(output_dir, "aligned_kg2.csv"))

    print(f"[train] Aligned embeddings saved: "
          f"KG1={A_kg1_df.shape}, KG2={A_kg2_df.shape}")

    return results, gat1, gat2, fusion, projector, A1_final, A2_final