import os
import json
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

from ..modules.data.graph import KGGraph, get_pair_indices_separate
from ..modules.models.gat import RelationalGATEncoder
from ..modules.models.fusion import AdaptiveFusion
from ..modules.models.projector import DualProjector
from ..modules.models.loss import (
    infonce_loss,
    triple_coherence_loss,
    sample_triples_batch,
)


# ─────────────────────────────────────────────
# EVALUATION FUNCTION
# ─────────────────────────────────────────────

def evaluate_alignment(
    A1: torch.Tensor,
    A2: torch.Tensor,
    src_ids,
    tgt_ids,
    top_k: List[int] = [1, 5, 10, 50],
) -> Dict:
    """
    Evaluate entity alignment.
    A1: KG1 aligned embeddings [N1, dim]
    A2: KG2 aligned embeddings [N2, dim]
    src_ids: KG1-local indices of test pairs
    tgt_ids: KG2-local indices of test pairs
    """
    from .eval.eval import test

    A_src = A1[src_ids].cpu().numpy()
    A_tgt = A2[tgt_ids].cpu().numpy()

    _, hits, mr, mrr = test(
        embeds1=A_src,
        embeds2=A_tgt,
        mapping=None,
        top_k=top_k,
        threads_num=4,
        metric='cosine',
        normalize=True,
        accurate=True,
    )

    results = {}
    for i, k in enumerate(top_k):
        results[f"Hits@{k}"] = hits[i]
    results["MRR"] = mrr
    results["MR"] = mr
    return results


# ─────────────────────────────────────────────
# PSEUDO LABEL EXPANSION
# ─────────────────────────────────────────────

def expand_pseudo_labels(
    A1: torch.Tensor,
    A2: torch.Tensor,
    G1: KGGraph,
    G2: KGGraph,
    current_src: List[str],
    current_tgt: List[str],
    emb1: pd.DataFrame,
    emb2: pd.DataFrame,
    threshold: float = 0.92,
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

def find_owl_same_as(relations=[]):
    for r in relations:
        if "same" in r.lower():
            return r
    return None


def build_rel_emb_tensor(
    G: KGGraph,
    rel_emb: pd.DataFrame,
    dim: int,
    device: torch.device,
) -> torch.Tensor:
    """Build relation embedding tensor indexed by G.relation2id."""
    R = torch.zeros(G.n_relations, dim, device=device, dtype=torch.float32)
    owl_same_as_id = find_owl_same_as(relations=G.relation2id.keys())
    for r_uri, r_id in G.relation2id.items():
        if r_uri in rel_emb.index:
            R[r_id] = torch.tensor(
                rel_emb.loc[r_uri].values, dtype=torch.float32
            ).to(device)
    return R, owl_same_as_id

# ──────────────────────────────────────────────────────────────────────────────
# COMPLEX LINK PREDICTION HEAD
# ──────────────────────────────────────────────────────────────────────────────


class ComplExHead(nn.Module):
    """
    ComplEx scoring head.
    Re-uses entity embeddings A_k[i] ∈ ℝ²⁵⁶ by interpreting them
    as 128 complex dimensions: e = e_re[:128] + i·e_im[128:].
    Only relation embeddings are learned here (re + im, 128 dims each).
    """

    def __init__(self, n_relations: int, entity_dim: int = 256):
        super().__init__()
        assert entity_dim % 2 == 0, "entity_dim must be even (re/im split)"
        self.k = entity_dim // 2  # 128

        # Embeddings of relation complexes (re and im separated)
        self.rel_re = nn.Embedding(n_relations, self.k)
        self.rel_im = nn.Embedding(n_relations, self.k)

        nn.init.xavier_uniform_(self.rel_re.weight)
        nn.init.xavier_uniform_(self.rel_im.weight)

    def score(
        self,
        e_h: torch.Tensor,   # (B, 256)
        r_idx: torch.Tensor,  # (B,)
        e_t: torch.Tensor,   # (B, 256)
    ) -> torch.Tensor:       # (B,)
        """
        ComplEx score :
        Re(<e_h, r, ē_t>) = Re(e_h) ⊙ Re(r) ⊙ Re(e_t)
                           + Re(e_h) ⊙ Im(r) ⊙ Im(e_t)
                           + Im(e_h) ⊙ Re(r) ⊙ Im(e_t)
                           - Im(e_h) ⊙ Im(r) ⊙ Re(e_t)
        """
        h_re, h_im = e_h[:, :self.k], e_h[:, self.k:]
        t_re, t_im = e_t[:, :self.k], e_t[:, self.k:]
        r_re = self.rel_re(r_idx)
        r_im = self.rel_im(r_idx)

        score = (
            (h_re * r_re * t_re).sum(-1)
            + (h_re * r_im * t_im).sum(-1)
            + (h_im * r_re * t_im).sum(-1)
            - (h_im * r_im * t_re).sum(-1)
        )
        return score  # (B,)

    def forward(
        self,
        A: torch.Tensor,          # (N, 256) all KG embeddings
        triples: torch.Tensor,    # (B, 3)  columns [h, r, t] — local indices
        n_negatives: int = 64,
        adv_temperature: float = 1.0,
    ) -> torch.Tensor:
        """
        Self-adversarial ComplEx loss (RotatE style).
        NNegatives generated by tail corruption.
        """
        h_idx = triples[:, 0]
        r_idx = triples[:, 1]
        t_idx = triples[:, 2]
        B = triples.size(0)
        N = A.size(0)

        e_h = A[h_idx]  # (B, 256)
        e_t = A[t_idx]  # (B, 256)

        pos_score = self.score(e_h, r_idx, e_t)  # (B,)

        # Tail corruption: n_negatives random indices per triplet
        neg_idx = torch.randint(0, N, (B, n_negatives),
                                device=A.device)  # (B, K)
        e_neg = A[neg_idx.view(-1)].view(B, n_negatives, -
                                         1)              # (B, K, 256)

        # Negative scores: expand h and r
        e_h_exp = e_h.unsqueeze(
            1).expand(-1, n_negatives, -1).reshape(B * n_negatives, -1)
        r_idx_exp = r_idx.unsqueeze(
            1).expand(-1, n_negatives).reshape(B * n_negatives)
        e_neg_flat = e_neg.reshape(B * n_negatives, -1)

        neg_scores = self.score(e_h_exp, r_idx_exp, e_neg_flat).view(
            B, n_negatives)  # (B, K)

        # Adversarial weights for negatives
        adv_weights = torch.softmax(
            adv_temperature * neg_scores.detach(), dim=-1)  # (B, K)

        # Loss
        pos_loss = -F.logsigmoid(pos_score).mean()
        neg_loss = -(adv_weights * F.logsigmoid(-neg_scores)).sum(-1).mean()

        return pos_loss + neg_loss


# ──────────────────────────────────────────────────────────────────────────────
# BATCH SPLITTER
# ──────────────────────────────────────────────────────────────────────────────

def split_batch(
    # (B, 3) : columns [h, r, t] — true triplets of the KG
    triples: torch.Tensor,
    owl_same_as_id: int,
):
    if not isinstance(triples, torch.Tensor):
        triples = torch.tensor(triples, dtype=torch.long)

    B = triples.size(0)
    mask = (triples[:, 1] == owl_same_as_id)
    if owl_same_as_id == None:
        return None, triples, 0.0, 1.0
    n_align = int(mask.sum().item())
    n_lp = B - n_align

    align_triples = triples[mask] if n_align > 0 else None
    lp_triples = triples[~mask] if n_lp > 0 else None

    lambda_align = B / (2 * n_align + 1e-8)
    lambda_lp = B / (2 * n_lp + 1e-8)

    return align_triples, lp_triples, lambda_align, lambda_lp


# ──────────────────────────────────────────────────────────────────────────────
# MAIN TRAINING FUNCTION
# ──────────────────────────────────────────────────────────────────────────────

def train_sage_tasks(
    data: Dict,
    G1: KGGraph,
    G2: KGGraph,
    E1: np.ndarray,
    E2: np.ndarray,
    P1: np.ndarray,
    P2: np.ndarray,
    output_dir: str,
    # Model hyperparameters
    hidden_dim: int = 256,
    n_heads: int = 4,
    dropout: float = 0.1,
    # Training hyperparameters
    epochs: int = 10,
    batch_size: int = 512,
    lr: float = 1e-3,
    # Loss hyperparameters
    temperature: float = 0.07,
    lambda_tc: float = 0.1,
    lambda_div: float = 0.01,
    lambda_lp: float = 0.5,          # global weights for the ComplEx loss
    lp_n_negatives: int = 64,        # NNegatives per triplet for ComplEx
    lp_adv_temperature: float = 1.0,  # self-adversarial temperature
    # Pseudo-label expansion
    expand_every: int = 5,
    expand_threshold: float = 0.92,
    expand_min_drop: float = 0.03,
    # Evaluation
    eval_every: int = 1,
    # Device
    device_str: str = "auto",
) -> Dict:
    """
    Train SAGE with separate GAT per KG.
    Deterministic task selector:
      - owl:sameAs  → InfoNCE + triple coherence (sameAs only)
      - others      → ComplEx self-adversarial
    """
    os.makedirs(output_dir, exist_ok=True)

    if device_str == "auto":
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    else:
        device = torch.device(device_str)

    print(f"\n{'='*55}")
    print(f"  SAGE TRAINING Tasks (separate GAT per KG)")
    print(f"{'='*55}")
    print(f"  Device          : {device}")
    print(f"  Epochs          : {epochs}")
    print(f"  Batch size      : {batch_size}")
    print(f"  LR              : {lr}")
    print(f"  Temperature     : {temperature}")
    print(f"  Lambda TC       : {lambda_tc}")
    print(f"  Lambda Div      : {lambda_div}")
    print(f"  Lambda LP       : {lambda_lp}")
    print(f"  LP negatives    : {lp_n_negatives}")
    print(f"  LP adv temp     : {lp_adv_temperature}")
    print(f"  Hidden dim      : {hidden_dim}")
    print(f"  N heads         : {n_heads}")
    print(f"{'='*55}\n")

    E1_t = torch.tensor(E1, dtype=torch.float32).to(device)
    E2_t = torch.tensor(E2, dtype=torch.float32).to(device)
    P1_t = torch.tensor(P1, dtype=torch.float32).to(device)
    P2_t = torch.tensor(P2, dtype=torch.float32).to(device)

    rel_emb1_t, owl_same_as_id1 = build_rel_emb_tensor(
        G1, data["rel_emb1"], data["dim"], device)
    rel_emb2_t, owl_same_as_id2 = build_rel_emb_tensor(
        G2, data["rel_emb2"], data["dim"], device)

    gat1 = RelationalGATEncoder(
        dice_dim=data["dim"],
        labse_dim=768,
        hidden_dim=hidden_dim,
        n_relations=G1.n_relations,
        n_heads=n_heads,
        dropout=dropout,
        align_rel_id=None,
    ).to(device)

    gat2 = RelationalGATEncoder(
        dice_dim=data["dim"],
        labse_dim=768,
        hidden_dim=hidden_dim,
        n_relations=G2.n_relations,
        n_heads=n_heads,
        dropout=dropout,
        align_rel_id=None,
    ).to(device)

    fusion = AdaptiveFusion(
        dice_dim=data["dim"],
        labse_dim=768,
        hidden_dim=hidden_dim,
        dropout=dropout,
    ).to(device)

    projector = DualProjector(
        hidden_dim=hidden_dim,
        dropout=dropout,
    ).to(device)

    # ComplEx heads — one per KG (different relations)
    complex_head1 = ComplExHead(
        n_relations=G1.n_relations,
        entity_dim=hidden_dim,
    ).to(device)

    complex_head2 = ComplExHead(
        n_relations=G2.n_relations,
        entity_dim=hidden_dim,
    ).to(device)

    optimizer = torch.optim.Adam(
        list(gat1.parameters()) +
        list(gat2.parameters()) +
        list(fusion.parameters()) +
        list(projector.parameters()) +
        list(complex_head1.parameters()) +
        list(complex_head2.parameters()),
        lr=lr,
        weight_decay=1e-5,
    )

    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
        optimizer, T_max=epochs, eta_min=lr * 0.1
    )

    train_src_np, train_tgt_np = get_pair_indices_separate(
        data["train_pairs"], G1, G2)
    val_src_np,   val_tgt_np = get_pair_indices_separate(
        data["val_pairs"],   G1, G2)
    test_src_np,  test_tgt_np = get_pair_indices_separate(
        data["test_pairs"],  G1, G2)

    train_src = torch.tensor(train_src_np, dtype=torch.long)
    train_tgt = torch.tensor(train_tgt_np, dtype=torch.long)
    val_src = torch.tensor(val_src_np,   dtype=torch.long)
    val_tgt = torch.tensor(val_tgt_np,   dtype=torch.long)
    test_src = torch.tensor(test_src_np,  dtype=torch.long)
    test_tgt = torch.tensor(test_tgt_np,  dtype=torch.long)

    current_train_pairs = list(data["train_pairs"])

    best_val_mrr = -1.0
    best_epoch = 0
    val_history = []
    expand_threshold_curr = expand_threshold
    patience = 2
    epochs_no_improve = 0

    total_params = (
        sum(p.numel() for p in gat1.parameters()) +
        sum(p.numel() for p in gat2.parameters()) +
        sum(p.numel() for p in fusion.parameters()) +
        sum(p.numel() for p in projector.parameters()) +
        sum(p.numel() for p in complex_head1.parameters()) +
        sum(p.numel() for p in complex_head2.parameters())
    )
    print(f"[train] Total parameters : {total_params:,}")
    print(f"[train] Train pairs      : {len(train_src)}")
    print(f"[train] Val pairs        : {len(val_src)}")
    print(f"[train] Test pairs       : {len(test_src)}")
    print()

    for epoch in range(epochs):

        gat1.train()
        gat2.train()
        fusion.train()
        projector.train()
        complex_head1.train()
        complex_head2.train()

        perm = torch.randperm(len(train_src))
        train_src_shuf = train_src[perm]
        train_tgt_shuf = train_tgt[perm]

        epoch_losses = defaultdict(float)
        n_batches = 0

        for batch_start in range(0, len(train_src_shuf), batch_size):
            batch_end = min(batch_start + batch_size, len(train_src_shuf))
            src_batch = train_src_shuf[batch_start:batch_end]
            tgt_batch = train_tgt_shuf[batch_start:batch_end]

            # ── Forward : embeddings for both KGs ──────────────────────
            H1 = gat1(E1_t, P1_t, G1.adj_lists, device)
            H2 = gat2(E2_t, P2_t, G2.adj_lists, device)
            Z1, gw1 = fusion(E1_t, P1_t, H1)
            Z2, gw2 = fusion(E2_t, P2_t, H2)
            A1 = projector.forward_kg1(Z1)
            A2 = projector.forward_kg2(Z2)

            # ── Alignment (InfoNCE) ────────────────────────────────────────
            A_src = A1[src_batch]
            A_tgt = A2[tgt_batch]
            align_loss, pos_sim, neg_sim = infonce_loss(
                A_src, A_tgt, temperature)

            # ── Triple coherence — ONLY on owl:sameAs triples ─────────────
            # We sample triples from the KG, then keep only sameAs.
            tc_loss = torch.tensor(0.0, device=device)
            if lambda_tc > 0:
                triples1_raw = sample_triples_batch(
                    triples=data["triples1"],
                    entity2id=G1.entity2id,
                    relation2id=G1.relation2id,
                    batch_size=512,
                    device=device,
                )
                triples2_raw = sample_triples_batch(
                    triples=data["triples2"],
                    entity2id=G2.entity2id,
                    relation2id=G2.relation2id,
                    batch_size=512,
                    device=device,
                )
                # Restriction to sameAs triples only
                if triples1_raw is not None:
                    mask1 = triples1_raw[:, 1] == owl_same_as_id1
                    if mask1:
                        tc_loss = tc_loss + triple_coherence_loss(
                            A1, rel_emb1_t, triples1_raw[mask1], data["model_type"]
                        )
                if triples2_raw is not None:
                    mask2 = triples2_raw[:, 1] == owl_same_as_id2
                    if mask2:
                        tc_loss = tc_loss + triple_coherence_loss(
                            A2, rel_emb2_t, triples2_raw[mask2], data["model_type"]
                        )
                tc_loss = tc_loss / 2

            # ── Link prediction (ComplEx) — triplets NON-sameAs ─────────────
            # We use the same raw sampled triples below,
            # by taking that time the domain triples (relation != sameAs).
            lp_loss = torch.tensor(0.0, device=device)
            if lambda_lp > 0:
                # KG1
                if triples1_raw is not None:
                    _, lp_triples1, _, lp_w1 = split_batch(
                        triples1_raw, owl_same_as_id1)
                    if lp_triples1 is not None:
                        lp_loss = lp_loss + lp_w1 * complex_head1(
                            A1, lp_triples1,
                            n_negatives=lp_n_negatives,
                            adv_temperature=lp_adv_temperature,
                        )
                # KG2
                if triples2_raw is not None:
                    _, lp_triples2, _, lp_w2 = split_batch(
                        triples2_raw, owl_same_as_id2)
                    if lp_triples2 is not None:
                        lp_loss = lp_loss + lp_w2 * complex_head2(
                            A2, lp_triples2,
                            n_negatives=lp_n_negatives,
                            adv_temperature=lp_adv_temperature,
                        )
                lp_loss = lp_loss / 2

            # ── Diversity on the gates ──────────────────────────────────────
            div_loss = torch.tensor(0.0, device=device)
            if lambda_div > 0:
                gw1_b = gw1[src_batch]
                gw2_b = gw2[tgt_batch]
                ent1 = -(gw1_b * torch.log(gw1_b.clamp(1e-8))).sum(-1).mean()
                ent2 = -(gw2_b * torch.log(gw2_b.clamp(1e-8))).sum(-1).mean()
                div_loss = -(ent1 + ent2) / 2

            # ── Total loss ──────────────────────────────────────────────────
            total_loss = (
                align_loss
                + lambda_tc * tc_loss
                + lambda_lp * lp_loss
                + lambda_div * div_loss
            )

            optimizer.zero_grad()
            total_loss.backward()
            torch.nn.utils.clip_grad_norm_(
                list(gat1.parameters()) + list(gat2.parameters()) +
                list(fusion.parameters()) + list(projector.parameters()) +
                list(complex_head1.parameters()) +
                list(complex_head2.parameters()),
                max_norm=1.0,
            )
            optimizer.step()

            epoch_losses["total_loss"] += total_loss.item()
            epoch_losses["align_loss"] += align_loss.item()
            epoch_losses["tc_loss"] += tc_loss.item()
            epoch_losses["lp_loss"] += lp_loss.item()
            epoch_losses["div_loss"] += div_loss.item()
            epoch_losses["pos_sim"] += pos_sim
            epoch_losses["neg_sim"] += neg_sim
            n_batches += 1

        avg_losses = {k: v / n_batches for k, v in epoch_losses.items()}
        scheduler.step()

        if epoch % eval_every == 0:
            gat1.eval()
            gat2.eval()
            fusion.eval()
            projector.eval()
            complex_head1.eval()
            complex_head2.eval()

            with torch.no_grad():
                H1_v = gat1(E1_t, P1_t, G1.adj_lists, device)
                H2_v = gat2(E2_t, P2_t, G2.adj_lists, device)
                Z1_v, gw1_v = fusion(E1_t, P1_t, H1_v)
                Z2_v, gw2_v = fusion(E2_t, P2_t, H2_v)
                A1_v = projector.forward_kg1(Z1_v)
                A2_v = projector.forward_kg2(Z2_v)

            val_results = evaluate_alignment(A1_v, A2_v, val_src, val_tgt)
            val_mrr = val_results["MRR"]
            val_hits1 = val_results["Hits@1"]

            avg_gw1 = gw1_v.mean(dim=0).cpu().numpy()
            avg_gw2 = gw2_v.mean(dim=0).cpu().numpy()

            print(
                f"[Epoch {epoch:03d}] "
                f"loss={avg_losses['total_loss']:.4f} "
                f"align={avg_losses['align_loss']:.4f} "
                f"tc={avg_losses['tc_loss']:.4f} "
                f"lp={avg_losses['lp_loss']:.4f} "
                f"| val H@1={val_hits1:.4f} MRR={val_mrr:.4f} "
                f"| gate1=[{avg_gw1[0]:.2f},{avg_gw1[1]:.2f},{avg_gw1[2]:.2f}] "
                f"gate2=[{avg_gw2[0]:.2f},{avg_gw2[1]:.2f},{avg_gw2[2]:.2f}]"
            )

            val_history.append({
                "epoch": epoch, "val_mrr": val_mrr,
                "val_hits1": val_hits1, **avg_losses,
            })

            if val_mrr > best_val_mrr:
                best_val_mrr = val_mrr
                best_epoch = epoch
                epochs_no_improve = 0
                torch.save({
                    "epoch": epoch,
                    "val_mrr": val_mrr,
                    "gat1": gat1.state_dict(),
                    "gat2": gat2.state_dict(),
                    "fusion": fusion.state_dict(),
                    "projector": projector.state_dict(),
                    "complex_head1": complex_head1.state_dict(),
                    "complex_head2": complex_head2.state_dict(),
                }, os.path.join(output_dir, "best_model.pt"))
                print(f"  ✓ New best model saved (val MRR={val_mrr:.4f})")
            else:
                epochs_no_improve += 1
                print(f"  No improvement ({epochs_no_improve}/{patience})")
                if epochs_no_improve >= patience:
                    print(f"\n[train] Early stopping at epoch {epoch}")
                    break

        if epoch > 0 and epoch % expand_every == 0:
            gat1.eval()
            gat2.eval()
            fusion.eval()
            projector.eval()

            with torch.no_grad():
                H1_e = gat1(E1_t, P1_t, G1.adj_lists, device)
                H2_e = gat2(E2_t, P2_t, G2.adj_lists, device)
                Z1_e, _ = fusion(E1_t, P1_t, H1_e)
                Z2_e, _ = fusion(E2_t, P2_t, H2_e)
                A1_e = projector.forward_kg1(Z1_e)
                A2_e = projector.forward_kg2(Z2_e)

            new_pairs, n_new = expand_pseudo_labels(
                A1=A1_e, A2=A2_e, G1=G1, G2=G2,
                current_src=[p[0] for p in current_train_pairs],
                current_tgt=[p[1] for p in current_train_pairs],
                emb1=data["emb1"], emb2=data["emb2"],
                threshold=expand_threshold_curr,
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

    # ── Loading the best model ────────────────────────────────────────
    print(f"\n[train] Loading best model from epoch {best_epoch}")
    checkpoint = torch.load(
        os.path.join(output_dir, "best_model.pt"), map_location=device
    )
    gat1.load_state_dict(checkpoint["gat1"])
    gat2.load_state_dict(checkpoint["gat2"])
    fusion.load_state_dict(checkpoint["fusion"])
    projector.load_state_dict(checkpoint["projector"])
    complex_head1.load_state_dict(checkpoint["complex_head1"])
    complex_head2.load_state_dict(checkpoint["complex_head2"])

    gat1.eval()
    gat2.eval()
    fusion.eval()
    projector.eval()
    complex_head1.eval()
    complex_head2.eval()

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
        "test_results": test_results,
        "best_epoch": best_epoch,
        "best_val_mrr": best_val_mrr,
        "val_history": val_history,
        "hyperparams": {
            "hidden_dim": hidden_dim,
            "n_heads": n_heads,
            "epochs": epochs,
            "batch_size": batch_size,
            "lr": lr,
            "temperature": temperature,
            "lambda_tc": lambda_tc,
            "lambda_div": lambda_div,
            "lambda_lp": lambda_lp,
            "lp_n_negatives": lp_n_negatives,
            "lp_adv_temperature": lp_adv_temperature,
        },
        "mode": "separate_gat_per_kg_complex_lp",
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

    return results, gat1, gat2, fusion, projector, complex_head1, complex_head2, A1_final, A2_final
