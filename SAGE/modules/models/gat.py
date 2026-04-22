
import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
from typing import Dict, Optional
from sage.modules.data.graph import MergedGraph


# ─────────────────────────────────────────────
# SPARSE SOFTMAX HELPER
# ─────────────────────────────────────────────

def _sparse_softmax(
    scores      : torch.Tensor,
    dst_indices : torch.Tensor,
    n_nodes     : int,
) -> torch.Tensor:
    """
    Compute softmax over neighbor attention scores
    for each destination node separately.
    Uses out-of-place operations for gradient compatibility.
    """
    n_heads = scores.size(1)
    n_edges = scores.size(0)

    max_scores = torch.full(
        (n_nodes, n_heads),
        float('-inf'),
        device=scores.device,
        dtype=scores.dtype
    )

    dst_exp = dst_indices.unsqueeze(1).expand(-1, n_heads)

    max_scores = max_scores.scatter_reduce(
        0, dst_exp, scores,
        reduce="amax",
        include_self=True
    )

    scores_shifted = scores - max_scores[dst_indices]
    exp_scores = torch.exp(scores_shifted)

    sum_exp = torch.zeros(
        n_nodes, n_heads,
        device=scores.device,
        dtype=scores.dtype
    )
    sum_exp = sum_exp.scatter_add(0, dst_exp, exp_scores)
    sum_exp = sum_exp.clamp(min=1e-8)

    weights = exp_scores / sum_exp[dst_indices]

    return weights


# ─────────────────────────────────────────────
# RELATIONAL GAT LAYER
# ─────────────────────────────────────────────

class RelationalGATLayer(nn.Module):
    """
    One layer of a Relational Graph Attention Network.

    For each entity i:
        1. For each relation type r and neighbor j via r:
           compute attention score a(i, j, r)
        2. Aggregate neighbor messages weighted by attention
        3. Combine with self transformation

    The ALIGN relation gets its own learned gate
    so the model controls how much cross-KG signal to use.

    Args:
        in_dim       : input embedding dimension
        out_dim      : output embedding dimension
        n_relations  : number of relation types
        n_heads      : number of attention heads
        dropout      : dropout rate
        align_rel_id : ID of the special ALIGN relation
    """

    def __init__(
        self,
        in_dim       : int,
        out_dim      : int,
        n_relations  : int,
        n_heads      : int = 4,
        dropout      : float = 0.1,
        align_rel_id : Optional[int] = None,
    ):
        super().__init__()

        assert out_dim % n_heads == 0, \
            f"out_dim ({out_dim}) must be divisible " \
            f"by n_heads ({n_heads})"

        self.in_dim       = in_dim
        self.out_dim      = out_dim
        self.n_relations  = n_relations
        self.n_heads      = n_heads
        self.head_dim     = out_dim // n_heads
        self.align_rel_id = align_rel_id

        self.rel_embed_dim = 32
        self.rel_embeddings = nn.Embedding(
            n_relations, self.rel_embed_dim
        )

        self.W_msg = nn.Linear(
            in_dim + self.rel_embed_dim,
            out_dim,
            bias=False
        )
    
        self.W_self = nn.Linear(in_dim, out_dim, bias=False)

        self.attn_vec = nn.Parameter(
            torch.Tensor(1, n_heads, self.head_dim)
        )
        nn.init.xavier_uniform_(
            self.attn_vec.view(1, n_heads, self.head_dim)
        )

        self.align_gate = nn.Parameter(torch.tensor(0.5))

        self.layer_norm = nn.LayerNorm(out_dim)
        self.drop       = nn.Dropout(dropout)

    def forward(
        self,
        x         : torch.Tensor,
        adj_lists : Dict[int, np.ndarray],
        device    : torch.device,
    ) -> torch.Tensor:
        """
        Forward pass.

        Args:
            x         : entity embeddings [n_entities, in_dim]
            adj_lists : {relation_id: array of (dst, src) pairs}
            device    : torch device

        Returns:
            updated embeddings [n_entities, out_dim]
        """
        n_entities = x.size(0)

        out = self.W_self(x)  # [n_entities, out_dim]

        all_messages = []
        all_dsts = []

        for r_id, edges in adj_lists.items():
            if len(edges) == 0:
                continue

            edges_t = torch.tensor(
                edges, dtype=torch.long, device=x.device
            )
            dst = edges_t[:, 0]
            src = edges_t[:, 1]

            n_edges = dst.size(0)
            src_emb = x[src]

            r_id_t = torch.tensor(r_id, device=x.device)
            r_emb  = self.rel_embeddings(r_id_t)
            r_emb_expanded = r_emb.unsqueeze(0).expand(n_edges, -1)

            msg_input = torch.cat([src_emb, r_emb_expanded], dim=1)
            msg = self.W_msg(msg_input)

            msg_heads = msg.view(n_edges, self.n_heads, self.head_dim)

            attn_score = (msg_heads * self.attn_vec).sum(dim=-1)
            attn_score = F.leaky_relu(attn_score, negative_slope=0.2)

            attn_weight = _sparse_softmax(attn_score, dst, n_entities)
            attn_weight = self.drop(attn_weight)

            if r_id == self.align_rel_id:
                gate = torch.sigmoid(self.align_gate)
                attn_weight = attn_weight * gate

            weighted = (msg_heads * attn_weight.unsqueeze(-1))
            weighted = weighted.view(n_edges, self.out_dim)

            all_messages.append((dst, weighted))

        if all_messages:
            agg = torch.zeros(
                n_entities, self.out_dim,
                device=x.device, dtype=x.dtype
            )
            for dst, weighted in all_messages:
                agg = agg.scatter_add(
                    0,
                    dst.unsqueeze(1).expand(-1, self.out_dim),
                    weighted
                )
            out = out + agg

        out = self.layer_norm(out)
        out = F.elu(out)

        return out

# ─────────────────────────────────────────────
# RELATIONAL GAT ENCODER
# ─────────────────────────────────────────────

class RelationalGATEncoder(nn.Module):
    """
    Full two-layer Relational GAT encoder.

    Takes DICE embeddings and LaBSE embeddings
    concatenated together and produces structurally
    enriched entity representations.

    Architecture:
        [DICE(256) || LaBSE(768)] → input_proj → 256
        → RelationalGATLayer 1 → 256
        + residual
        → RelationalGATLayer 2 → 256
        + residual
        → LayerNorm → output 256

    Args:
        dice_dim     : DICE embedding dimension (256)
        labse_dim    : LaBSE embedding dimension (768)
        hidden_dim   : hidden and output dimension (256)
        n_relations  : number of relation types
        n_heads      : attention heads per layer
        dropout      : dropout rate
        align_rel_id : ID of the ALIGN relation
    """

    def __init__(
        self,
        dice_dim     : int,
        labse_dim    : int,
        hidden_dim   : int,
        n_relations  : int,
        n_heads      : int = 4,
        dropout      : float = 0.1,
        align_rel_id : Optional[int] = None,
    ):
        super().__init__()

        self.dice_dim    = dice_dim
        self.labse_dim   = labse_dim
        self.hidden_dim  = hidden_dim

        input_dim = dice_dim + labse_dim  # 256 + 768 = 1024

        self.input_proj = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ELU(),
        )

        self.gat1 = RelationalGATLayer(
            in_dim       = hidden_dim,
            out_dim      = hidden_dim,
            n_relations  = n_relations,
            n_heads      = n_heads,
            dropout      = dropout,
            align_rel_id = align_rel_id,
        )

        self.gat2 = RelationalGATLayer(
            in_dim       = hidden_dim,
            out_dim      = hidden_dim,
            n_relations  = n_relations,
            n_heads      = n_heads,
            dropout      = dropout,
            align_rel_id = align_rel_id,
        )

        self.out_norm = nn.LayerNorm(hidden_dim)

    def forward(
        self,
        E         : torch.Tensor,
        P         : torch.Tensor,
        adj_lists : Dict[int, np.ndarray],
        device    : torch.device,
    ) -> torch.Tensor:
        """
        Forward pass.

        Args:
            E         : DICE embeddings [n_entities, dice_dim]
            P         : LaBSE embeddings [n_entities, labse_dim]
            adj_lists : {relation_id: array of (dst, src) pairs}
            device    : torch device

        Returns:
            Enriched embeddings [n_entities, hidden_dim]
        """
        x = torch.cat([E, P], dim=1)
        x = self.input_proj(x)
        
        h1 = self.gat1(x, adj_lists, device)
        h1 = h1 + x
        
        h2 = self.gat2(h1, adj_lists, device)
        h2 = h2 + h1
        
        out = self.out_norm(h2)

        return out