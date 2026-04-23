# sage/modules/data/labse.py

import numpy as np
import torch
import os
from typing import Dict, List, Optional
from ..data.graph import MergedGraph


def build_dbpedia_names(attr_triples_path: str) -> Dict[str, str]:
    """
    Build a {uri: name} dict for DBpedia entities from attr_triples.

    Uses foaf:name as the primary source — the curated human label
    (e.g. "North Sea Hijack") rather than the URI-split form.
    Falls back to uri_to_text() for entities with no foaf:name entry.
    """
    FOAF_NAME = "http://xmlns.com/foaf/0.1/name"
    names: Dict[str, str] = {}

    with open(attr_triples_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) != 3:
                continue
            subj, pred, obj = parts
            if pred == FOAF_NAME and subj not in names:
                names[subj] = obj.strip('"')

    print(
        f"[dbpedia_names] Extracted names for {len(names)} entities from {attr_triples_path}")
    return names


def build_wikidata_names(attr_triples_path: str) -> Dict[str, str]:
    """
    Build a {uri: name} dict for Wikidata entities from attr_triples.

    Priority per entity:
      1. P373  — Wikipedia page title     (e.g. "Vincent Price")
      2. altLabel (skos)                  (e.g. "Acores")
      3. P1476 — work/film title          (e.g. "Reno 911!: Miami")
      4. schema.org/description           (e.g. "Canadian ice hockey player")

    Entities with none of the above fall back to uri_to_text()
    which returns the bare QID — LaBSE signal will be weak for those.
    """
    P373 = "http://www.wikidata.org/entity/P373"
    ALT_LABEL = "http://www.w3.org/2004/02/skos/core#altLabel"
    P1476 = "http://www.wikidata.org/entity/P1476"

    # schema.org/description is intentionally excluded — descriptions like
    # "Canadian ice hockey player" hurt LaBSE matching more than bare QIDs

    names: Dict[str, str] = {}

    priority = {P373: 1, ALT_LABEL: 2, P1476: 3}
    current_priority: Dict[str, int] = {}

    with open(attr_triples_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) != 3:
                continue
            subj, pred, obj = parts
            if pred not in priority:
                continue
            p = priority[pred]
            if subj not in current_priority or p < current_priority[subj]:
                names[subj] = obj.strip('"')
                current_priority[subj] = p

    covered = len(names)
    print(
        f"[wikidata_names] Extracted names for {covered} entities from {attr_triples_path}")
    return names


def uri_to_text(uri: str, names_dict: Optional[Dict] = None) -> str:
    """
    Convert entity URI to text for LaBSE encoding.

    Priority:
    1. Use names_dict if provided and has entry
    2. Extract from URI path
    3. Return empty string for blank nodes

    Examples:
        http://dbpedia.org/resource/Barack_Obama
            → "Barack Obama"
        http://de.dbpedia.org/resource/Sheffield_Wednesday
            → "Sheffield Wednesday"
        http://dbpedia.org/resource/A_Love_in_Germany
            → "A Love in Germany"
    """
    if names_dict and uri in names_dict:
        name = names_dict[uri]
        if name:
            return name

    if "genid" in uri or "/.well-known/" in uri:
        return ""

    name = uri.split("/")[-1]

    replacements = {
        "%20": " ", "%28": "(", "%29": ")",
        "%2C": ",", "%27": "'", "%26": "&",
        "%2F": "/", "%3A": ":", "%C3%BC": "ü",
        "%C3%B6": "ö", "%C3%A4": "ä", "%C3%9F": "ß",
        "%C3%A9": "é", "%C3%A8": "è", "%C3%AA": "ê",
        "%C3%B4": "ô", "%C3%A0": "à", "%C3%B9": "ù",
        "%C3%9C": "Ü", "%C3%96": "Ö", "%C3%84": "Ä",
    }
    for enc, dec in replacements.items():
        name = name.replace(enc, dec)

    name = name.replace("_", " ")
    name = " ".join(name.split())
    return name.strip()


class LaBSEEncoder:
    """
    Encodes entity names using the LaBSE multilingual
    sentence transformer model.

    LaBSE produces 768-dimensional vectors that are
    already aligned across 100+ languages. This means
    "Paris" in English and "Paris" in German will have
    very similar vectors without any training.

    Usage:
        encoder = LaBSEEncoder()
        matrix = encoder.encode_graph(G, names1, names2)
    """

    def __init__(
        self,
        model_name: str = "LaBSE",
        device: str = "auto",
        cache_dir: Optional[str] = None,
    ):
        """
        Initialize LaBSE encoder.

        Args:
            model_name : HuggingFace model name
                         "LaBSE" or full path to local model
            device     : "auto", "cpu", "cuda", "cuda:0" etc.
            cache_dir  : optional path to cache downloaded model
        """
        self.model_name = model_name
        self.cache_dir = cache_dir
        self.model = None
        self.dim = 768  # LaBSE output dimension

        if device == "auto":
            self.device = "cuda" if torch.cuda.is_available() else "cpu"
        else:
            self.device = device

        print(f"[labse] Device: {self.device}")

    def _load_model(self):
        """Load LaBSE model on first use."""
        if self.model is not None:
            return

        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ImportError(
                "sentence-transformers not installed.\n"
                "Install with: pip install sentence-transformers"
            )

        print(f"[labse] Loading LaBSE model...")
        print(f"[labse] This may take a minute on first run...")

        self.model = SentenceTransformer(
            self.model_name,
            cache_folder=self.cache_dir,
            device=self.device,
        )

        print(f"[labse] Model loaded successfully")

    def encode_texts(
        self,
        texts: List[str],
        batch_size: int = 512,
        normalize: bool = True,
    ) -> np.ndarray:
        """
        Encode a list of text strings to LaBSE vectors.

        Args:
            texts      : list of strings to encode
            batch_size : encoding batch size
            normalize  : L2 normalize output vectors

        Returns:
            numpy array of shape (len(texts), 768)
        """
        self._load_model()

        empty_mask = [t == "" for t in texts]
        texts_to_encode = [
            t if t else "entity" for t in texts
        ]

        print(
            f"[labse] Encoding {len(texts)} texts "
            f"(batch_size={batch_size})..."
        )

        embeddings = self.model.encode(
            texts_to_encode,
            batch_size=batch_size,
            show_progress_bar=True,
            normalize_embeddings=normalize,
            convert_to_numpy=True,
        )

        n_empty = sum(empty_mask)
        if n_empty > 0:
            embeddings[empty_mask] = 0.0
            print(
                f"[labse] Zeroed {n_empty} blank node "
                f"embeddings"
            )

        return embeddings.astype(np.float32)

    def encode_graph(
        self,
        G: MergedGraph,
        names1: Dict[str, str],
        names2: Dict[str, str],
        batch_size: int = 512,
        cache_path: Optional[str] = None,
    ) -> np.ndarray:
        """
        Encode all entities in the merged graph.

        Returns a matrix P of shape (n_entities, 768)
        where P[i] = LaBSE embedding of entity with
        global graph ID i.

        Entities from KG1 use names1 dict.
        Entities from KG2 use names2 dict.

        Args:
            G          : MergedGraph object
            names1     : {uri: name} for KG1 entities
            names2     : {uri: name} for KG2 entities
            batch_size : LaBSE encoding batch size
            cache_path : if given, save/load from this path
                         avoids recomputing on repeated runs

        Returns:
            numpy array shape (n_entities, 768)
        """
        # ── Try loading from cache ───────────────
        if cache_path and os.path.exists(cache_path):
            print(f"[labse] Loading cached embeddings: {cache_path}")
            cached = np.load(cache_path)
            if cached.shape[0] == G.n_entities:
                print(
                    f"[labse] Cache loaded: shape={cached.shape}"
                )
                return cached
            else:
                print(
                    f"[labse] Cache shape mismatch "
                    f"({cached.shape[0]} vs {G.n_entities}), "
                    f"recomputing..."
                )

        texts = []
        kg1_count = 0
        kg2_count = 0
        empty_count = 0

        for i in range(G.n_entities):
            uri = G.id2entity[i]

            if i in G.kg1_entity_ids:
                text = uri_to_text(uri, names1)
                kg1_count += 1
            else:
                text = uri_to_text(uri, names2)
                kg2_count += 1

            if not text:
                empty_count += 1

            texts.append(text)

        print(f"\n[labse] Text extraction summary:")
        print(f"         KG1 entities : {kg1_count}")
        print(f"         KG2 entities : {kg2_count}")
        print(f"         Empty names  : {empty_count}")

        print(f"\n[labse] Sample entity texts:")
        shown = 0
        for i in range(G.n_entities):
            if texts[i] and shown < 6:
                uri = G.id2entity[i]
                kg = "KG1" if i in G.kg1_entity_ids else "KG2"
                short = uri.split("/")[-1][:30]
                print(
                    f"         [{kg}] {short:30} "
                    f"→ '{texts[i]}'"
                )
                shown += 1

        P = self.encode_texts(
            texts,
            batch_size=batch_size,
            normalize=True,
        )

        print(f"\n[labse] LaBSE matrix shape: {P.shape}")

        self._verify_alignment_signal(G, P)

        # ── Save to cache ─────────────────────────
        if cache_path:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            np.save(cache_path, P)
            print(f"[labse] Saved to cache: {cache_path}")

        return P

    def encode_kg(
        self,
        G: "KGGraph",
        names: Dict[str, str],
        batch_size: int = 512,
        cache_path: Optional[str] = None,
    ) -> np.ndarray:
        """
        Encode all entities in a single KGGraph.

        Returns P of shape (n_entities, 768) where
        P[i] = LaBSE embedding of entity with local ID i.

        Args:
            G          : KGGraph (single KG)
            names      : {uri: name} dict for this KG
            batch_size : LaBSE encoding batch size
            cache_path : optional cache file path

        Returns:
            numpy array shape (n_entities, 768)
        """
        if cache_path and os.path.exists(cache_path):
            print(f"[labse] Loading cached embeddings: {cache_path}")
            cached = np.load(cache_path)
            if cached.shape[0] == G.n_entities:
                return cached
            print(f"[labse] Cache shape mismatch, recomputing...")

        texts = [
            uri_to_text(G.id2entity[i], names)
            for i in range(G.n_entities)
        ]

        P = self.encode_texts(texts, batch_size=batch_size, normalize=True)

        if cache_path:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            np.save(cache_path, P)
            print(f"[labse] Saved to cache: {cache_path}")

        return P

    def _verify_alignment_signal(
        self,
        G: MergedGraph,
        P: np.ndarray,
        n_samples: int = 10,
    ):
        """
        Check cosine similarity for:
        1. Aligned pairs    → should be HIGH (>0.7)
        2. Non-aligned pairs → should be LOW (<0.3)

        The gap between these two tells you how
        discriminative LaBSE is for your dataset.
        """
        align_edges = G.adj_lists.get(G.align_relation_id)

        if align_edges is None or len(align_edges) == 0:
            print("[labse] No ALIGN edges to verify")
            return

        aligned_pairs = []
        for dst, src in align_edges:
            if (dst in G.kg1_entity_ids
                    and src in G.kg2_entity_ids):
                aligned_pairs.append((dst, src))

        n = min(n_samples, len(aligned_pairs))
        sample_aligned = aligned_pairs[:n]

        aligned_sims = []
        for id1, id2 in sample_aligned:
            v1 = P[id1]
            v2 = P[id2]
            sim = float(np.dot(v1, v2))
            aligned_sims.append(sim)

        kg1_ids = list(G.kg1_entity_ids)
        kg2_ids = list(G.kg2_entity_ids)

        aligned_set = set(
            (dst, src) for dst, src in align_edges
            if dst in G.kg1_entity_ids
        )

        np.random.seed(42)
        random_kg1 = np.random.choice(kg1_ids, n * 3, replace=False)
        random_kg2 = np.random.choice(kg2_ids, n * 3, replace=False)

        non_aligned_sims = []
        for id1, id2 in zip(random_kg1, random_kg2):
            if (id1, id2) in aligned_set:
                continue
            v1 = P[id1]
            v2 = P[id2]
            sim = float(np.dot(v1, v2))
            non_aligned_sims.append(sim)
            if len(non_aligned_sims) >= n:
                break

        avg_aligned = np.mean(aligned_sims)
        avg_non_aligned = np.mean(non_aligned_sims)
        gap = avg_aligned - avg_non_aligned

        print(f"\n[labse] ── LaBSE Alignment Signal Check ──────")
        print(f"[labse] Aligned pair similarity:")
        print(f"         Average : {avg_aligned:.4f}")
        print(f"         Min     : {np.min(aligned_sims):.4f}")
        print(f"         Max     : {np.max(aligned_sims):.4f}")
        print(f"\n[labse] Non-aligned pair similarity:")
        print(f"         Average : {avg_non_aligned:.4f}")
        print(f"         Min     : {np.min(non_aligned_sims):.4f}")
        print(f"         Max     : {np.max(non_aligned_sims):.4f}")
        print(f"\n[labse] Discriminability gap: {gap:.4f}")
        print(f"         (aligned avg - non-aligned avg)")

        if gap > 0.5:
            print(
                f"[labse] ✓ Excellent — LaBSE very discriminative"
            )
        elif gap > 0.3:
            print(
                f"[labse] ✓ Good — LaBSE clearly discriminative"
            )
        elif gap > 0.1:
            print(
                f"[labse] ~ Moderate — LaBSE somewhat discriminative"
            )
        else:
            print(
                f"[labse] ✗ Weak — LaBSE not very discriminative"
            )

        print(f"[labse] ──────────────────────────────────────────")
