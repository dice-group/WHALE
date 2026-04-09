# modules/pipeline.py
import os
import torch
import pandas as pd
from sklearn.model_selection import train_test_split

# === Model and training ===
from modules.models.train import train_alignment_model 


from modules.data_loader import (
    extract_files_from_directory,
    load_embeddings,
    load_parquet_triples,
    remove_brackets_from_indices,
    build_alignment_dict,
    clean_dict,
    load_triples_from_files,
    load_triples,
    clean_uri,
    load_alignment_links, create_train_val_test_matrices_from_links, load_parquet_triples
)


def run_pipeline_for_ckeci(
    directory_1,
    directory_2,
    alignment_dir,
    test_triples_path,
    output_dir,
    device="cpu",
):
    # 1) Load embeddings & IDs
    m1, e2i1, r2i1 = extract_files_from_directory(directory_1)
    m2, e2i2, r2i2 = extract_files_from_directory(directory_2)

    ent_df1, rel_df1 = load_embeddings(m1, e2i1, r2i1)
    ent_df2, rel_df2 = load_embeddings(m2, e2i2, r2i2)

    ent1 = remove_brackets_from_indices(ent_df1)
    ent2 = remove_brackets_from_indices(ent_df2)
    rel1 = remove_brackets_from_indices(rel_df1)
    rel2 = remove_brackets_from_indices(rel_df2)
    ent1.index = ent1.index.map(clean_uri)
    ent2.index = ent2.index.map(clean_uri)

    print("\n=== EMBEDDING SAMPLE ===")
    print(list(ent1.index)[:5])
    print(list(ent2.index)[:5])
    
    print(f"\n[Loading KG1 triples from {args.train_triples_path_1}]")
    triples_1 = load_triples(args.train_triples_path_1)
    print(f" KG1 triples loaded: {len(triples_1)} triples (first 3): {triples_1[:3]}")
    triples_2 = load_triples(args.train_triples_path_2)
    print(f" KG2 triples loaded: {len(triples_2)} triples (first 3): {triples_2[:3]}")


    if alignment_dir is not None:
        alignment_dict = clean_dict(build_alignment_dict(alignment_dir))
    else:
        alignment_dict = {}
        
    
    alignment_dict = {
        e1: e2
        for e1, e2 in alignment_dict.items()
        if e1 in ent1.index and e2 in ent2.index
    }

    print(f"Valid alignment after filtering: {len(alignment_dict)}")

    if len(alignment_dict) == 0:
        raise ValueError(" No valid alignment pairs found after filtering!")

    all_links = list(alignment_dict.items())
    
    print("\n=== DEBUG: ALIGNMENT vs EMBEDDINGS ===")

    for e1, e2 in list(alignment_dict.items())[:10]:
        if e1 not in ent1.index:
            print("NOT IN ent1:", repr(e1))
            break

    for idx in list(ent1.index)[:10]:
        print("EMB SAMPLE:", repr(idx))
        break

    all_links = list(alignment_dict.items())
    n_links = len(all_links)

    print(f"\nTotal alignment links: {n_links}")

    if n_links < 50:
        print("Small alignment set (<50) → using train + val only")

        train_links, val_links = train_test_split(
            all_links, test_size=0.2, random_state=42
        )
        test_links = [] 

    else:
        train_links, temp_links = train_test_split(
            all_links, test_size=0.2, random_state=42
        )
        val_links, test_links = train_test_split(
            temp_links, test_size=0.5, random_state=42
        )

    print(f"Train: {len(train_links)}, Val: {len(val_links)}, Test: {len(test_links)}")


    (S_train, T_train,
     S_val, T_val,
     S_test, T_test,
     S_train_keys, T_train_keys,
     S_val_keys, T_val_keys,
     S_test_keys, T_test_keys) = create_train_val_test_matrices_from_links(
        train_links,
        val_links,
        test_links,
        ent1, ent2
    )
    
    merged_rel = pd.concat([rel1, rel2])
    merged_rel = merged_rel[~merged_rel.index.duplicated(keep="first")]
    triples_batch = load_triples_from_files([args.train_triples_path_1,args.train_triples_path_2])
    kg1_triples = load_triples_from_files([args.train_triples_path_1])
    kg2_triples = load_triples_from_files([args.train_triples_path_2])
    triples_for_gcn = kg1_triples + kg2_triples

    val_triples, _ = train_test_split(triples_batch, test_size=0.01, random_state=42)

    final_model = train_alignment_model(
        input_dim=256, 
        hidden_dim=256, 
        epochs=20, 
        lr=0.001, 
        S_test_keys=S_test_keys,
        T_test_keys=T_test_keys,
        entity_embeddings1=ent1,
        entity_embeddings2=ent2,
        relation_embeddings=merged_rel,
        output_dir=output_dir,
        triples_batch=triples_for_gcn,
        kg_1=kg1_triples,
        kg_2=kg2_triples,
        device=device,
        S_train_keys=S_train_keys,
        T_train_keys=T_train_keys,
        S_val_keys=S_val_keys,
        T_val_keys=T_val_keys,
        val_triples=val_triples,
        train_triples=triples_batch,
        directory_1=directory_1,
        test_triples_path=args.test_triples_path
    )

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run full NAAS + CKECI pipeline")
    parser.add_argument("--directory_1", required=True)
    parser.add_argument("--directory_2", required=True)
    parser.add_argument("--alignment_dir", default=None)
    parser.add_argument("--train_triples_path_1", required=True, help="Training triples for KG1 (e.g., DBpedia)")
    parser.add_argument("--train_triples_path_2", required=True, help="Training triples for KG2 (e.g., Wikipedia)")
    parser.add_argument("--test_triples_path", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--device", default="cpu")
    args = parser.parse_args()

    run_pipeline_for_ckeci(
        directory_1=args.directory_1,
        directory_2=args.directory_2,
        alignment_dir=args.alignment_dir,
        test_triples_path=args.test_triples_path,
        output_dir=args.output_dir,
        device=args.device
    )
