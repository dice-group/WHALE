import os
import sys
import yaml
import logging
import argparse
from typing import Dict, List, Tuple, Optional
from math import ceil

from sparql_query import get_top_props_cached
from xml_builder import generate_config, load_config_template
from align_classes import process_class_alignment
from helper import run_limes, compute_cache_filename, progress_file_for_chunk, load_done_pairs, append_done_pair, classify_limes
from merge_alignment import merge_alignments
from nt_converter import enhance_dataset_with_same_as

def split_into_chunks(items: List, chunk_index: int, num_chunks: int):
    if num_chunks <= 1:
        return items
    
    n = len(items)
    if n == 0:
        return items
    
    chunk_index = max(0, min(chunk_index, num_chunks - 1))
    chunk_size = ceil(n / num_chunks)
    start = chunk_index * chunk_size
    end = min(n, start + chunk_size)
    return items[start:end]

def load_class_pairs(class_alignment_file: str) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    with open(class_alignment_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            s_uri, t_uri = parts[0], parts[1]
            pairs.append((s_uri, t_uri))
    return pairs

def resolve_paths(config: Dict) -> Dict:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for key, value in config['file_paths'].items():
        if not os.path.isabs(value):
            config['file_paths'][key] = os.path.join(script_dir, value)
    return config

def load_config(config_file: str) -> Dict:
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_config = os.path.join(script_dir, '..', "config.yaml")

    parser = argparse.ArgumentParser(description='Process source and target endpoints.')

    parser.add_argument(
        "--config",
        type=str,
        default=default_config,
        help=f"Path to yaml config file (default: {default_config})",
    )

    parser.add_argument(
        "--enhance",
        action="store_true",
        help="Enhance datasets with owl:sameAs after merge."
    )

    parser.add_argument(
        "--stage",
        choices=["full", "class_align", "entity_align","merge_only"],
        default="full",
        help=(
            "Pipeline stage to run:\n"
            "full = do everything in one job,\n"
            "class_align = only run class-level LIMES alignment,\n"
            "entity_align = only run same-class entity alignment,\n"
            "merge_only = only merge entity alignments + enhance datasets." 
        ),
    )

    parser.add_argument(
        "--chunk-index",
        type=int,
        default=0,
        help="Index of the class-pair chunk to process (0-based).",
    )

    parser.add_argument(
        "--num-chunks",
        type=int,
        default=1,
        help="Total number of chunks the class-pairs are split into.",
    )

    parser.add_argument("--source_endpoint", type=str, help="Source endpoint URL", default=None)
    parser.add_argument("--target_endpoint", type=str, help="Target endpoint URL", default=None)
    
    args = parser.parse_args()

    config_file = args.config
    config = load_config(config_file)
    config = resolve_paths(config)

    logging_level = config['logging']['level']
    logging.basicConfig(level=logging_level,format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout,)

    logging.info(f"Using config: {os.path.abspath(config_file)}")

    s_endpoint = args.source_endpoint if args.source_endpoint else config['endpoints']['s_endpoint']
    t_endpoint = args.target_endpoint if args.target_endpoint else config['endpoints']['t_endpoint']

    s_graph = config['endpoints'].get('s_graph', '') or ''
    t_graph = config['endpoints'].get('t_graph', '') or ''

    config['endpoints']['s_endpoint'] = s_endpoint
    config['endpoints']['t_endpoint'] = t_endpoint
    config['endpoints']['s_graph'] = s_graph
    config['endpoints']['t_graph'] = t_graph
    
    template_file = config['file_paths']['template_file']
    config_output_dir = config['file_paths']['config_output_dir']
    linking_output_dir = config['file_paths']['linking_output_dir']
    limes_path = config['file_paths']['limes_jar']
    cache_dir = config['file_paths']['cache_dir']

    config_template = load_config_template(template_file)

    s_props_data = get_top_props_cached(cache_dir, s_endpoint, graph=s_graph)
    t_props_data = get_top_props_cached(cache_dir, t_endpoint, graph=t_graph)
    s_props_list = [entry['property'] for entry in s_props_data]
    t_props_list = [entry['property'] for entry in t_props_data]

    class_alignment_file: Optional[str] = None

    if args.stage in ("full", "class_align", "entity_align"):
        if args.stage in ("full", "class_align"):
            class_alignment_file = process_class_alignment(config)
        else:
            class_alignment_file = compute_cache_filename(
                cache_dir,
                s_endpoint,
                t_endpoint,
            )
            if not os.path.exists(class_alignment_file):
                raise FileNotFoundError(
                    f"Class alignment file not found: {class_alignment_file}. "
                    f"Run with --stage class_align first."
                )
            
    if args.stage in ("full", "entity_align"):
        assert class_alignment_file is not None
        all_pairs = load_class_pairs(class_alignment_file)
        logging.info(f"Total class pairs in alignment file: {len(all_pairs)} {class_alignment_file}")

        class_pairs_chunk = split_into_chunks(
            all_pairs,
            args.chunk_index,
            args.num_chunks,
        )
        logging.info(
            f"Chunk {args.chunk_index + 1}/{args.num_chunks} "
            f"has {len(class_pairs_chunk)} class pairs"
        )

        progress_file = progress_file_for_chunk(cache_dir, class_alignment_file, args.chunk_index, args.num_chunks)
        done_pairs = load_done_pairs(progress_file)
        if done_pairs:
            logging.info(f"Loaded {len(done_pairs)} done pairs from {progress_file}")

        total_in_chunk = len(class_pairs_chunk)
        failed = 0

        for s_uri, t_uri in class_pairs_chunk:
            if (s_uri, t_uri) in done_pairs:
                continue

            linking_config_file = generate_config(
                s_uri, 
                t_uri, 
                s_graph,
                t_graph,
                config_output_dir, 
                config_template, 
                s_endpoint, 
                t_endpoint, 
                linking_output_dir, 
                s_props_list, 
                t_props_list
            )

            try:
                cp = run_limes(limes_path, linking_config_file)
                status = classify_limes(cp)

                if status in ("ok", "empty_ok"):
                    append_done_pair(progress_file, s_uri, t_uri)
                    done_pairs.add((s_uri, t_uri))
                    logging.info(f"Marked done: {s_uri} {t_uri}")
                else:
                    failed += 1
                    tail = (cp.stdout or "")[-4000:]
                    logging.error(f"LIMES failed ({status}) for {s_uri} {t_uri}. Last output:\n{tail}")

            finally:
                if os.path.exists(linking_config_file):
                    os.remove(linking_config_file)

        logging.info(f"Failed pairs: {failed}/{total_in_chunk}")
    
    if args.stage in ("full", "merge_only"):
        same_as_file = merge_alignments(linking_output_dir)

        if args.enhance:
            enhance_dataset_with_same_as(s_endpoint, same_as_file)
            enhance_dataset_with_same_as(t_endpoint, same_as_file, 't')

if __name__ == "__main__":
    main()