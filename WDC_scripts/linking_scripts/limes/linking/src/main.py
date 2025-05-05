import os
import yaml
import logging
import subprocess
import argparse
from typing import Dict
from sparql_query import get_top_props_cached
from xml_builder import generate_config, load_config_template
from align_classes import process_class_alignment
from helper import run_limes
from merge_alignment import merge_alignments
from nt_converter import enhance_dataset_with_same_as

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
    parser = argparse.ArgumentParser(description='Process source and target endpoints.')
    parser.add_argument("--source_endpoint", type=str, help="Source endpoint URL", default=None)
    parser.add_argument("--target_endpoint", type=str, help="Target endpoint URL", default=None)
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, '..', 'config.yaml')

    config = load_config(config_file)
    config = resolve_paths(config)

    logging_level = config['logging']['level']
    logging.basicConfig(level=logging_level,format="%(asctime)s [%(levelname)s] %(message)s")

    s_endpoint = args.source_endpoint if args.source_endpoint else config['endpoints']['s_endpoint']
    t_endpoint = args.target_endpoint if args.target_endpoint else config['endpoints']['t_endpoint']

    config['endpoints']['s_endpoint'] = s_endpoint
    config['endpoints']['t_endpoint'] = t_endpoint
    
    template_file = config['file_paths']['template_file']
    config_output_dir = config['file_paths']['config_output_dir']
    linking_output_dir = config['file_paths']['linking_output_dir']
    query_path = config['file_paths']['query_path']
    limes_path = config['file_paths']['limes_jar']
    cache_dir = config['file_paths']['cache_dir']

    config_template = load_config_template(template_file)

    s_props_data = get_top_props_cached(cache_dir, s_endpoint, query_path)
    t_props_data = get_top_props_cached(cache_dir, t_endpoint, query_path)
    s_props_list = [entry['property'] for entry in s_props_data]
    t_props_list = [entry['property'] for entry in t_props_data]

    class_alignment_file = process_class_alignment(config)

    with open(class_alignment_file, 'r', encoding='utf-8') as f:
      for line in f:
        parts = line.strip().split()
        s_uri, t_uri = parts[0], parts[1]
        linking_config_file = generate_config(
            s_uri, 
            t_uri, 
            config_output_dir, 
            config_template, 
            s_endpoint, 
            t_endpoint, 
            linking_output_dir, 
            s_props_list, 
            t_props_list
        )

        try:
            run_limes(limes_path, linking_config_file)
        except subprocess.CalledProcessError as e:
            logging.error(f"LIMES process failed for config {linking_config_file}: {e}")
        os.remove(linking_config_file)
    
    same_as_file = merge_alignments(linking_output_dir)

    enhance_dataset_with_same_as(s_endpoint, same_as_file)
    enhance_dataset_with_same_as(t_endpoint, same_as_file, 't')

if __name__ == "__main__":
    main()