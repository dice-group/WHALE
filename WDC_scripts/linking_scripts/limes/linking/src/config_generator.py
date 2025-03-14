import yaml
import os
import logging
from typing import List, Dict
from sparql_query import get_top_props
from xml_builder import generate_config

def resolve_paths(config: dict) -> dict:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for key, value in config['file_paths'].items():
        if not os.path.isabs(value):
            config['file_paths'][key] = os.path.join(script_dir, value)

    return config

def load_config(config_file: str) -> Dict:
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def load_config_template(template_file: str) -> str:
  with open(template_file, 'r', encoding='utf-8') as f:
    return f.read()

def main():
    config = load_config('config.yaml')
    config = resolve_paths(config)

    s_endpoint = config['endpoints']['s_endpoint']
    t_endpoint = config['endpoints']['t_endpoint']
    template_file = config['file_paths']['template_file']
    output_dir = config['file_paths']['config_output_dir']
    linking_output_dir = config['file_paths']['linking_output_dir']
    list_classes = config['file_paths']['list_classes']
    query_path = config['file_paths']['query_path']

    logging_level = config['logging']['level']
    logging.basicConfig(level=logging_level)

    config_template = load_config_template(template_file)

    s_props_data = get_top_props(s_endpoint, query_path)
    t_props_data = get_top_props(t_endpoint, query_path)
    s_props_list = [entry['property'] for entry in s_props_data]
    t_props_list = [entry['property'] for entry in t_props_data]

    with open(list_classes, 'r', encoding='utf-8') as f:
      for line in f:
        parts = line.strip().split()
        s_uri, t_uri = parts[0], parts[1]
        generate_config(s_uri, t_uri, output_dir, config_template, 
                        s_endpoint, t_endpoint, linking_output_dir, 
                        s_props_list, t_props_list)

if __name__ == "__main__":
    main()