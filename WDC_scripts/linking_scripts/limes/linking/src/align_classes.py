import os 
import subprocess
import logging
from typing import Dict
from xml_builder import load_config_template
from helper import run_limes, compute_cache_filename, get_endpoint_type

def generate_alignment_config_file(config: Dict, temp_config_path: str, output_alignment_file: str) -> None:
    logging.info("Generating temporary alignment config file for LIMES...")
    alignment_template_file = config['file_paths']['class_alignment_template_file']
    s_endpoint = config['endpoints']['s_endpoint']
    t_endpoint = config['endpoints']['t_endpoint']
        
    template = load_config_template(alignment_template_file)

    s_type = get_endpoint_type(s_endpoint)
    t_type = get_endpoint_type(t_endpoint)

    config_content = template.format(
        s_endpoint=s_endpoint,
        t_endpoint=t_endpoint,
        class_alignment=output_alignment_file,
        s_type=s_type,
        t_type=t_type,
    )

    with open(temp_config_path, 'w', encoding='utf-8') as f:
        f.write(config_content)
    logging.info(f"Temporary alignment config file generated: {temp_config_path}")

def process_class_alignment(config: Dict) -> str:
    s_endpoint = config['endpoints']['s_endpoint']
    t_endpoint = config['endpoints']['t_endpoint']
    cache_dir = config['file_paths']['cache_dir']

    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
        logging.info(f"Created alignment cache directory: {cache_dir}")

    cache_file = compute_cache_filename(cache_dir, s_endpoint, t_endpoint)

    if os.path.exists(cache_file):
        logging.info(f"Using cached alignment file: {cache_file}")
    else:
        logging.info(f"No cached alignment file found. Generating a new one...")
        temp_config_file = os.path.join(cache_dir, "temp_alignment_config.xml")
        generate_alignment_config_file(config, temp_config_file, cache_file)

        limes_jar = config['file_paths']['limes_jar']
        try:
            run_limes(limes_jar, temp_config_file)
        except Exception as e:
            logging.error(f"LIMES process failed: {e}")
            raise
        finally:
            if os.path.exists(temp_config_file):
                os.remove(temp_config_file)
                logging.info(f"Temporary alignment config file {temp_config_file} deleted.")
    
    return cache_file
