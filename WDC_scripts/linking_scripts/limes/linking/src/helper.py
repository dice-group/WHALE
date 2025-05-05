import os
import logging
import subprocess
import hashlib
from rdflib.util import guess_format

def get_endpoint_type(source: str) -> str:
    if os.path.exists(source) and os.path.isfile(source):
        fmt = guess_format(source)
        if fmt:
            fmt = fmt.lower()
            if fmt in ['nt', 'ntriples']:
                return 'N3'
            elif fmt in ['turtle']:
                return 'TURTLE'
            elif fmt in ['csv']:
                return 'CSV'
            elif fmt in ['xml']:
                return 'XML'
            else:
                logging.warning(f"Unknow format '{fmt}' for local file {source}. Using 'local' as type.")
                return 'local'
        else:
            logging.warning(f"Could not guess format for local file {source}. Using 'local' as type.")
            return 'local'
    else:
        return 'sparql'

def compute_cache_filename(cache_dir: str, *args: str) -> str:
    if not args:
        raise ValueError("At least one input string must be provided to compute the cache filename.")
    
    key = "_".join(args)
    hash_val = hashlib.md5(key.encode('utf-8')).hexdigest()
    filename = f"{hash_val}.nt"
    return os.path.join(cache_dir, filename)

def run_limes(limes_jar: str, config_file: str) -> None:
    command = ['java', '-Xmx16g', '-jar', limes_jar, config_file]
    logging.info(f"Running LIMES: {' '.join(command)}")
    subprocess.run(command, check=True)
    logging.info("LIMES process completed.")

def run_limes_on_configs( limes_jar: str, config_dir: str) -> None:
    config_files = [
        os.path.join(config_dir, f)
        for f in os.listdir(config_dir)
        if f.endswith('.xml') and os.path.isfile(os.path.join(config_dir, f))
    ]

    logging.info(f"Found {len(config_files)} config files in {config_dir}")

    for config_file in config_files:
        run_limes(limes_jar, config_file)
        