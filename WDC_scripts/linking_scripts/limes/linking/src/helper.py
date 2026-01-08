import os
import logging
import subprocess
import hashlib
from rdflib.util import guess_format
from typing import List

def progress_file_for_chunk(cache_dir: str, class_alignment_file: str, chunk_index: int, num_chunks: int) -> str:
    os.makedirs(cache_dir, exist_ok=True)
    base = os.path.splitext(os.path.basename(class_alignment_file))[0]
    return os.path.join(cache_dir, f"{base}.done.chunk{chunk_index:05d}_of_{num_chunks:05d}.txt")

def load_done_pairs(progress_file: str) -> set[tuple[str, str]]:
    done: set[tuple[str, str]] = set()
    if not os.path.exists(progress_file):
        return done
    with open(progress_file, "r", encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 2:
                done.add((parts[0], parts[1]))
    return done

def append_done_pair(progress_file: str, s_uri: str, t_uri: str) -> None:
    with open(progress_file, "a", encoding="utf-8") as f:
        f.write(f"{s_uri}\t{t_uri}\n")
        f.flush()
        os.fsync(f.fileno())

def classify_limes(cp) -> str:
    out = cp.stdout or ""
    rc = cp.returncode

    if rc == 0:
        return "ok"
    
    if ("NullPointerException" in out) and ("mlm" in out) and ("is null" in out) and ("getLinkSpecification" in out):
        return "empty_ok"
    
    transient_markers = [
        "502", "503", "504", "Gateway", "Bad Gateway", "Service Unavailable",
        "SocketTimeoutException", "ConnectTimeoutException",
        "Connection reset", "Broken pipe", "Read timed out",
        "UnknownHostException", "ConnectException", "Connection refused",
        "SSLHandshakeException",
    ]
    if any(m in out for m in transient_markers):
        return "transient_fail"
    
    return "hard_fail"

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
            elif fmt in ['nquads', 'trig']:
                return 'NQUADS'
            else:
                logging.warning(f"Unknow format '{fmt}' for local file {source}. Using 'local' as type.")
                return 'local'
        else:
            logging.warning(f"Could not guess format for local file {source}. Using 'local' as type.")
            return 'local'
    else:
        return 'sparql'

def is_graph(graph):
    if graph:
        return f'<GRAPH>{graph}</GRAPH>'
    else:
        return ''

def compute_cache_filename(cache_dir: str, *args: str) -> str:
    if not args:
        raise ValueError("At least one input string must be provided to compute the cache filename.")
    
    key = "_".join(args)
    hash_val = hashlib.md5(key.encode('utf-8')).hexdigest()
    filename = f"{hash_val}.nt"
    return os.path.join(cache_dir, filename)

def run_limes(limes_jar: str, config_file: str) -> subprocess.CompletedProcess:
    command = [
        'java',
        '-Xmx240g',
        "-XX:+UseG1GC",
        '-jar', limes_jar, config_file
    ]
    logging.info(f"Running LIMES: {' '.join(command)}")

    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

    out_lines: List[str] = []
    assert proc.stdout is not None

    for line in proc.stdout:
        print(line, end="")
        out_lines.append(line)

    rc = proc.wait()
    out = "".join(out_lines)

    logging.info(f"LIMES finished with return code: {rc}")
    return subprocess.CompletedProcess(args=command, returncode=rc, stdout=out)

def run_limes_on_configs( limes_jar: str, config_dir: str) -> None:
    config_files = [
        os.path.join(config_dir, f)
        for f in os.listdir(config_dir)
        if f.endswith('.xml') and os.path.isfile(os.path.join(config_dir, f))
    ]

    logging.info(f"Found {len(config_files)} config files in {config_dir}")

    for config_file in config_files:
        run_limes(limes_jar, config_file)
        