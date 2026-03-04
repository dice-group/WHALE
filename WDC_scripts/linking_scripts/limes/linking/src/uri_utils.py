from typing import Dict
from urllib.parse import urlparse

def extract_uri_name(uri: str) -> str:
    uri = uri.strip('<>')
    return uri.rsplit('#')[-1] if '#' in uri else uri.rsplit('/')[-1]

def extract_namespace(uri: str) -> str:
    uri = uri.strip('<>')
    return uri.rsplit('#', 1)[0] + '#' if '#' in uri else uri.rsplit('/', 1)[0] + '/'

def generate_prefix_label(uri: str, used_labels: Dict[str, int]) -> str:
    parsed = urlparse(uri)
    host = parsed.netloc[4:] if parsed.netloc.startswith('www.') else parsed.netloc
    candidate = host.split('.')[0]
    used_labels[candidate] = used_labels.get(candidate, 0) + 1
    return f"{candidate}{used_labels[candidate]}" if used_labels[candidate] > 1 else candidate