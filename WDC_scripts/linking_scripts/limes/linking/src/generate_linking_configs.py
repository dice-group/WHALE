import os
from urllib.parse import urlparse

def load_config_template(template_file):
  with open(template_file, 'r', encoding='utf-8') as f:
    return f.read()

def extract_class_name(uri):
    return uri.strip('<>').rstrip('/').split('/')[-1]

def extract_namespace(uri):
    parts = uri.strip('<>').rstrip('/').split('/')
    return '/'.join(parts[:-1]) + '/'

def generate_prefix_label(uri, used_labels):
    parsed = urlparse(uri)
    candidate = parsed.netloc[:3]

    if candidate in used_labels:
        path_parts = [p for p in parsed.path.split('/') if p]
        if path_parts:
            candidate = path_parts[0][:3]

    if candidate in used_labels:
        used_labels[candidate] += 1
        candidate = f"{candidate}{used_labels[candidate]}"
    else:
        used_labels[candidate] = 1
    return candidate

def generate_config(s_cls_uri, t_cls_uri, output_dir, config_template, s_endpoint, t_endpoint, linking_output_dir):
    s_cls = extract_class_name(s_cls_uri)
    t_cls = extract_class_name(t_cls_uri)

    s_namespace = extract_namespace(s_cls_uri)
    t_namespace = extract_namespace(t_cls_uri)


    local_used_labels = {}

    ns_dict = {}
    for ns in (s_namespace, t_namespace):
        if ns not in ns_dict:
            ns_dict[ns] = generate_prefix_label(ns, local_used_labels)

    s_label = ns_dict[s_namespace]
    t_label = ns_dict[t_namespace]
    
    prefixes = ""
    for ns, label in ns_dict.items():
        prefixes += f"\n  <PREFIX>\n    <NAMESPACE>{ns}</NAMESPACE>\n    <LABEL>{label}</LABEL>\n  </PREFIX>"

    config_content = config_template.format(
        prefixes=prefixes,
        s_cls=s_cls,
        t_cls=t_cls,
        s_prefix=s_label,
        t_prefix=t_label,
        s_endpoint=s_endpoint,
        t_endpoint=t_endpoint,
        linking_output_dir=linking_output_dir
    )

    filename = f"{s_cls}_{t_cls}_config.xml"
    file_path = os.path.join(output_dir, filename)

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(config_content)
    print(f"Generated config for classes '{s_cls}' and '{t_cls}': {file_path}")

def main():
    s_endpoint = 'http://localhost:3030/galaxies'
    t_endpoint = 'http://localhost:3030/the-old-republic-wiki'
    template_file = '/scratch/hpc-prf-whale/albert/WHALE/WDC_scripts/linking_scripts/limes/linking/data/templates/test.xml'
    output_dir = '/scratch/hpc-prf-whale/albert/WHALE/LIMES/configs/test'
    linking_output_dir='/scratch/hpc-prf-whale/albert/WHALE/LIMES/output/starwars/'
    list_classes = '/scratch/hpc-prf-whale/albert/WHALE/LIMES/output/starwars/class_alignment/same_as_total.nt'
    config_template = load_config_template(template_file)

    with open(list_classes, 'r', encoding='utf-8') as f:
      for line in f:
        parts = line.strip().split()
        s_uri = parts[0]
        t_uri = parts[1]
        generate_config(s_uri, t_uri, output_dir, config_template, s_endpoint, t_endpoint, linking_output_dir)

if __name__ == "__main__":
    main()
