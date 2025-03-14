import os
from urllib.parse import urlparse
from SPARQLWrapper import SPARQLWrapper, JSON

def format_property_with_prefix(prop_uri, ns_dict):
    ns = extract_namespace(prop_uri)
    prefix = ns_dict.get(ns)
    local_name = extract_uri_name(prop_uri)
    if prefix:
        return f"{prefix}:{local_name}"
    else:
        return prop_uri

def generate_props_for_config(properties, ns_dict):
    if not properties:
        return ""

    xml_lines = []
    first_prop = format_property_with_prefix(properties[0], ns_dict)
    xml_lines.append(f"\n    <PROPERTY>{first_prop} AS nolang->lowercase</PROPERTY>")

    for prop in properties[1:]:
        formatted_prop = format_property_with_prefix(prop, ns_dict)
        xml_lines.append(f"    <OPTIONAL_PROPERTY>{formatted_prop} AS nolang->lowercase</OPTIONAL_PROPERTY>")
    return "\n".join(xml_lines)

def get_top_props(endpoint):
    query_path = '/scratch/hpc-prf-whale/albert/WHALE/WDC_scripts/linking_scripts/limes/linking/data/queries/query_props_coverage_all.rq'
    with open(query_path, 'r') as file:
        query = file.read()

    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    results = sparql.query().convert()

    top_props = []
    for result in results["results"]["bindings"]:
        coverage = float(result["coverage"]["value"])
        if coverage >= 50:
            top_props.append({
                "property": result["p"]["value"],
                "count": int(result["count"]["value"]),
                "coverage": coverage
            })
    return top_props

def load_config_template(template_file):
  with open(template_file, 'r', encoding='utf-8') as f:
    return f.read()

def extract_uri_name(uri):
    uri = uri.strip('<>')
    if '#' in uri:
        return uri.rsplit('#', 1)[1]
    else:
        return uri.rsplit('/', 1)[1]

def extract_namespace(uri):
    uri = uri.strip('<>')
    if '#' in uri:
        return uri.rsplit('#', 1)[0] + '#'
    else:
        return uri.rsplit('/', 1)[0] + '/'

def generate_prefix_label(uri, used_labels):
    parsed = urlparse(uri)
    host = parsed.netloc
    if host.startswith('www.'):
        host = host[4:]
    candidate = host.split('.')[0]

    if candidate in used_labels:
        used_labels[candidate] += 1
        candidate =f"{candidate}{used_labels[candidate]}"
    else:
        used_labels[candidate] = 1
    return candidate

def generate_config(s_cls_uri, t_cls_uri, output_dir, config_template, s_endpoint, t_endpoint, linking_output_dir, s_props_list, t_props_list):
    s_cls = extract_uri_name(s_cls_uri)
    t_cls = extract_uri_name(t_cls_uri)

    s_namespace = extract_namespace(s_cls_uri)
    t_namespace = extract_namespace(t_cls_uri)


    local_used_labels = {}
    ns_dict = {}

    for ns in (s_namespace, t_namespace):
        if ns not in ns_dict:
            ns_dict[ns] = generate_prefix_label(ns, local_used_labels)
    
    for prop in s_props_list + t_props_list:
        ns_prop = extract_namespace(prop)
        if ns_prop not in ns_dict:
            ns_dict[ns_prop] = generate_prefix_label(ns_prop, local_used_labels)

    source_properties_xml = generate_props_for_config(s_props_list, ns_dict)
    target_properties_xml = generate_props_for_config(t_props_list, ns_dict)

    prefixes = ""
    for ns, label in ns_dict.items():
        prefixes += f"\n  <PREFIX>\n    <NAMESPACE>{ns}</NAMESPACE>\n    <LABEL>{label}</LABEL>\n  </PREFIX>"

    config_content = config_template.format(
        prefixes=prefixes,
        s_cls=s_cls,
        t_cls=t_cls,
        s_prefix=ns_dict[s_namespace],
        t_prefix=ns_dict[t_namespace],
        s_endpoint=s_endpoint,
        t_endpoint=t_endpoint,
        linking_output_dir=linking_output_dir,
        source_properties=source_properties_xml,
        target_properties=target_properties_xml
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
    linking_output_dir='/scratch/hpc-prf-whale/albert/WHALE/LIMES/output/test/'
    list_classes = '/scratch/hpc-prf-whale/albert/WHALE/LIMES/output/starwars/class_alignment/same_as_total.nt'
    
    config_template = load_config_template(template_file)

    s_props_data = get_top_props(s_endpoint)
    t_props_data = get_top_props(t_endpoint)

    s_props_list = [entry['property'] for entry in s_props_data]
    t_props_list = [entry['property'] for entry in t_props_data]

    with open(list_classes, 'r', encoding='utf-8') as f:
      for line in f:
        parts = line.strip().split()
        s_uri = parts[0]
        t_uri = parts[1]
        generate_config(s_uri, t_uri, output_dir, config_template, s_endpoint, t_endpoint, linking_output_dir, s_props_list, t_props_list)
if __name__ == "__main__":
    main()
