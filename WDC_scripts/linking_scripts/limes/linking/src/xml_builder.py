import os
import logging
from typing import List, Dict
from uri_utils import extract_namespace, extract_uri_name, generate_prefix_label

#---------------------------- Property formatting --------------------------
def format_property_with_prefix(prop_uri: str, ns_dict: Dict[str, str]) -> str:
    ns = extract_namespace(prop_uri)
    prefix = ns_dict.get(ns)
    local_name = extract_uri_name(prop_uri)
    return f"{prefix}:{local_name}" if prefix else prop_uri

def generate_props_for_config(properties: List[str], ns_dict: Dict[str, str]) -> str:
    if not properties:
        return ""
    xml_lines = [
        f"    <PROPERTY>{format_property_with_prefix(properties[0], ns_dict)} AS nolang->lowercase</PROPERTY>"
        ]
    for prop in properties[1:]:
        xml_lines.append(f"    <OPTIONAL_PROPERTY>{format_property_with_prefix(prop, ns_dict)} AS nolang->lowercase</OPTIONAL_PROPERTY>")
    return "\n".join(xml_lines)

#---------------------------- Config Generation --------------------------
def generate_config(
    s_cls_uri: str, 
    t_cls_uri: str, 
    output_dir: str, 
    config_template: str, 
    s_endpoint: str, 
    t_endpoint: str, 
    linking_output_dir: str, 
    s_props_list: List[str], 
    t_props_list: List[str]
    ) -> None:
    s_cls, t_cls = extract_uri_name(s_cls_uri), extract_uri_name(t_cls_uri)
    s_namespace, t_namespace = extract_namespace(s_cls_uri), extract_namespace(t_cls_uri)

    local_used_labels: Dict[str, int] = {}
    ns_dict: Dict[str, str] = {
        s_namespace: generate_prefix_label(s_namespace, local_used_labels), 
        t_namespace: generate_prefix_label(t_namespace, local_used_labels)
    }
    
    for prop in s_props_list + t_props_list:
        ns_prop = extract_namespace(prop)
        if ns_prop not in ns_dict:
            ns_dict[ns_prop] = generate_prefix_label(ns_prop, local_used_labels)

    source_properties_xml = generate_props_for_config(s_props_list, ns_dict)
    target_properties_xml = generate_props_for_config(t_props_list, ns_dict)

    prefixes = "\n".join(
        f"  <PREFIX>\n    <NAMESPACE>{ns}</NAMESPACE>\n    <LABEL>{label}</LABEL>\n  </PREFIX>" 
        for ns, label in ns_dict.items()
    )

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
    logging.info(f"Generated config for classes '{s_cls}' and '{t_cls}': {file_path}")