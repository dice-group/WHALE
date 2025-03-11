import pandas as pd
import os

def load_config_template(template_file):
  with open(template_file, 'r', encoding='utf-8') as f:
    return f.read()

def extract_class_name(uri):
    return uri.strip('<>').rstrip('/').split('/')[-1]

def generate_config(s_cls, t_cls, output_dir, config_template):
    s_cls = extract_class_name(s_cls)
    t_cls = extract_class_name(t_cls)
    config_content = config_template.format(s_cls=s_cls, t_cls=t_cls)
    filename = f"{s_cls}_{t_cls}_config.xml"
    file_path = os.path.join(output_dir, filename)

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(config_content)
    print(f"Generated config for classes '{s_cls}' and '{t_cls}': {file_path}")

def main():
    list_classes = '/scratch/hpc-prf-whale/albert/WHALE/LIMES/output/starwars/class_alignment/same_as_total.nt'
    output_dir = '/scratch/hpc-prf-whale/albert/WHALE/LIMES/configs/starwars'

    template_file = '/scratch/hpc-prf-whale/albert/WHALE/WDC_scripts/linking_scripts/limes/config_files/templates/starwars.xml'

    config_template = load_config_template(template_file)

    with open(list_classes, 'r', encoding='utf-8') as f:
      for line in f:
        parts = line.strip().split()
        s_cls = parts[0]
        t_cls = parts[1]
        generate_config(s_cls, t_cls, output_dir, config_template)

if __name__ == "__main__":
    main()
