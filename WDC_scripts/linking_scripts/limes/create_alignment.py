import os
import subprocess
import argparse

script_dir = os.path.dirname(__file__)
base_dir = os.path.abspath(os.path.join(script_dir, "../", "../", "../")) 
aml_dir = os.path.join(base_dir, "AML_v3.2") 
execution_dir = os.path.join(aml_dir, "data")
aml_jar = os.path.join(aml_dir, "AgreementMakerLight.jar")

if not os.path.exists(execution_dir):
    print(f"{execution_dir} doesn't exist. Creating directory...")
    os.makedirs(execution_dir)

def run_script(script_name, args, is_java=False):

    if is_java:
        command = ['java', "-jar", script_name] + args
    else:
        full_script_path = os.path.join(script_dir, script_name)
        command = ["python3", full_script_path] + args

    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error occured while running {script_name}: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Run the alignment workflow.")
    parser.add_argument("-c1", "--nt_input_cls_1", required=True, help="Path to the first input class N_Triples file.")
    parser.add_argument("-c2", "--nt_input_cls_2", required=True, help="Path to the second input class N_Triples file.")
    parser.add_argument("-p1", "--nt_input_props_1", required=True, help="Path to the first input property N_Triples file.")
    parser.add_argument("-p2", "--nt_input_props_2", required=True, help="Path to the firsecondst input property N_Triples file.")
    args = parser.parse_args()

    nt_input_cls_1 = args.nt_input_cls_1
    nt_input_cls_2 = args.nt_input_cls_2
    nt_input_props_1 = args.nt_input_props_1
    nt_input_props_2 = args.nt_input_props_2

    rdf_output_cls_1 = os.path.join(execution_dir, "output_cls_1.rdf")
    rdf_output_cls_2 = os.path.join(execution_dir, "output_cls_2.rdf")
    rdf_output_props_1 = os.path.join(execution_dir, "output_props_1.rdf")
    rdf_output_props_2 = os.path.join(execution_dir, "output_props_2.rdf")
    class_alignment_file = os.path.join(execution_dir, "class_alignment_file.rdf")
    property_alignment_file = os.path.join(execution_dir, "property_alignment_file.rdf")
    class_nt_output = os.path.join(execution_dir, "class_output.nt")
    property_nt_output = os.path.join(execution_dir, "property_output.nt")


    run_script('add_class_type.py', [nt_input_cls_1, rdf_output_cls_1])
    run_script('add_class_type.py', [nt_input_cls_2, rdf_output_cls_2])
    run_script('add_class_type.py', [nt_input_props_1, rdf_output_props_1])
    run_script('add_class_type.py', [nt_input_props_2, rdf_output_props_2])

    aml_args = [
        "-s", rdf_output_cls_1,
        "-t", rdf_output_cls_2,
        "-o", class_alignment_file,
        "-m"
    ]
    run_script(aml_jar, aml_args, is_java=True)

    aml_args = [
        "-s", rdf_output_props_1,
        "-t", rdf_output_props_2,
        "-o", property_alignment_file,
        "-m"
    ]
    run_script(aml_jar, aml_args, is_java=True)

    run_script('rdf_to_nt.py', [class_alignment_file, class_nt_output, "eq"])
    run_script('rdf_to_nt.py', [property_alignment_file, property_nt_output, "eqp"])

    print("Alignment completed successfully.")

if __name__ == "__main__":
    main()
