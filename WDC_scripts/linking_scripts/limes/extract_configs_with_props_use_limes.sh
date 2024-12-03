#!/bin/bash
#SBATCH --job-name=xfn_AML_props
#SBATCH --time=21-00:00:00
#SBATCH --mem=32G
#SBATCH -p normal
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --output=/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/logs/job_output_%A_%a_xfn.log
#SBATCH --error=/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/logs/job_error_%A_%a_xfn.log
#SBATCH --mail-type=ALL
#SBATCH --mail-user=

DATA_DIR="/scratch-n1/hpc-prf-dsg/WHALE-data/domain_specific/linking_dataset/xfn"
CONFIG_DIR="/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/config_files/xfn_props"
OUTPUT_DIR="/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/output_files/xfn_props"
CLASS_MAP_DIR="/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/config_files/alignment/WDC_wikidata_verynear_AML.nt"
PROPS_MAP_DIR="/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/config_files/alignment/WDC_wikidata_props_alignment_heavy.nt"

start_time=$(date +%s)

total_cores=$(nproc --all)
busy_cores=$(squeue --user=$USER --noheader | grep " R" | wc -l)
available_cores=$((total_cores - busy_cores))
usable_cores=$((available_cores < 129 ? available_cores : 129))

echo "Using $usable_cores cores for processing."

echo "Extracting configs..."
find "$DATA_DIR" -name "*.txt" | xargs -n 1 -P "$usable_cores" -I {} python /scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/limes_config_extractor.py specific \
--path {} \
--config_output_path "$CONFIG_DIR" \
--output_path "$OUTPUT_DIR" \
--class_mapping "$CLASS_MAP_DIR" \
--property_mapping "$PROPS_MAP_DIR"

wait 

echo "Running LIMES..."
find "$CONFIG_DIR" -name "*.xml" | xargs -n 1 -P "$usable_cores" java -jar /scratch-n1/hpc-prf-dsg/WHALE-output/limes/limes-core-1.8.1-WORDNET.jar

python /scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/check_triples.py "$OUTPUT_DIR"

end_time=$(date +%s)

elapsed_time=$((end_time - start_time))

hours=$((elapsed_time / 3600))
minutes=$(( (elapsed_time % 3600) / 60 ))
seconds=$((elapsed_time % 60))

printf "Total time taken: %02d:%02d:%02d\n" $hours $minutes $seconds