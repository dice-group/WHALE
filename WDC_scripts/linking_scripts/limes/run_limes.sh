#!/bin/bash
#SBATCH --job-name=xfn_limes
#SBATCH --time=21-00:00:00
#SBATCH --mem=128G
#SBATCH -p normal
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --output=/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/logs/job_output_%A_%a.log
#SBATCH --error=/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/logs/job_error_%A_%a.log
#SBATCH --mail-type=ALL
#SBATCH --mail-user=akhomich@mail.uni-paderborn.de

CONFIG_DIR="/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/config_files/xfn"
OUTPUT_DIR="/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/output_files/xfn"

start_time=$(date +%s)

estimated_memory_per_process=2 #Gb
total_memory_gb=128
reserved_memory=8 #Gb
usable_memory=$(( total_memory_gb - reserved_memory ))
max_processes=$(( usable_memory / estimated_memory_per_process ))

usable_cores=$(( SLURM_CPUS_PER_TASK < max_processes ? SLURM_CPUS_PER_TASK : max_processes ))
 
# Run LIMES with extracted configs
echo "Using $usable_cores cores for processing." 
echo "Running LIMES..."

java_heap_size=2G

export JAVA_OPTS="-Xmx${java_heap_size}"

if command -v parallel &> /dev/null; then
    find "$CONFIG_DIR" -name "*.xml" | parallel -j "$usable_cores" java -Xmx${java_heap_size} -jar /scratch-n1/hpc-prf-dsg/WHALE-output/limes/limes-core-1.8.1-WORDNET.jar {}
else
    find "$CONFIG_DIR" -name "*.xml" | xargs -n 1 -P "$usable_cores" java -Xmx${java_heap_size} -jar /scratch-n1/hpc-prf-dsg/WHALE-output/limes/limes-core-1.8.1-WORDNET.jar
fi

# Check LIMES' output
python check_triples.py "$OUTPUT_DIR"

end_time=$(date +%s)

elapsed_time=$((end_time - start_time))

hours=$((elapsed_time / 3600))
minutes=$(( (elapsed_time % 3600) / 60 ))
seconds=$((elapsed_time % 60))

printf "Total time taken: %02d:%02d:%02d\n" $hours $minutes $seconds