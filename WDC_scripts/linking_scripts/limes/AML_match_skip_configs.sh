#!/bin/bash

DATA_DIR="/scratch-n1/hpc-prf-dsg/WHALE-data/domain_specific/linking_dataset/xfn"
CONFIG_DIR="/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/config_files/xfn"
OUTPUT_DIR="/scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/output_files/xfn"

start_time=$(date +%s)

# limit parallel
batch_size=500
current_process=0

skip_prefixes=("a" "b" "c" "d" "e" "f" "g" "h" "i" "j" "k" "l" "m" "n" "o" "p" "q" "r" "sa" "sc" "sh" "sif" "sil")

check_prefixes=true

# Extract configs
for data in "$DATA_DIR"/*txt; do
    base_name=$(basename "$data")

    if $check_prefixes; then
        if [[ $base_name == sindark* ]]; then
            echo "Encountered 'sindark' prefix. Processing without further prefix checks: $data"
            check_prefixes=false
        else
            skip_file=false
            for prefix in "${skip_prefixes[@]}"; do
                if [[ $base_name == $prefix* ]]; then
                    echo "Skipping: $data (matched prefix: $prefix)"
                    skip_file=true
                    break
                fi
            done

            if $skip_file; then
                continue
            fi
        fi
    else
        echo "Processing without prefix cheks: $data"
    fi


    echo "Extracting from: $data"
    python limes_config_extractor.py -p "$data" -c "$CONFIG_DIR" -o "$OUTPUT_DIR" specific &

    ((current_process++))

    if ((current_process >= batch_size)); then
        wait
        current_process=0
    fi
done

wait
echo "All configs were extracted."

# Run LIMES with extracted configs
current_process=0
for config in "$CONFIG_DIR"/*xml; do
    echo "Running LIMES with config: $config"
    java -jar /scratch-n1/hpc-prf-dsg/WHALE-output/limes/limes-core-1.8.1-WORDNET.jar "$config" &

    ((current_process++))

    if ((current_process >= batch_size)); then
        wait
        current_process=0
    fi
done
wait
echo "All configurations have been processed."

# Check LIMES' output
python check_triples.py "$OUTPUT_DIR"

end_time=$(date +%s)

elapsed_time=$((end_time - start_time))
echo "Total time taken: $elapsed_time seconds"