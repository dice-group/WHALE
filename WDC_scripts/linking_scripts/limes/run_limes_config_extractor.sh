#!/bin/bash

DIR="/scratch-n1/hpc-prf-dsg/WHALE-data/domain_specific/linking_dataset/xfn"

for data in "$DIR"/*.txt; do
    echo "Extracting from: $data"
    python limes_config_extractor.py -p "$data" -c /scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/config_files/xfn/ -o /scratch/hpc-prf-lola/albert/WHALE/WDC_scripts/linking_scripts/limes/output_files/xfn specific &
done

wait 
echo "All configs were extracted."