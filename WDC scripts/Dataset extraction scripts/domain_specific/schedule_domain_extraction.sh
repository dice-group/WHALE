#!/bin/bash

# Define the directory
RAW_DATA_DIR="raw_data"

# Check if the directory exists, if not, create it
if [ ! -d "$RAW_DATA_DIR" ]; then
    echo "Directory $RAW_DATA_DIR does not exist. Creating it now."
    mkdir -p "$RAW_DATA_DIR"
else
    echo "Directory $RAW_DATA_DIR already exists."
fi

# Array of URLs to download
urls=(
    "https://files.dice-research.org/datasets/WHALE/WDC/adr/dpef.html-adr.nq-all.tar.gz"
    "https://files.dice-research.org/datasets/WHALE/WDC/geo/dpef.html-geo.nq-all.tar.gz"
    "https://files.dice-research.org/datasets/WHALE/WDC/hcalendar/dpef.html-hcalendar.nq-all.tar.gz"
    "https://files.dice-research.org/datasets/WHALE/WDC/hlisting/dpef.html-hlisting.nq-all.tar.gz"
    "https://files.dice-research.org/datasets/WHALE/WDC/hrecipe/dpef.html-hrecipe.nq-all.tar.gz"
    "https://files.dice-research.org/datasets/WHALE/WDC/hresume/dpef.html-hresume.nq-all.tar.gz"
    "https://files.dice-research.org/datasets/WHALE/WDC/hreview/dpef.html-hreview.nq-all.tar.gz"
    "https://files.dice-research.org/datasets/WHALE/WDC/species/dpef.html-mf-species.nq-all.tar.gz"
    "https://files.dice-research.org/datasets/WHALE/WDC/xfn/dpef.html-xfn.nq-all.tar.gz"
)

# Loop over each URL to download and extract
for url in "${urls[@]}"; do
    echo "Downloading $url..."
    wget "$url" -P "$RAW_DATA_DIR"
    
    # Extract the downloaded file
    filename=$(basename "$url")
    echo "Extracting $filename..."
    tar -xzf "$RAW_DATA_DIR/$filename" -C "$RAW_DATA_DIR"
done

echo "All files have been downloaded and extracted to $RAW_DATA_DIR."

declare -A urls
urls=(
    ["https://webdatacommons.org/structureddata/2023-12/files/html-rdfa.list"]="raw_data/rdfa"
    ["https://webdatacommons.org/structureddata/2023-12/files/html-embedded-jsonld.list"]="raw_data/jsonld"
    ["https://webdatacommons.org/structureddata/2023-12/files/html-microdata.list"]="raw_data/microdata"
    ["https://webdatacommons.org/structureddata/2023-12/files/html-mf-hcard.list"]="raw_data/hcard"
)

# Loop over each URL and download files to the corresponding directory
for url in "${!urls[@]}"; do
    dest_dir="${urls[$url]}"
    
    # Create the destination directory if it doesn't exist
    if [ ! -d "$dest_dir" ]; then
        echo "Directory $dest_dir does not exist. Creating it now."
        mkdir -p "$dest_dir"
    else
        echo "Directory $dest_dir already exists."
    fi
    
    # Download the .gz files listed in the .list file
    echo "Downloading files listed in $url to $dest_dir..."
    wget -i "$url" -P "$dest_dir"
done

echo "All files have been downloaded to their respective directories."

declare -a python_commands=(
    "python3 domain_extraction.py raw_data/dpef.html-adr.nq-all domain_files/adr_domains.txt domain_dataset/adr_dataset"
    "python3 domain_extraction.py raw_data/dpef.html-geo.nq-all domain_files/geo_domains.txt domain_dataset/geo_dataset"
    "python3 domain_extraction.py raw_data/dpef.html-hcalendar.nq-all domain_files/hcalendar_domains.txt domain_dataset/hcalendar_dataset"
    "python3 domain_extraction.py raw_data/dpef.html-hlisting.nq-all domain_files/hlisting_domains.txt domain_dataset/hlisting_dataset"
    "python3 domain_extraction.py raw_data/dpef.html-hrecipe.nq-all domain_files/hrecipe_domains.txt domain_dataset/hrecipe_dataset"
    "python3 domain_extraction.py raw_data/dpef.html-hresume.nq-all domain_files/hresume_domains.txt domain_dataset/hresume_dataset"
    "python3 domain_extraction.py raw_data/dpef.html-hreview.nq-all domain_files/hreview_domains.txt domain_dataset/hreview_dataset"
    "python3 domain_extraction.py raw_data/dpef.html-mf-species.nq-all domain_files/species_domains.txt domain_dataset/species_dataset"
    "python3 domain_extraction.py raw_data/dpef.html-xfn.nq-all domain_files/xfn_domains.txt domain_dataset/xfn_dataset"
    "python3 domain_extraction_compressed.py raw_data/rdfa domain_files/rdfa_domains.txt domain_dataset/rdfa_dataset"
    "python3 domain_extraction_compressed.py raw_data/hcard domain_files/hcard_domains.txt domain_dataset/hcard_dataset"
    "python3 domain_extraction_compressed.py raw_data/jsonld domain_files/jsonld_domains.txt jsonld_dataset"
    "python3 domain_extraction_compressed.py raw_data/microdata domain_files/microdata_domains.txt microdata_dataset"
)

for cmd in "${python_commands[@]}"
do
    domain_dataset=$(echo $cmd | awk '{print $NF}' | awk -F'/' '{print $NF}' | awk -F'.' '{print $1}')
    case $domain_dataset in
        rdfa_dataset)
            n_tasks=209
            n_nodes=6
            time_limit="01-00:00:00"
            ;;
        hcard_dataset)
            n_tasks=828
            n_nodes=21
            time_limit="02-00:00:00"
            ;;
        microdata_dataset)
            n_tasks=7410
            n_nodes=186
            time_limit="08-00:00:00"
            ;;
        jsonld_dataset)
            n_tasks=8897
            n_nodes=223
            time_limit="11-00:00:00"
            ;;
        adr_dataset)
            n_tasks=1
            n_nodes=1
            time_limit="01:00:00"  # 1 hour
            ;;
        xfn_dataset)
            n_tasks=1
            n_nodes=1
            time_limit="07:00:00"  # 7 hours
            ;;
        *)
            n_tasks=1
            n_nodes=1
            time_limit="00:30:00"  # 30 minutes for all others
            ;;
    esac

    echo "Scheduling job for domain dataset: $domain_dataset with time limit: $time_limit, tasks: $n_tasks, nodes: $n_nodes"
    srun -J "$domain_dataset" -n "$n_tasks" -N "$n_nodes" -t "$time_limit" run_script_domain_extraction.sh "$cmd"
done

echo "All jobs have been scheduled."
