import os
import pandas as pd
import matplotlib.pyplot as plt
import random
from pathlib import Path
from tqdm import tqdm  # For the progress bar
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Base directory containing subdirectories
base_dir = "D:\DICE\Code\WHALE\WDC_scripts\embeddings"

# Subdirectories to explore
root_dirs = ['adr', 'geo', 'hcalendar', 'hlisting', 'hrecipe', 'hresume', 'hreview', 'rdfa', 'species', 'xfn']

# Function to extract dataset name from sub-directory name
def get_dataset_name(sub_dir_name):
    return sub_dir_name.split("Keci_GPU_")[1].rsplit(".txt", 1)[0]

# Collect all `Keci_GPU_*` subdirectories
logging.info("Collecting all subdirectories...")
all_subdirs = []
for root_dir in root_dirs:
    models_dir = os.path.join(base_dir, root_dir, "models")
    if os.path.exists(models_dir):
        subdirs = [os.path.join(models_dir, sub_dir) for sub_dir in os.listdir(models_dir) if sub_dir.startswith("Keci_GPU")]
        all_subdirs.extend(subdirs)

# Calculate 1% of total subdirectories
sample_size = max(1, int(0.01 * len(all_subdirs)))  # Ensure at least one sample
random.shuffle(all_subdirs)  # Shuffle to randomly sample
sampled_subdirs = []

logging.info(f"Sampling 0.1% of total subdirectories ({sample_size} samples)...")

# Ensure sampled subdirectories have an `epoch_losses.csv`
while len(sampled_subdirs) < sample_size:
    subdir = all_subdirs.pop()
    if "epoch_losses.csv" in os.listdir(subdir):
        sampled_subdirs.append(subdir)

logging.info(f"Total sampled subdirectories with valid `epoch_losses.csv`: {len(sampled_subdirs)}")

# Process sampled subdirectories and generate plots
logging.info("Starting plot generation...")
for subdir in tqdm(sampled_subdirs, desc="Generating plots"):
    epoch_loss_file = os.path.join(subdir, 'epoch_losses.csv')
    dataset_name = get_dataset_name(os.path.basename(subdir))
    root_dir = Path(subdir).parents[2]  # Get root directory (e.g., adr, geo)
    plots_dir = os.path.join(root_dir, "plots")
    os.makedirs(plots_dir, exist_ok=True)  # Create plots directory if not exists
    
    # Load epoch losses
    epoch_losses = pd.read_csv(epoch_loss_file)['EpochLoss']
    relative_loss = epoch_losses.iloc[-1] / epoch_losses.iloc[0]
    
    # Plotting for the dataset without a title
    plt.figure(figsize=(6, 4))
    plt.bar([dataset_name], [relative_loss], color='blue')
    plt.ylabel('Relative Loss (Epoch 100 / Epoch 1)')

    # Dynamically adjust text position above the bar
    plt.text(0, relative_loss + (0.01 * relative_loss), f"{relative_loss:.4f}", ha='center', fontsize=10)

    # Adjust layout
    plt.tight_layout()


    # Save the plot
    plot_path = os.path.join(plots_dir, f"{dataset_name}_relative_loss.png")
    plt.savefig(plot_path)
    plt.close()
    logging.info(f"Plot saved for {dataset_name} at {plot_path}")

logging.info("All plots generated successfully!")
