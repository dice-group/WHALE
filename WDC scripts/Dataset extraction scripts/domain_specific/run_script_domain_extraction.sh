#!/bin/sh

#SBATCH -q express
#SBATCH -p normal
#SBATCH --cpus-per-task=1
#SBATCH --mem=180000M
#SBATCH -A hpc-prf-dsg
#SBATCH -o %x-%j.log
#SBATCH -e %x-%j.log

. $HOME/.bashrc
conda activate dice

export cmd="$1"
echo "Running command: $cmd"
$cmd
conda deactivate
exit 0
~