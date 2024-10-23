#!/bin/bash
#SBATCH --job-name=xfn_AML
#SBATCH --time=21-00:00:00
#SBATCH --mem=32G
#SBATCH -p normal
#SBATCH --output=job_output_%A_%a.log
#SBATCH --error=job_error_%A_%a.log
#SBATCH --mail-type=ALL
#SBATCH --mail-user=akhomich@mail.uni-paderborn.de

./AML_match.sh
