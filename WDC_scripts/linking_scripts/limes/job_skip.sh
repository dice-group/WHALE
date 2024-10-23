#!/bin/bash
#SBATCH --job-name=xfn_AML_s
#SBATCH --time=21-00:00:00
#SBATCH --mem=32G
#SBATCH -p normal
#SBATCH --ntasks=1
#SBATCH --output=job_output_s.log
#SBATCH --error=job_error_s.log

./AML_match_skip_configs.sh
