#!/bin/bash

#SBATCH -p general
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --mem=64g
#SBATCH -t 12:00:00
#SBATCH --mail-type=all
#SBATCH --mail-user=kieranf@email.unc.edu

cd /work/users/k/i/kieranf/projects/portfolio-optimization
module purge
module load python/3.9.6
source port-venv-1/bin/activate
python pull_stock_data.py
