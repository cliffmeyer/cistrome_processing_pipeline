#!/bin/bash
#SBATCH --job-name=schedule_q
#SBATCH --nodes 1
#SBATCH -n 1
#SBATCH --time 0-18:10
#SBATCH --mem=300MB
#SBATCH --partition unrestricted
#SBATCH -o schedule_tmp.log
source /n/home08/cliffmeyer/miniconda3/bin/activate
conda activate cistrome_chips
module load gcc
module load R
module load texlive
python /n/home08/cliffmeyer/projects/cistrome_processing_pipeline/scheduler.py -c config/rc-fas-harvard.conf
