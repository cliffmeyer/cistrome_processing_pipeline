# Cistrome data processing pipeline

Scripts for downloading samples from sra, running chips pipeline and transferring to Cistrome DB server.
main script is scheduler. Run this by the schedule.sbatch script.

To initialize the environment:
`source /n/xiaoleliu_lab/chips/miniconda3/bin/activate`
`conda activate chips`

To submit the scheduler to the SLURM:
`sbatch schedule.sbatch`

To check status of jobs on cluster:
`python cluster_stats.py -c config/rc-fas-harvard.conf`

Configuration file:
`config/rc-fas-harvard.conf`


## Scheduler
`schedule.batch` runs `scheduler.py`

The scheduler runs several processes:
- updates sampless in local queue
- download raw sample data from from sra
- setup and run CHIPS Snakemake pipeline for each sample
- check CHIPS results for completion and integrity checklist 
- transfer to server
- transfer_to_backup_server
- clean_up_after_completion

