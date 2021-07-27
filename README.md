# Cistrome data processing pipeline

Scripts for downloading samples from sra, running chips pipeline and transferring to Cistrome DB server.
main script is scheduler. Run this by the schedule.sbatch script.
The system is designed to run on a cluster with the SLURM workload manager. 

## Requires:
NCBI SRA toolkit https://github.com/ncbi/sra-tools
CHIPS (Cistrome DB3 Version) ChIP-seq and chromatin accessibility processing pipeline
Google Cloud SDK https://cloud.google.com/sdk/docs/install


## Installation

## Configuration file:
`config/rc-fas-harvard.conf`

To initialize the environment:
`source /n/home08/cliffmeyer/miniconda3/bin/activate`
`conda activate cistrome_chips`

The pipeline is intended to work on a SLURM server and this file needs to 
be configured:
`schedule.sbatch` 

## How it works

The system is run by a script that runs indefinitely, which is submitted to the SLURM 
with this script:

`sbatch schedule.sbatch`

The `schedule.batch` job submission script
starts the scheduler script `scheduler.py`, which runs indefinitely, 
mostly in the background. This script checks the status of various steps 
at regular intervals, checks resources and submits new jobs when needed.


The scheduler runs several processes:
- polls the Cistrome DB home server for sample processing requests and updates a local queue of these samples.
- downloads raw sample data from from SRA using the SRA tookit and converts files to fastq format.
- sets up and runs the CHIPS Snakemake pipeline for each sample. This is where most of the processing occurs, 
including read mapping, peak calling, quality control assessment, as well as downstream processed such as peak annotation and 
motif enrichment analysis. These CHIPs jobs are run on scavanged cluster resources which may be, and often are terminated, mid-process. 
The scheduler restarts incomplete CHIPs jobs, as many times as needed to get the jobs complete. 
- check CHIPS results for completion against an integrity checklist. This is designed to catch gross errors and missing data.
- transfer processed data to Cistrome DB home server
- transfer processed data to backup server, and if process fail to transfer status report (TODO)
- clean up after completion: delete most files, leaving only record that processing occurred. 
- The process status is saved in a file `dataset{CISTROME_ID|EXTERNAL_ID}_status.json`.


The larger jobs initiated by the scheduler are submitted via SLURM sbatch. 

To check status of jobs on cluster:
`python cluster_stats.py -c config/rc-fas-harvard.conf`

## Note on resource optimization

The Harvard Cannon cluster allocates resources via a 'FairShare' resource usage system. 
In this system requested memory is as important as used CPU time, therefore it is important 
to keep memory down and to ensure the memory allocation does not greatly exceed requirements over the 
course of the entire job. Mapping is a resource intensive step, which can take a long time. To mitigate the 
problem of mapping being interrupted mid-process and having to remap from zero, fastq files are split into 
smaller files. This is leads to smaller and more predictable memory requirements. 

## Note on restarting sample processing from scratch 

Sometimes processing is interrupted in a way that does not allow for recovery. Sometimes these jobs 
need to be restarted from the beginning. After a limited number of tries these samples are given up on.


  

