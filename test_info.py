import sched
import time
import datetime
from functools import wraps
from threading import Thread
import subprocess
import os
import argparse
import configparser
import re
import requests_from_cistromeDB
import cluster_stats
import json


def match_sbatch_history(suffix='',jobs_name=[],jobs_status=[],jobs_id=[]):
    pattern    = re.compile(f'([a-zA-Z0-9]+)_{suffix}\Z')
    job_status = {}

    for i,jobname in enumerate(jobs_name):
        status = jobs_status[i]
        job_id = jobs_id[i]
        match  = pattern.match(jobname)
        if not isinstance(match,type(None)):
            job_type = suffix
            sampleid = match.group(1)
            #job_status[sampleid] = {'type':job_type,'status':status}
            job_status[sampleid] = {'type':job_type, 'status':{job_id:status}}

    return job_status


def update_samples_in_local_queue():
    configpath = Config.configpath
    sample_queue = requests_from_cistromeDB.SampleQueue(configpath)
    sample_queue.update_local_queue()


def update_cluster_runstats_in_local_queue():
    configpath = Config.configpath
    sample_queue = requests_from_cistromeDB.SampleQueue(configpath)
    sample_queue.read_local_queue()

    cluster_status = cluster_stats.ClusterStats(configpath)
    cluster_status.get_account_info()

    print(cluster_status.account_info)
    # update record of sbatch jobs

    job_status = match_sbatch_history( suffix='chips', jobs_name=cluster_status.account_info['name'], jobs_status=cluster_status.account_info['status'], jobs_id=cluster_status.account_info['job_id'])

    print(job_status)
    for sampleid,job_type_status in job_status.items():
        #sample_queue.set_sample_info( sample_id=sampleid, info_key=job_type_status['type'], info_val=job_type_status['status'])
        sample_queue.set_sample_info( sample_id=sampleid, info_key=job_type_status['type'], info_val=job_type_status['status'])

    job_status = match_sbatch_history( suffix='chips_check', jobs_name=cluster_status.account_info['name'], jobs_status=cluster_status.account_info['status'], jobs_id=cluster_status.account_info['job_id'])
    for sampleid,job_type_status in job_status.items():
        #sample_queue.set_sample_info( sample_id=sampleid, info_key=job_type_status['type'], info_val=job_type_status['status'])
        sample_queue.set_sample_info( sample_id=sampleid, info_key=job_type_status['type'], info_val=job_type_status['status'])

    print(sample_queue)
    with open('test.json','w') as fp:
        json.dump(sample_queue.local_samples,fp)

    print(sample_queue.get_sample_fail_count(sample_id=sampleid,info_key='chips'))


class Config():
    configpath = ''
    sys_config = None

    def __init__(self,configpath):
        Config.sys_config = configparser.ConfigParser()
        Config.sys_config.optionxform=str
        Config.sys_config.read(configpath)
        Config.configpath = configpath



def main(configpath):
    Config(configpath)
    update_cluster_runstats_in_local_queue()


if __name__ == '__main__':
    main('./config/rc-fas-harvard.conf')
 
