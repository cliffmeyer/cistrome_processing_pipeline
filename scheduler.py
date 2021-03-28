import sched
import time
import datetime
from functools import wraps
from threading import Thread
import subprocess
import os
import glob
import argparse
import configparser
import re
import requests_from_cistromeDB
import cluster_stats


DEBUG = False

START_HOUR = 11

event_sched = {
               'update_samples_in_local_queue':  { 'start_time': {'hr':START_HOUR,'min':10,'sec':0}, 'interval': 24*60*60 },
               'download_from_sra':              { 'start_time': {'hr':START_HOUR,'min':50,'sec':0}, 'interval': 60*60    },
               'setup_and_run_chips':            { 'start_time': {'hr':START_HOUR,'min':15,'sec':0}, 'interval': 30*60    },
               'check_chips_results':            { 'start_time': {'hr':START_HOUR,'min':50,'sec':0}, 'interval': 60*60    },
               'transfer_to_server':             { 'start_time': {'hr':START_HOUR,'min':35,'sec':0}, 'interval': 60*60    },
               'transfer_to_backup_server':      { 'start_time': {'hr':20,'min':30,'sec':0}, 'interval': 24*60*60    },
               'clean_up_after_completion':      { 'start_time': {'hr':START_HOUR,'min':55,'sec':0}, 'interval': 60*60    },
               'test':                           { 'start_time': {'hr':START_HOUR,'min':22,'sec':0}, 'interval': 10*60    }
}


def replace_multiplier(string):
    multiplier = {'K':2**10,'M':2**20,'G':2**30,'T':2**40,'P':2**50}
    multiplier = {key:str(val) for key,val in multiplier.items()}
    rep_escaped = map(re.escape, multiplier.keys())
    re_mode = re.IGNORECASE
    pattern = re.compile("|".join(rep_escaped), re_mode)
    val = pattern.sub(lambda match: multiplier[match.group(0)], string)
    return val
 

def set_next_hour_minute(hour,minute,second):
    today_str = time.strftime("%a, %d %b %Y", time.localtime())
    print(today_str)
    set_time_str = today_str + " %02d:%02d:%02d" % (hour,minute,second)
    print(set_time_str)
    set_time = time.strptime( set_time_str, "%a, %d %b %Y %H:%M:%S")
    set_time_epoch_secs = time.mktime(set_time) 
    local_time_epoch_secs = time.mktime(time.localtime())
    if local_time_epoch_secs > set_time_epoch_secs:
        set_time_epoch_secs += 60*60*24
    return set_time_epoch_secs


def asynch(func):
    @wraps(func)
    def async_func(*args, **kwargs):
        func_h1 = Thread(target=func, args=args, kwargs=kwargs)
        func_h1.start()
        return func_h1
    return async_func


def schedule(init_time,interval=24*60*60):
    def decorator(func):
        def periodic(scheduler, interval, action, actionargs=()):
            print('periodic')
            priority = 1
            scheduler.enter(interval,priority,periodic, (scheduler,interval,action,actionargs))
            action(*actionargs)

        def first_run(scheduler, init_time, interval, action, actionargs=()):
            print('first')
            priority = 1
            init_time = set_next_hour_minute(init_time['hr'],init_time['min'],init_time['sec'])
            scheduler.enterabs(init_time,priority,periodic, (scheduler,interval,action,actionargs))
            action(*actionargs)

        @wraps(func)
        def wrap(*args, **kwargs):
            scheduler = sched.scheduler(time.time, time.sleep)
            first_run(scheduler, init_time, interval, func)
            scheduler.run()
        return wrap
    return decorator


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


@asynch
@schedule( event_sched['update_samples_in_local_queue']['start_time'],interval=event_sched['update_samples_in_local_queue']['interval'])
def update_samples_in_local_queue():
    configpath = Config.configpath
    sample_queue = requests_from_cistromeDB.SampleQueue(configpath)
    sample_queue.update_local_queue()


def fastq_check(gsmid):
    fastq_path = Config.sys_config['paths']['fastq']
    sample_check_path = os.path.join(fastq_path,f'{gsmid}.check')
    #print(sample_check_path)
    return os.path.exists(sample_check_path)         


def filter_fastq_samples(sample_set):
    filtered_samples = []
    prefixes_excluded = ['SRR','DRR','ERR']
    pattern_exclude = '|'.join([f'^{prefix}' for prefix in prefixes_excluded])
    pattern_exclude = f'{pattern_exclude}'
    for sample in sample_set:
        if re.search(pattern_exclude,sample) == None:
            filtered_samples += [sample]
    return set(filtered_samples)


def get_fastq_sample_number():
    fastq_path = Config.sys_config['paths']['fastq']
    sample_check_path = os.path.join(fastq_path,'*.fastq')
    # count paired end files for same sample as one
    fastq_list = [os.path.split(elem)[1] for elem in glob.glob(sample_check_path)]
    fastq_list = [elem.split('.')[0] for elem in fastq_list]
    fastq_set  = set([elem.split('_')[0] for elem in fastq_list])
    fastq_set = filter_fastq_samples(fastq_set)
    return len(fastq_set)


@asynch
@schedule( event_sched['download_from_sra']['start_time'], interval=event_sched['download_from_sra']['interval'])
def download_from_sra():

    configpath = Config.configpath
    config = Config.sys_config
    partition = config['process_server']['partition']
    cluster_status = cluster_stats.ClusterStats(configpath)
    max_fastq_file_number = int(Config.sys_config['process_server']['max_fastq_file_number'])
    max_fails = int(Config.sys_config['process_server']['max_fails'])
    max_restarts = int(Config.sys_config['process_server']['max_restarts'])

    # TODO scratch = cluster_status.get_scratch_use()
    # check disk space availability
    #if (scratch['quota'] - scratch['used']) < float(config['process_server']['min_disk_space_avail']):
    #    return

    sample_queue = requests_from_cistromeDB.SampleQueue(configpath)
    sample_queue.read_local_queue() 
    samples_to_process = sample_queue.get_local_queue()

    fp = open('schedule_sra_log.txt','a')

    for gsmid,sample_info in samples_to_process.items():

        # don't download if there are already enough files to process
        if get_fastq_sample_number() > max_fastq_file_number:
            print('too many fastq files', get_fastq_sample_number(), max_fastq_file_number, file=fp)
            break

        print(gsmid,file=fp)

        log_path = get_sra_log_path(gsmid)

        # check results have not been sent back already
        if transfer_complete_check(gsmid) == True:
            continue
 
        # check fastq check-file does not exist
        if fastq_check(gsmid) == True:
            continue

        # limit number of restarts
        if sample_queue.get_sample_restart_count(sample_id=gsmid) >= max_restarts:
            continue

        # limit number of download tries
        if sample_queue.get_sample_status_count(sample_id=gsmid,info_key='SRA') > max_fails:
            continue

        # check number of jobs pending
        cluster_status.get_jobs_in_queue()
        if (cluster_status.get_pending_job_count() > int(Config.sys_config['process_server']['max_jobs_pending'])):
            print(gsmid,'too many jobs pending', cluster_status.get_pending_job_count(), int(Config.sys_config['process_server']['max_jobs_pending']), file=fp)
            break

        # check number of jobs running
        if (cluster_status.get_running_job_count() > int(Config.sys_config['process_server']['max_jobs_running'])):
            print(gsmid,'too many jobs running', cluster_status.get_running_job_count(), int(Config.sys_config['process_server']['max_jobs_running']), file=fp)
            break

        # check job is not already in queue
        jobname = f'{gsmid}_sra'
        if cluster_status.is_job_name_in_queue(f'{gsmid}_sra') == True:  # SRA download job name: {ID}_sra
            continue       

        cmd = f'python sra_download.py -c {configpath} -i {gsmid}'

        sbatch_path = os.path.join( Config.sys_config['paths']['data_collection_sbatch'], f'{jobname}.sbatch')

        if DEBUG:
            sbatch_cmd = f'python sbatch_header.py --cmd "{cmd}" --time 300 --mem 2000 --partition {partition} --jobname {jobname} --sbatchfile {sbatch_path} --log {log_path}'
        else:
            sbatch_cmd = f'python sbatch_header.py --cmd "{cmd}" --time 300 --mem 2000 --partition {partition} --jobname {jobname} --sbatchfile {sbatch_path} --log {log_path} --submit'

        subprocess.run(sbatch_cmd,shell=True)
        time.sleep(1)
        print(datetime.datetime.now(),file=fp)
    fp.close()


def chips_complete_check(gsmid):
    chips_run_path = Config.sys_config['paths']['data_collection_runs']
    sample_check_path = os.path.join(chips_run_path,f'{gsmid}/analysis/logs/empty_file_list.txt')
    return os.path.exists(sample_check_path) 


def chips_check_complete_check(gsmid):
    chips_run_path = Config.sys_config['paths']['data_collection_runs']
    sample_check_path = os.path.join(chips_run_path,f'{gsmid}/cistrome/{gsmid}.md5')
    return os.path.exists(sample_check_path) 


def transfer_complete_check(gsmid):
    chips_run_path = Config.sys_config['paths']['data_collection_runs']
    sample_check_path = os.path.join(chips_run_path,f'{gsmid}/{gsmid}_rsync_ok.txt')
    return os.path.exists(sample_check_path) 


def transfer_to_backup_complete_check(gsmid):
    chips_run_path = Config.sys_config['paths']['data_collection_runs']
    sample_check_path = os.path.join(chips_run_path,f'{gsmid}/{gsmid}_backup_ok.txt')
    return os.path.exists(sample_check_path) 


def update_cluster_runstats_in_local_queue():
    configpath = Config.configpath
    sample_queue = requests_from_cistromeDB.SampleQueue(configpath)
    sample_queue.read_local_queue()

    cluster_status = cluster_stats.ClusterStats(configpath)
    cluster_status.get_account_info()

    # update record of sbatch jobs
    job_status = match_sbatch_history( suffix='sra', jobs_name=cluster_status.account_info['name'], jobs_status=cluster_status.account_info['status'], jobs_id=cluster_status.account_info['job_id'])
    for sampleid,job_type_status in job_status.items():
        sample_queue.set_sample_info( sample_id=sampleid, info_key=job_type_status['type'], info_val=job_type_status['status'])

    job_status = match_sbatch_history( suffix='chips', jobs_name=cluster_status.account_info['name'], jobs_status=cluster_status.account_info['status'], jobs_id=cluster_status.account_info['job_id'])
    for sampleid,job_type_status in job_status.items():
        sample_queue.set_sample_info( sample_id=sampleid, info_key=job_type_status['type'], info_val=job_type_status['status'])

    job_status = match_sbatch_history( suffix='chips_check', jobs_name=cluster_status.account_info['name'], jobs_status=cluster_status.account_info['status'], jobs_id=cluster_status.account_info['job_id'])
    for sampleid,job_type_status in job_status.items():
        sample_queue.set_sample_info( sample_id=sampleid, info_key=job_type_status['type'], info_val=job_type_status['status'])

    sample_queue.write_local_queue() 


def clean_up_failed_samples():
    configpath = Config.configpath
    partition = Config.sys_config['process_server']['partition']
    max_fails = int(Config.sys_config['process_server']['max_fails'])
    cluster_status = cluster_stats.ClusterStats(configpath)

    sample_queue = requests_from_cistromeDB.SampleQueue(configpath)
    sample_queue.read_local_queue() 
    samples_to_process = sample_queue.get_local_queue()
 
    for gsmid,sample_info in samples_to_process.items(): 
        if (sample_queue.get_sample_fail_count(sample_id=gsmid,info_key='SRA') >= max_fails or
            sample_queue.get_sample_fail_count(sample_id=gsmid,info_key='CHIPS') >= max_fails or
            sample_queue.get_sample_status_count(sample_id=gsmid,info_key='CHIPS_CHECK') >= max_fails):
            # NOTE: testing to see how many time chips_check has run rather than has failed
            # if it is running many times there is a problem with the sample 
            # sample_queue.get_sample_fail_count(sample_id=gsmid,info_key='CHIPS_CHECK') >= max_fails):

            print(f'cleaning up failed sample {gsmid}') 
            sample_queue.clear_sample_info(sample_id=gsmid,info_key='SRA')
            sample_queue.clear_sample_info(sample_id=gsmid,info_key='CHIPS')
            sample_queue.clear_sample_info(sample_id=gsmid,info_key='CHIPS_CHECK')
            delete_sbatch_files(gsmid)
            delete_sra_files(gsmid)
            delete_fastq_files(gsmid)
            delete_result_files(gsmid,complete=False)
            sample_queue.increment_sample_restart_count(sample_id=gsmid)

    sample_queue.write_local_queue()
    return


@asynch
@schedule( event_sched['setup_and_run_chips']['start_time'], interval=event_sched['setup_and_run_chips']['interval'])
def setup_and_run_chips():

    fp = open('schedule_chips_log.txt','a')
    print('job stat update running:',datetime.datetime.now(),file=fp)
    update_cluster_runstats_in_local_queue()
    print('clean up failed samples:',datetime.datetime.now(),file=fp)
    clean_up_failed_samples()

    print('chips job submission running:',datetime.datetime.now(),file=fp)
    # processing differs between sample types
    sampletype_lookup = {'dnase':'dnase', 'atac':'atac', 'tf':'tf', 'h3k27ac':'h3k27ac', 'h3k4me3':'h3k4me3' } 

    max_jobs_pending = int(Config.sys_config['process_server']['max_jobs_pending'])
    max_restarts = int(Config.sys_config['process_server']['max_restarts'])
    configpath = Config.configpath
    partition = Config.sys_config['process_server']['partition']
    cluster_status = cluster_stats.ClusterStats(configpath)

    sample_queue = requests_from_cistromeDB.SampleQueue(configpath)
    sample_queue.read_local_queue() 
    samples_to_process = sample_queue.get_local_queue()
 
    # TODO confirm consistency between words used to specify chips and types in sample request file
    for gsmid,sample_info in samples_to_process.items():
 
        #print('chips loop',gsmid,file=fp)
        if sample_queue.get_sample_restart_count(sample_id=gsmid) >= max_restarts:
            continue

        # check fastq check-file exists
        if fastq_check(gsmid) == False:
            continue

        # check chips run is not complete 
        if chips_complete_check(gsmid) == True:
            continue

        # check results have not been sent back already
        if transfer_complete_check(gsmid) == True:
            continue

        # check number of jobs in queue
        cluster_status.get_jobs_in_queue()
        if cluster_status.get_pending_job_count() > max_jobs_pending:
            print(gsmid,'too many jobs pending: break chips submission',file=fp) 
            break

        # check job is not already in queue
        if cluster_status.is_job_name_in_queue(f'{gsmid}_chips') == True:  # chips job name: {ID}_chips
            continue            
 
        species = sample_info['species']
        sampletype = sample_info['sampletype']
        if sample_info['broad'].lower() == 'true':
            broad = '--broad'
        else:
            broad = ''

        # Improve place of Lookup
        sampletype = sampletype_lookup[sampletype.lower()]
        cmd = f'python chips_job_submission.py -c {configpath} --gsm {gsmid} --species {species} --sampletype {sampletype} {broad} --submit'
        if DEBUG:
            print(gsmid)
            print(sample_info)
            print(cmd)
        else:
            subprocess.run(cmd,shell=True)
            print(gsmid,datetime.datetime.now(),file=fp)
            time.sleep(1)

    fp.close()


@asynch
@schedule( event_sched['check_chips_results']['start_time'], interval=event_sched['check_chips_results']['interval'])
def check_chips_results():

    configpath     = Config.configpath
    partition      = Config.sys_config['process_server']['partition']
    cluster_status = cluster_stats.ClusterStats(configpath)

    sample_queue = requests_from_cistromeDB.SampleQueue(configpath)
    sample_queue.read_local_queue() 
    samples_to_process = sample_queue.get_local_queue()

    for gsmid,sample_info in samples_to_process.items():

        sample_path = os.path.join( Config.sys_config['paths']['data_collection_runs'], gsmid  )
        log_path    = os.path.join( sample_path, f'chips_check_log_{gsmid}.txt' )
        chips_yaml  = os.path.join( sample_path,'config.yaml')
        jobname     = f'{gsmid}_chips_check'
        sbatch_path = os.path.join( Config.sys_config['paths']['data_collection_sbatch'], f'{jobname}.sbatch')

        print(gsmid)
        # check results have not been sent back already
        if transfer_complete_check(gsmid) == True:
            continue

        # check chips run is complete 
        if chips_complete_check(gsmid) == False:
            continue

        # check chips run has not been checked already 
        if chips_check_complete_check(gsmid) == True:
            continue

        # check number of jobs in queue
        cluster_status.get_jobs_in_queue()
        if (cluster_status.get_pending_job_count() > int(Config.sys_config['process_server']['max_jobs_pending'])):
            break

        # check job is not already in queue
        if cluster_status.is_job_name_in_queue(jobname) == True:  # chips check job name: {ID}_chips_check
            continue            

        cmd = f'python check_chips.py -c {configpath} -i {gsmid}'

        if DEBUG:
            sbatch_cmd = f'python sbatch_header.py --cmd "{cmd}" --time 480 --mem 2000 --partition {partition} --jobname {jobname} --sbatchfile {sbatch_path} --log {log_path}'
        else:
            sbatch_cmd = f'python sbatch_header.py --cmd "{cmd}" --time 480 --mem 2000 --partition {partition} --jobname {jobname} --sbatchfile {sbatch_path} --log {log_path} --submit'

        subprocess.run(sbatch_cmd,shell=True)
        time.sleep(1)
        print(datetime.datetime.now())

    return 


# using transfer_to_data instead of transfer_to_home
#@asynch
#@schedule( event_sched['transfer_to_home_server']['start_time'], interval=event_sched['transfer_to_home_server']['interval'])
#def transfer_to_home_server():
#
#    configpath   = Config.configpath
#    sample_queue = requests_from_cistromeDB.SampleQueue(configpath)
#    sample_queue.read_local_queue() 
#    samples_to_process = sample_queue.get_local_queue()
#
#    for gsmid,sample_info in samples_to_process.items():
#        if chips_check_complete_check(gsmid) == True and transfer_complete_check(gsmid) == False:
#            sample_path        = os.path.join( Config.sys_config['paths']['data_collection_runs'], gsmid  )
#            rsync_ok           = os.path.join( sample_path,f'{gsmid}_rsync_ok.txt' )
#            cistrome_path      = os.path.join( sample_path, Config.sys_config['paths']['cistrome_result'] )
#            home_server_path   = Config.sys_config['home_server']['home_server_path']
#            home_server_user   = Config.sys_config['home_server']['home_server_user']
#            home_server_domain = Config.sys_config['home_server']['home_server_domain']
#            home_server_port   = Config.sys_config['home_server']['home_server_port']
#            transfer_cmd  = f'rsync -aPL -e "ssh -p {home_server_port}" {cistrome_path} {home_server_user}@{home_server_domain}:{home_server_path} && touch {rsync_ok}'
#            fp_stdout = open(os.path.join(sample_path,'rsync_stdout.txt'),'w')
#            fp_stderr = open(os.path.join(sample_path,'rsync_stderr.txt'),'w')
#            if DEBUG == False:
#                subprocess.call(transfer_cmd,shell=True,stdout=fp_stdout,stderr=fp_stderr)
#                time.sleep(1)
#            else:
#                print(transfer_cmd)
#                pass
#            fp_stdout.close()
#            fp_stderr.close()
#
#    return 



@asynch
@schedule( event_sched['transfer_to_server']['start_time'], interval=event_sched['transfer_to_server']['interval'])
def transfer_to_server():

    configpath = Config.configpath
    server = 'home_server'
    cluster_status = cluster_stats.ClusterStats(configpath)
    sample_queue = requests_from_cistromeDB.SampleQueue(configpath)
    sample_queue.read_local_queue() 
    samples_to_process = sample_queue.get_local_queue()
    max_data_rsync   = int(Config.sys_config['process_server']['max_jobs_rsync_data'])
    max_jobs_running = int(Config.sys_config['process_server']['max_jobs_running'])
    max_jobs_pending = int(Config.sys_config['process_server']['max_jobs_pending'])

    partition = Config.sys_config['process_server']['partition']

    fp = open('schedule_rsync_data_log.txt','a')
    print('rsync data running:',datetime.datetime.now(),file=fp)
 
    for gsmid,sample_info in samples_to_process.items():
        if chips_check_complete_check(gsmid) == True and transfer_complete_check(gsmid) == False:

            # check number of jobs in queue
            cluster_status.get_jobs_in_queue() 
            if cluster_status.get_pending_job_count() >= max_jobs_pending: 
                break

            if cluster_status.get_running_job_count() >= max_jobs_running:
                break

            n_rsync_jobs = len([jobname for jobname in cluster_status.list_job_names_in_queue() if '_data_rsync' in jobname])
            if n_rsync_jobs >= max_data_rsync:
                break

            jobname = f'{gsmid}_data_rsync'
            if cluster_status.is_job_name_in_queue(jobname) == True:
                continue 

            sample_path = os.path.join(Config.sys_config['paths']['data_collection_runs'], gsmid)
            log_path = os.path.join( sample_path, f'data_rsync_log_{gsmid}.txt' )
            sbatch_path = os.path.join( Config.sys_config['paths']['data_collection_sbatch'], f'{jobname}.sbatch')
            cmd = f'python file_transfer_to_server.py -c {configpath} -i {gsmid} -s {server}'

            if not DEBUG:
                print(f'rsync {gsmid}:',datetime.datetime.now(),file=fp)
                sbatch_cmd = f'python sbatch_header.py --cmd "{cmd}" --time 3600 --mem 1000 --partition {partition} --jobname {jobname} --sbatchfile {sbatch_path} --log {log_path} --submit'
                subprocess.run(sbatch_cmd,shell=True)
            else:
                print(cmd)

            time.sleep(10) 
            print(datetime.datetime.now())
            fp.flush()

    fp.close()
    return 



@asynch
@schedule( event_sched['transfer_to_backup_server']['start_time'], interval=event_sched['transfer_to_backup_server']['interval'])
def transfer_to_backup_server():

    configpath = Config.configpath
    server = 'backup_server'
    cluster_status = cluster_stats.ClusterStats(configpath)
    sample_queue = requests_from_cistromeDB.SampleQueue(configpath)
    sample_queue.read_local_queue() 
    samples_to_process = sample_queue.get_local_queue()
    max_backup_rsync = int(Config.sys_config['process_server']['max_jobs_rsync_backup'])
    max_jobs_running = int(Config.sys_config['process_server']['max_jobs_running'])
    max_jobs_pending = int(Config.sys_config['process_server']['max_jobs_pending'])

    partition = Config.sys_config['process_server']['partition']
 
    n_fails = 0 # keep track of failures

    fp = open('schedule_rsync_backup_log.txt','a')
    print('rsync backup running:',datetime.datetime.now(),file=fp)
 
    for gsmid,sample_info in samples_to_process.items():
        if chips_check_complete_check(gsmid) == True and transfer_to_backup_complete_check(gsmid) == False:

            # check number of jobs in queue
            cluster_status.get_jobs_in_queue() 
            if cluster_status.get_pending_job_count() >= max_jobs_pending: 
                break

            if cluster_status.get_running_job_count() >= max_jobs_running:
                break

            # TODO check how more than max_backup_rsync jobs can run (slurm failed to report jobs?)
            n_backup_rsync_jobs = len([jobname for jobname in cluster_status.list_job_names_in_queue() if '_backup_rsync' in jobname])
            if n_backup_rsync_jobs >= max_backup_rsync:
                break

            jobname = f'{gsmid}_backup_rsync'
            if cluster_status.is_job_name_in_queue(jobname) == True:
                continue 

            sample_path = os.path.join(Config.sys_config['paths']['data_collection_runs'], gsmid)
            log_path = os.path.join( sample_path, f'backup_rsync_log_{gsmid}.txt' )
            sbatch_path = os.path.join( Config.sys_config['paths']['data_collection_sbatch'], f'{jobname}.sbatch')
            cmd = f'python file_transfer_to_server.py -c {configpath} -i {gsmid} -s {server}'

            if not DEBUG:
                print(f'rsync {gsmid}:',datetime.datetime.now(),file=fp)
                #subprocess.check_call(cmd,shell=True)
                #print(f'rsync {gsmid} complete:',datetime.datetime.now(),file=fp)
                sbatch_cmd = f'python sbatch_header.py --cmd "{cmd}" --time 3600 --mem 1000 --partition {partition} --jobname {jobname} --sbatchfile {sbatch_path} --log {log_path} --submit'
                subprocess.run(sbatch_cmd,shell=True)

            # track failure to backup
            #if transfer_to_backup_complete_check(gsmid) == False:
            #    n_fails += 1
            #    print(f'rsync {gsmid} failure {n_fails}:',datetime.datetime.now(),file=fp)
            # reduce fail count on success 
            #elif n_fails > 0:
            #    n_fails -= 1
            #if n_fails >= MAX_BACKUP_FAILURES:
            #    break

            time.sleep(10) 
            print(datetime.datetime.now())
            fp.flush()

    fp.close()
    return 


def get_sra_log_path(gsmid):
    sra_log_path = os.path.join( Config.sys_config['paths']['sra'], f'sra_log_{gsmid}.txt' )
    return sra_log_path


def sra_paths_from_sra_log(gsmid):
    sra_path_list = []
    sra_log_path = get_sra_log_path(gsmid)
    print(sra_log_path)
 
    if os.path.exists(sra_log_path) == False:
        return sra_path_list

    with open(sra_log_path,'r') as fp:
        for line in fp.readlines():
            if 'sra file OK:' in line:
                sra_path_list += [line.split()[-1]]
    return sra_path_list


def delete_sra_files(gsmid):
    # delete SRA files
    sra_path_list = sra_paths_from_sra_log(gsmid)
    sra_path_string = ' '.join(sra_path_list)
    if len(sra_path_string) > 0 and os.path.exists(sra_path_string):
        delete_sra_cmd = f'rm {sra_path_string}'

        if DEBUG == False:
            subprocess.call(delete_sra_cmd,shell=True)
            time.sleep(1)
        else:
            print(delete_sra_cmd)
            pass


def delete_fastq_files(gsmid):
    fastq_path = Config.sys_config['paths']['fastq']

    if os.path.exists( os.path.join( fastq_path, f'{gsmid}.fastq' )):
        fastq_file_path_list  = [os.path.join( fastq_path, f'{gsmid}.fastq' )]  
    elif os.path.exists( os.path.join( fastq_path, f'{gsmid}_R1.fastq' )):
        fastq_file_path_list  = [os.path.join( fastq_path, f'{gsmid}_R1.fastq' )]
        fastq_file_path_list += [os.path.join( fastq_path, f'{gsmid}_R2.fastq' )]
    else:
        fastq_file_path_list = []

    fastq_file_string = ' '.join(fastq_file_path_list)

    if os.path.exists( os.path.join( fastq_path, f'{gsmid}.check' )):
        fastq_check_path  = os.path.join( fastq_path, f'{gsmid}.check' )
    else:
        fastq_check_path = ''

    if len(fastq_file_string) > 0 or len(fastq_check_path) > 0:
        delete_cmd = f'rm {fastq_file_string} {fastq_check_path}'

        if DEBUG == False:
            subprocess.call(delete_cmd,shell=True)
            time.sleep(1)
        else:
            print(delete_cmd)
            pass


def delete_sbatch_files(gsmid):
    jobname_list = [f'{gsmid}_sra',f'{gsmid}_chips',f'{gsmid}_chips_check']
    sbatch_path_list = [os.path.join( Config.sys_config['paths']['data_collection_sbatch'], f'{elem}.sbatch') for elem in jobname_list]
    sbatch_path_string = ' '.join( [ elem for elem in sbatch_path_list if os.path.exists(elem)] )
    #sbatch_path_string = ' '.join(sbatch_path_list)

    if len(sbatch_path_string) > 0: 
        delete_sbatch_cmd = f'rm {sbatch_path_string}'

        if DEBUG == False:
            subprocess.call(delete_sbatch_cmd,shell=True)
            time.sleep(1)
        else:
            print(delete_sbatch_cmd)
            pass


def delete_result_files(gsmid,complete=False):
    sample_path = os.path.join( Config.sys_config['paths']['data_collection_runs'], gsmid)

    if os.path.exists(sample_path):
        files_in_sample_path = os.path.join( sample_path, '*' )
        rsync_ok    = os.path.join( sample_path,f'{gsmid}_rsync_ok.txt' )

        # TODO 
        if complete == True: 
            delete_cmd  = f'rm -rf {files_in_sample_path} && touch {rsync_ok}'
        else:            
            delete_cmd  = f'rm -rf {files_in_sample_path}'

        if DEBUG == False:
            subprocess.call(delete_cmd,shell=True)
            time.sleep(1)
        else:
            print(delete_cmd)
            pass


@asynch
@schedule( event_sched['clean_up_after_completion']['start_time'], interval=event_sched['clean_up_after_completion']['interval'])
def clean_up_after_completion():

    configpath   = Config.configpath
    sample_queue = requests_from_cistromeDB.SampleQueue(configpath)
    sample_queue.read_local_queue() 
    samples_to_process = sample_queue.get_local_queue()

    for gsmid,sample_info in samples_to_process.items():

        sample_path   = os.path.join( Config.sys_config['paths']['data_collection_runs'], gsmid  )
        cistrome_path = os.path.join( sample_path, Config.sys_config['paths']['cistrome_result'] ) 

        if gsmid == '':
            gsmid = 'missing_id_do_not_delete_the_path'

        # check transfer is complete and results have not yet been deleted
        if (transfer_complete_check(gsmid) == True and 
            transfer_to_backup_complete_check(gsmid) == True and 
            os.path.exists(cistrome_path) == True):

            delete_sra_files(gsmid)
            delete_fastq_files(gsmid)
            delete_sbatch_files(gsmid)
            delete_result_files(gsmid,complete=True)

    return 


class Config():
    configpath = ''
    sys_config = None

    def __init__(self,configpath):
        Config.sys_config = configparser.ConfigParser()
        Config.sys_config.optionxform=str
        Config.sys_config.read(configpath)
        Config.configpath = configpath


@asynch
@schedule( event_sched['test'], interval=60*60)
def test():
    configpath = Config.configpath
    with open('test_schedule_check.txt','a') as fp:
        print(datetime.datetime.now(),file=fp)
    return 


def main(configpath):
    Config(configpath)
    update_samples_in_local_queue()
    download_from_sra() 
    setup_and_run_chips()
    check_chips_results()
    transfer_to_server() 
    transfer_to_backup_server() 
    clean_up_after_completion()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="""Run Cistrome DB data processing""")
    parser.add_argument( '-c', dest='configpath', type=str, required=True, help='the path of config file')
    args = parser.parse_args()
    main(args.configpath)

