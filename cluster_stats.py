import argparse
import configparser
import subprocess
import re
import math


def read_config(configpath):
    config = configparser.ConfigParser()
    config.optionxform=str
    config.read(configpath)
    return(config)


def replace_multiplier(string):
    multiplier = {'':1,'K':2**10,'M':2**20,'G':2**30,'T':2**40,'P':2**50}
    rep_escaped = map(re.escape, multiplier.keys())
    re_mode = re.IGNORECASE
    format_str = "(\d*\.?\d+)" + "([" + "|".join(rep_escaped) + "]*)"
    pattern = re.compile(format_str, re_mode)
    match = pattern.match(string)
    if match != None and match.lastindex == 2:
        val = float(match.group(1)) * multiplier[match.group(2)]
    elif match != None and match.lastindex == 1:
        val = float(match.group(1))
    else:
        val = math.nan
    return val

 
class ClusterStats():


    def __init__(self,configpath):
        config = read_config(configpath)
        self.cluster_account   = config['process_server']['cluster_account']
        self.cluster_scratch   = config['process_server']['cluster_scratch']
        self.cluster_partition = config['process_server']['partition']
        self.jobs_in_queue = {'partition':[],'status':[],'name':[],'time':[],'memory':[]} 
        self.account_info  = {'partition':[],'status':[],'name':[],'exit_code':[]} 


    def __str__(self):

        jobs = self.account_info
        s = '\t'.join(['partition','status','name','exit_code'])
        s += '\n'
        for i,elem in enumerate(jobs['partition']):
            s += '\t'.join([jobs['partition'][i], jobs['status'][i], jobs['name'][i], jobs['exit_code'][i]])
            s += '\n'

        s += '======================= Jobs in queue ====================\n'
        jobs = self.jobs_in_queue
        s += '\t'.join(['partition','status','name','time','memory','nodelist'])
        s += '\n'
        for i,elem in enumerate(jobs['partition']):
            s += '\t'.join([jobs['partition'][i], jobs['status'][i], jobs['name'][i], jobs['time'][i], '%4.2e' % jobs['memory'][i], jobs['nodelist'][i]])
            s += '\n'
        return s


    def get_scratch_use(self):
        print(self.cluster_scratch)
        cmd = f'lfs quota -hg {self.cluster_account} {self.cluster_scratch}'
        print(cmd)
        ret = subprocess.run(cmd,shell=True,capture_output=True)
        print(ret)
        try:
            rstring = str(ret.stdout, 'utf-8')
            print(rstring)
            rlist = rstring.splitlines()
            keys = rlist[1].strip().split()
            keys = keys[1:5]
            vals = rlist[3].strip().split()
            vals = vals[0:4]
            vals = [replace_multiplier(elem) for elem in vals]
            stat_dict = dict(zip(keys,vals))
        except: # TODO figure out this error
            print('lfs error')
            stat_dict = {}
        return stat_dict


    def get_fairshare(self):
        cmd = f'sshare --account={self.cluster_account}'
        ret = subprocess.run(cmd,shell=True,capture_output=True)
        rstring = str(ret.stdout, 'utf-8')
        print(rstring)
        rlist = rstring.splitlines()
        if len(rlist) > 1:
            keys = rlist[0].strip().split()
            key_lookup = {key:i for i,key in enumerate(keys)}
            info = rlist[-1].strip().split()
            fairshare_info = {key: info[i] for key,i in key_lookup.items() }
        else:
            fairshare_info = {}
        print(fairshare_info)
        return fairshare_info
 

    def get_jobs_in_queue(self):
        # codes = {'CD':'COMPLETED','CG':'COMPLETING','F':'FAILED','PD':'PENDING','PR':'PREEMPTED','R':'RUNNING','S':'SUSPENDED','ST':'STOPPED'}	

        self.jobs_in_queue['partition'] = []
        self.jobs_in_queue['name']      = []
        self.jobs_in_queue['status']    = []
        self.jobs_in_queue['time']      = []
        self.jobs_in_queue['memory']    = []
        self.jobs_in_queue['nodelist']  = []

        #NAME,PARTITION,USER,STATE,TIME,MIN_MEMORY
        cmd = f'squeue --account {self.cluster_account} --format="%j,%P,%u,%T,%M,%m,%N"'
        ret = subprocess.run(cmd,shell=True,capture_output=True)
        rstring = str(ret.stdout, 'utf-8')
        rlist = rstring.splitlines()

        if len(rlist) > 1:
            keys = rlist[0].strip().split(',')
            key_lookup = {key:i for i,key in enumerate(keys)}

            for job_str in rlist[1:]:
                job_info = job_str.strip().split(',')
                job_partition = job_info[key_lookup['PARTITION']]
                job_status    = job_info[key_lookup['STATE']]
                job_name      = job_info[key_lookup['NAME']]
                job_time      = job_info[key_lookup['TIME']]
                job_nodelist  = job_info[key_lookup['NODELIST']]
                job_memory    = job_info[key_lookup['MIN_MEMORY']]
                job_memory  = replace_multiplier(job_memory)
 
                self.jobs_in_queue['partition'] += [job_partition]
                self.jobs_in_queue['nodelist']  += [job_nodelist]
                self.jobs_in_queue['name']      += [job_name]
                self.jobs_in_queue['status']    += [job_status]
                self.jobs_in_queue['time']      += [job_time]
                self.jobs_in_queue['memory']    += [job_memory]


    def get_account_info(self):

        self.account_info['partition'] = []
        self.account_info['name']      = []
        self.account_info['job_id']    = []
        self.account_info['status']    = []
        self.account_info['exit_code'] = []

        cmd = f'sacct --account {self.cluster_account} --format="Partition%30,State%30,JobID%30,JobName%30,ExitCode%10,State%10"'
        ret = subprocess.run(cmd,shell=True,capture_output=True)
        rstring = str(ret.stdout, 'utf-8')
        rlist = rstring.splitlines()

        if len(rlist) > 2:
            keys = rlist[0].strip().split()
            key_lookup = {key:i for i,key in enumerate(keys)}

            for job_str in rlist[2:]:
                job_info = job_str.strip().split()
                try:
                    job_partition = job_info[key_lookup['Partition']]
                    job_status    = job_info[key_lookup['State']]
                    job_name      = job_info[key_lookup['JobName']]
                    job_id        = job_info[key_lookup['JobID']]
                    job_exit_code = job_info[key_lookup['ExitCode']]
                    self.account_info['partition'] += [job_partition]
                    self.account_info['name']      += [job_name]
                    self.account_info['job_id']    += [job_id]
                    self.account_info['status']    += [job_status]
                    self.account_info['exit_code'] += [job_exit_code]
                except:
                    pass 


    def get_pending_job_count(self):
        partition = self.cluster_partition
        job_index_in_partition = [i for i,elem in enumerate(self.jobs_in_queue['partition']) if elem == partition]
        return len([i for i in job_index_in_partition if self.jobs_in_queue['status'][i] in ['PD','PENDING']])


    def get_running_job_count(self):
        partition = self.cluster_partition
        job_index_in_partition = [i for i,elem in enumerate(self.jobs_in_queue['partition']) if elem == partition]
        return len([i for i in job_index_in_partition if self.jobs_in_queue['status'][i] in ['R','RUNNING']])


    def is_job_name_in_queue(self,job_name):
        return (job_name in self.jobs_in_queue['name'])


    def list_job_names_in_queue(self):
        return self.jobs_in_queue['name']



def main(configpath):
    cluster_stats = ClusterStats(configpath)
    scratch_use = cluster_stats.get_scratch_use()
    print(f'scratch use: {scratch_use}\n')
    cluster_stats.get_jobs_in_queue()
    print(f'jobs in queue: {cluster_stats.account_info}\n')
    print(f'pending job count: {cluster_stats.get_pending_job_count()}\n')
    print(f'running_job_count: {cluster_stats.get_running_job_count()}\n')
    print(f'job in queue: {cluster_stats.is_job_name_in_queue("test_123")}\n')
    cluster_stats.get_account_info()
    print(f'account info: {cluster_stats.account_info}\n')
    print(cluster_stats)
    cluster_stats.get_fairshare()
 

if __name__ == '__main__':
        parser = argparse.ArgumentParser(description="""Get cluster status.""")
        parser.add_argument( '-c', dest='configpath', type=str,  required=True, help='the path of config file')
        args = parser.parse_args()
        main(args.configpath)

