import datetime
import os,sys,time
from socket import gethostname
import argparse

# ===============================================================================================
#
# Note: the files sbatch_configuration_for_CLUSTERNAME and environment_for_CLUSTERNAME
# need to be defined as part of the configuration of CLUSTERNAME
# Note: the configuration function is looked up using the domain name.
#
# ===============================================================================================

def get_domain_name():
    name = gethostname()
    name = name.split('.')[-4:]
    name = '.'.join(name)
    return(name)


def time_format_as_day_hr_min(minutes):
    hours = int(minutes/60)
    days    = int(hours/24)
    hours   = hours % 24
    minutes = minutes % 60
    day_hour_minute = '%d-%02d:%02d' % (days,hours,minutes)
    return day_hour_minute


class SbatchHeader():

    def __init__(self,  nodes=1, cpus=1, time=1, mem=1, job_name='test', partition='test', log_filename='test_log_tmp'):

        """ Configuration note: cluster specific header configuration register. Add method to generate sbatch header below."""
        self.cluster_register = {'rc.fas.harvard.edu':self.sbatch_configuration_for_odyssey,'O2':self.sbatch_configuration_for_O2}
        self.cluster_env_register = {'rc.fas.harvard.edu':self.environment_for_odyssey,'O2':self.environment_for_O2}

        self.cluster_name = get_domain_name()
        self.cluster_str_method = self.cluster_register[self.cluster_name]      
        self.cluster_str_env_method = self.cluster_env_register[self.cluster_name]      
        self.nodes = str(nodes)
        self.cpus = str(cpus)
        self.time = time
        self.mem_Mb = str(mem)
        self.log_filename = log_filename
        self.job_name = job_name
 
    def __str__(self):
        tmp_str  = self.cluster_str_method()
        tmp_str += self.cluster_str_env_method()
        return tmp_str


    def sbatch_configuration_for_odyssey(self):
        """Configuration note: this function needs to be defined for the cluster"""
        #if self.time > 12*60:
        #    partition = 'shared'
        if self.time < 10:
            partition = 'test'
        else:
            partition = 'serial_requeue'  # serial_request is half the cost of other queues

        day_hr_min = time_format_as_day_hr_min(self.time)
 
        header  = ['#!/bin/bash']
        header += [f'#SBATCH --job-name={self.job_name}']
        header += [f'#SBATCH --nodes {self.nodes}']
        header += [f'#SBATCH -n {self.cpus}']
        header += [f'#SBATCH --time {day_hr_min}']
        header += [f'#SBATCH --mem={self.mem_Mb}MB']
        header += [f'#SBATCH --partition {partition}']
        header += [f'#SBATCH -o {self.log_filename}']
        header += [f'#SBATCH --no-requeue'] # so the same job is not submitted twice
        header += ['']
        return '\n'.join(header) 

    def environment_for_odyssey(self):
        """Configuration note: this function needs to be defined for the cluster"""
        environ  = ['source /n/xiaoleliu_lab/chips/miniconda3/bin/activate']
        environ += ['conda activate chips']
        environ += ['module load gcc']
        environ += ['module load R']
        environ += ['module load texlive']
        environ += ['']
        return '\n'.join(environ)


    def sbatch_configuration_for_O2(self):
        """Configuration note: this function needs to be defined for the cluster"""
        pass

    def environment_for_O2(self):
        """Configuration note: this function needs to be defined for the cluster"""
        pass


def write_sbatch(cmd, sbatch_path='', header=''):
    with open(sbatch_path, "w") as fp:
        fp.write(header)
        fp.write(cmd)


def submit_sbatch(sbatch_path):
    if os.path.exists(sbatch_path):
        os.system(f'sbatch {sbatch_path}')
    else:
        print('sbatch file is missing')


def main():
    parser = argparse.ArgumentParser(description="""Write sbatch file""")
    parser.add_argument( '--cmd',       dest='cmd_str',   type=str, required=True,                       help='command to be executed')
    parser.add_argument( '--time',      dest='time',      type=int, required=False, default=1,           help='time in minutes')
    parser.add_argument( '--mem',       dest='mem',       type=int, required=False, default=200,         help='memory in Mb')
    parser.add_argument( '--partition', dest='partition', type=str, required=False, default='test',      help='name of partition')
    parser.add_argument( '--nodes',     dest='nodes',     type=int, required=False, default=1,           help='number of nodes')
    parser.add_argument( '--cpus',      dest='cpus',      type=int, required=False, default=1,           help='number of cpus')
    parser.add_argument( '--jobname',   dest='jobname',   type=str, required=False, default='test',      help='job name')
    parser.add_argument( '--log',       dest='logfile',   type=str, required=False, default='tmp.log',   help='log file name')
    parser.add_argument( '--sbatchfile',dest='sbatchfile',type=str, required=False, default='tmp.sbatch',help='sbatch file name') # TODO default to stdout
    parser.add_argument( '--submit',    dest='submit', action='store_true', help='submit job to sbatch queue')
    #parser.add_argument( '--config', dest='configpath', type=str, required=False, help='the path of config file')

    args = parser.parse_args()
    header = SbatchHeader(nodes=args.nodes, cpus=args.cpus, time=args.time, mem=args.mem, job_name=args.jobname, partition=args.partition, log_filename=args.logfile)
    
    write_sbatch( args.cmd_str, sbatch_path=args.sbatchfile, header=header.__str__())
    if args.submit == True:
        time.sleep(1)
        submit_sbatch(args.sbatchfile)


if __name__ == '__main__':
    main()

