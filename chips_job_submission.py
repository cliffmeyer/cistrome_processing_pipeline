#!/usr/bin/env python

import yaml
import os
import errno
import subprocess
import sys
import time
import argparse
import re
import configparser 
import math
import sbatch_header
from cistrome_logger import cistrome_logger 


def set_runtime(fastq_size):
    MAX_MINUTES = 60*23
    size_Mb = (1.0*fastq_size)/(1024*2024)
    minutes = int(60 + 0.5*size_Mb)
    minutes = min(minutes,MAX_MINUTES)
    return minutes


def set_mem(fastq_size):
    min_Mb = 10000  # for mapping to the human genome a minimum is needed for the mapping index
    max_Mb = 16000  # 
    size_Mb = (1.0*fastq_size)/(1024*2024)
    mem = int(min_Mb + 0.1*size_Mb)
    mem = min(mem,max_Mb)
    return mem


class SystemConfig():
    def __init__(self,config_filename):
        self.config_filename = config_filename
        self.read_config()

    def read_config(self):
        self.config = configparser.ConfigParser()
        self.config.optionxform=str
        self.config.read(self.config_filename)


class ChipsSetup():

    def __init__(self, system_config_filename = None, species = None, sample_id = None, sample_type = None):
        self.sys_config = SystemConfig(system_config_filename).config
        tmp_logger = cistrome_logger('chips_pipeline',self.sys_config['paths']['log_file'])
        self.logger = tmp_logger.logger
        self.species = species
        self.sample_id = sample_id
        self.sbatch_filename = f'{self.sample_id}_chips.sbatch'
        self.jobname = f'{self.sample_id}_chips'
        self.sample_type = sample_type.lower() # h3k27ac, dnase, tf
 
    def set_paths(self):
        # sra_files
        # fastq_files
        #     - sample1.fastq
        #     - sample2_R1.fastq
        #     - sample2_R2.fastq
        # root_folder
        #     - sbatch_folder
        #         - sample1_chips.sbatch
        #         - sample1_sra.sbatch
        #         - sample2_chips.sbatch 
        #         - sample2_sra.sbatch 
        #     - runs_folder
        #         - sample1_folder
        #             - analysis
        #         - sample2_folder
        #             - analysis 
        self.root_path   = self.sys_config['paths']['data_collection_root']
        self.sbatch_path = self.sys_config['paths']['data_collection_sbatch']
        self.runs_path   = self.sys_config['paths']['data_collection_runs']
        self.fastq_path  = self.sys_config['paths']['fastq']
        self.sra_path    = self.sys_config['paths']['sra'] 
        self.sample_path = os.path.join( self.runs_path, self.sample_id )
        self.chips_work_path = os.path.join( self.sample_path, self.sys_config['paths']['chips_work_directory'] )
        self.chips_cistrome_result_path = os.path.join( self.sample_path, self.sys_config['paths']['cistrome_result'] )
        self.metasheet_filename = os.path.join( self.sample_path, 'metasheet.csv' )
        self.chips_yaml  = os.path.join( self.sample_path, 'config.yaml' )
        self.chips_log_path = os.path.join( self.sample_path, f'chips_log_{self.sample_id}.txt' )

    def determine_and_set_sample_fastq_path_from_layout(self):
        single_end_path = os.path.join( self.fastq_path, f'{self.sample_id}.fastq' )
        paired_end_1_path = os.path.join( self.fastq_path, f'{self.sample_id}_R1.fastq' )
        paired_end_2_path = os.path.join( self.fastq_path, f'{self.sample_id}_R2.fastq' )
        if os.path.exists(single_end_path):
            self.sample_fastq_path = [single_end_path]
        elif os.path.exists(paired_end_1_path) and os.path.exists(paired_end_2_path):
            self.sample_fastq_path = [paired_end_1_path,paired_end_2_path] 
        else:
            self.logger.error(f'fastq file not found {self.sample_id}')
            raise FileNotFoundError(errno.ENOENT,os.strerror(errno.ENOENT),self.sample_id)


    def make_missing_directories(self):
       if not os.path.exists(self.root_path):
           os.mkdir(self.root_path)
       if not os.path.exists(self.runs_path):
           os.mkdir(self.runs_path)
       if not os.path.exists(self.sbatch_path):
           os.mkdir(self.sbatch_path)
       if not os.path.exists(self.sample_path):
           os.mkdir(self.sample_path)
       if not os.path.exists(self.chips_work_path):
           os.mkdir(self.chips_work_path)
       if not os.path.exists(self.chips_cistrome_result_path):
           os.mkdir(self.chips_cistrome_result_path)
 

    def link_chips_files(self):
        #ref_files_path  = os.path.join(self.sample_path, 'ref_files')
        cistrome_chips_path = os.path.join(self.sample_path, 'cistrome_chips')
        if os.path.exists(cistrome_chips_path) == False:
            subprocess.run('ln -s %s %s' % (self.sys_config['chips']['chips_path'], cistrome_chips_path), shell=True )
        #if os.path.exists(ref_files_path) == False:
        #    subprocess.run('ln -s %s %s' % (self.sys_config['chips']['chips_ref_files'], ref_files_path), shell=True )


    def get_sample_type_dependent_parameters(self):

        broad_histone  = ["h3k27me1","h3k27me2","h3k27me3","h3k9me1","h3k9me2","h3k9me3","h3k20me1","h3k20me2","h3k20me3","h3k36me1","h3k36me2","h3k36me3","h3k79me1","h3k79me2","h3k79me3","h2ak119ub"]
        narrow_histone = ["h3k27ac","h3k9ac","h3k4me1","h3k4me2","h3k4me3","h3s10p"]   

        chips_config = {}

        if self.sample_type == "dnase":
            chips_config["cutoff"] = 150
            chips_config["ChIP_model"] = False
            self.broad_type = False
        elif self.sample_type == "atac":
            chips_config["cutoff"] = 150
            chips_config["ChIP_model"] = False
            self.broad_type = False
        elif self.sample_type=="tf":
            chips_config["motif"] = 'mdseqpos'
            chips_config["ChIP_model"] = True
            self.broad_type = False
        elif self.sample_type in narrow_histone:
            self.broad_type = False
            chips_config["ChIP_model"] = True
        elif self.sample_type in broad_histone:
            self.broad_type = True

        chips_config["macs2_broadpeaks"] = self.broad_type

        return chips_config 


    def get_paths_from_cistrome_config(self):
        chips_config = {}
        # parameters from pipeline config file
        for key,val in self.sys_config[self.species].items():
            chips_config[key] = val
        return chips_config
 

    def write_chips_config_file(self):
        chips_config = {}
        chips_config["metasheet"]    = self.metasheet_filename
        chips_config["ref"]          = os.path.join(self.sys_config['chips']['chips_path'],'ref.yaml')
        chips_config["output_path"]  = self.chips_work_path
        chips_config["assembly"]     = self.species
        chips_config["aligner"]      = "bwa"
        chips_config["cnv_analysis"] = False
        chips_config["cutoff"]       = 0
        chips_config["CistromeApi"]  = True
        chips_config["Cistrome_path"]= self.chips_cistrome_result_path
        chips_config["samples"]      = {}
        chips_config["samples"][self.sample_id] = self.sample_fastq_path
 
        paths_from_cistrome_config = self.get_paths_from_cistrome_config()
        if bool(paths_from_cistrome_config):
            chips_config.update(paths_from_cistrome_config)

        sample_type_dependent_parameters = self.get_sample_type_dependent_parameters()
        if bool(sample_type_dependent_parameters):
            chips_config.update(sample_type_dependent_parameters)

        self.get_sample_type_dependent_parameters()
        with open(self.chips_yaml,"w") as fp:
            yaml.dump(chips_config, fp)


    def write_chips_metadata_file(self):
        metasheet_header  = ["RunName","Treat1","Cont1","Treat2","Cont2"]
        metasheet_content = [self.sample_id,self.sample_id,"","",""]
        with open(self.metasheet_filename,"w") as metasheet_file:
            metasheet_file.write(",".join(metasheet_header)+"\n")
            metasheet_file.write(",".join(metasheet_content)+"\n")


    def set_resources_from_fastqfile_check(self):
        if isinstance( self.sample_fastq_path, list ):
            fastq_size = os.path.getsize(self.sample_fastq_path[0])
        else:
            fastq_size = os.path.getsize(self.sample_fastq_path)
        self.time = set_runtime(fastq_size)
        self.mem  = set_mem(fastq_size)
        self.core = 1


    def write_chips_command_sbatch(self):

        if isinstance( self.sample_fastq_path, list ):
            sample_fastq_path = self.sample_fastq_path[0]
        else:
            sample_fastq_path = self.sample_fastq_path
 
        if os.path.exists(sample_fastq_path):
            self.set_resources_from_fastqfile_check()
        else:
            self.logger.error(f'fastq file not found {sample_fastq_path}')
            sys.stderr.write("MISSING fastq FILES! -- %s"% sample_fastq_path)

        cmd = f'cd {self.sample_path}\n'
        chips_snakemake_path = os.path.join( self.sys_config['chips']['chips_path'], 'chips.snakefile' )
        cmd += f'snakemake -s {chips_snakemake_path} --configfile {self.chips_yaml} --rerun-incomplete --unlock\n'
        cmd += 'sleep 5\n'
        cmd += f'snakemake -s {chips_snakemake_path} -j {self.core} --configfile {self.chips_yaml} --rerun-incomplete\n'

        header = sbatch_header.SbatchHeader( time=self.time, mem=self.mem, job_name=f'{self.jobname}', log_filename=self.chips_log_path )
        path = os.path.join(self.sbatch_path,self.sbatch_filename)
        sbatch_header.write_sbatch( cmd, sbatch_path=path, header=header.__str__() )
 

    def submit_sbatch(self):
        sbatch_path = os.path.join(self.sbatch_path,self.sbatch_filename)
        if os.path.exists(sbatch_path):
            os.system(f'sbatch {sbatch_path}')
        else:
            self.logger.error(f'sbatch file not found {sbatch_path}')


    def cancel_sbatch(self):
        os.system(f'scancel --name {self.jobname}')
        time.sleep(60)


def main():

    try:
        parser = argparse.ArgumentParser(description="""get chips command, and optionally submit""")
        parser.add_argument( '-c',           dest='configpath', type=str,  required=True, help='the path of config file')
        parser.add_argument( '--gsm',        dest='gsmID',      type=str,  required=True, help='gsm ID of sample')
        parser.add_argument( '--species',    dest='species',    type=str,  required=True, help='species of sample [hg38,mm10]')
        parser.add_argument( '--sampletype', dest='sampletype', type=str,  required=True, help='type of sample [tf,h3h27ac,dnase,atac]')
        parser.add_argument( '--broad',      dest='broad',      action='store_true',      help='call broad peak')
        parser.add_argument( '--submit',     dest='submit',     action='store_true',      help='submit job to sbatch queue')

        args = parser.parse_args()

        chips_obj = ChipsSetup( system_config_filename=args.configpath, species=args.species, sample_id=args.gsmID, sample_type=args.sampletype.lower() )

        chips_obj.set_paths()
        chips_obj.determine_and_set_sample_fastq_path_from_layout()
        chips_obj.make_missing_directories()
        chips_obj.link_chips_files()
        chips_obj.write_chips_config_file()
        chips_obj.write_chips_metadata_file()
        chips_obj.set_resources_from_fastqfile_check()
        chips_obj.write_chips_command_sbatch()

        if args.submit == True:
            chips_obj.cancel_sbatch()
            chips_obj.submit_sbatch()        

    except KeyboardInterrupt:
        sys.stderr.write("User interrupted me!\n")
        sys.exit(0)


if __name__ == '__main__':
    main()
