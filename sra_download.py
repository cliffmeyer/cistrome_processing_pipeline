#!/usr/bin/env python

# ================================
# Liulab DFCI
# @date: July 2020
# ================================

import os, sys
import urllib.request
import re
import argparse
import subprocess
import configparser
from pathlib import Path
from cistrome_logger import cistrome_logger
 
#SRATOOL_DEFAULT = "/repository/user/main/public/root"
class Log():
    logger = None


def read_config(config_filename):
    config = configparser.ConfigParser()
    config.optionxform=str
    config.read(config_filename)
    return(config)
 

def gzip_files(filename_list):
    for elem in filename_list:
        cmd = ['gzip',elem]
    subprocess.run(cmd)


def gunzip_files(filename_list):
    for elem in filename_list:
        cmd = ['gunzip',elem]
    subprocess.run(cmd)


def rename_file(old_filename,new_filename):
    rename_command = ["mv", old_filename, new_filename]
    subprocess.run(rename_command)


def concatenate_files(filename_list,catfilename):
    concat_command = ["cat"] + filename_list
    with open(catfilename,'w') as outfile:
        subprocess.run(concat_command,stdout=outfile)


def delete_files(filename_list):
    try:
        rm_command = ["rm"] + filename_list
        print(rm_command)
        subprocess.run(rm_command)
    except:
        pass


def check_files_exist(filename_list):
    for filename in filename_list:
       if os.path.exists(filename) == False:
           return False
    return True
 

class SRA_Tools():

    def __init__(self,config_filename):
        self.MIN_FASTQ_SIZE = 100
        self.config_filename=config_filename
        self.config = read_config(self.config_filename)
        self.set_paths() 
        self.configure_sratools_path()


    def set_logger(self,sample_id):
        log = cistrome_logger(f'__{sample_id}__ sra_download:',self.config['paths']['log_file'])
        Log.logger = log.logger # setting global variable here!
 

    def set_paths(self):
        self.sra_default_path = self.config['paths']['sratool_default']
        self.sra_root = self.config['paths']['sratool_custom']
        self.sra_path = os.path.join(self.sra_root,'sra')
        self.fastq_path = os.path.join(self.sra_root,'fastq')


    def configure_sratools_path(self):
        """
        SRA-Tools will download files by default to a path,, which cannot be overridden by a user option on the scripts.
        The default is changed here to a more suitable storage location with more space.
        """
        path_str = '%s=%s' % (self.sra_default_path, self.sra_root)
        cmd = ['vdb-config','--set',path_str]
        subprocess.run(cmd)


    # note one multiple runs (SRRs) can comprise a single SRA
    # in addition care needs to be taken of paired end data
    # see https://hbctraining.github.io/Accessing_public_genomic_data/lessons/downloading_from_SRA.html
    def download_fastq_srr_by_prefetch(self, srr_list):
        for i,srr in enumerate(srr_list):
            cmd = ['prefetch', srr]
            cmd_output = subprocess.run(cmd, stdout=subprocess.PIPE)


    def check_sra(self,sra_file):
        status = False
        try:
            sra_filename = os.path.basename(sra_file)
            cmd_output = subprocess.run(['vdb-validate', sra_file], capture_output=True)
            result = cmd_output.stderr.decode('utf-8')

            print('check sra output:', result)

            if ("'%s' is consistent" % sra_filename in result) and \
               ("'%s' metadata: md5 ok" % sra_filename in result):
                print("sra file OK: %s\n" % sra_file)
                status = True
            else:
                Log.logger.error(f'sra validate failed {sra_file}')
                status = False
        except:
            pass
        return status


    def check_file_size(self,filename):
        if os.path.getsize(filename) < self.MIN_FASTQ_SIZE:
            return False
        else:
            return True


    def check_prefetch(self,srr_file):
        # log errors 
        sra_path = os.path.join(self.sra_path, srr_file)
        if (os.path.exists(sra_path) == False):
           Log.logger.error(f'sra prefetch file not found {sra_path}')
           status = False
        elif (self.check_file_size(sra_path) == False):
           Log.logger.error(f'sra prefetch file size too small {sra_path}')
           status = False
        elif (self.check_sra(sra_path) == False ):
           Log.logger.error(f'sra prefetch check failed {srr_file}')
           status = False
        else:
           status = True

        return status


    def single_end_fastq_from_sra(self,srr_id):
        srr_filename = '%s.sra' % srr_id
        srr_path = os.path.join(self.sra_path,srr_filename)
        cmd = ['fastq-dump',srr_path,'-O',self.fastq_path]
        cmd_output = subprocess.run(cmd,capture_output=True)
        result_stdout = cmd_output.stdout.decode('utf-8')
        match_read  = re.search(r'Read[\s+]([0-9]+)[\s+]spots',result_stdout)
        match_write = re.search(r'Written[\s+]([0-9]+)[\s+]spots',result_stdout)

        if match_read and match_write:
            n_read  = int(match_read.group(1)) 
            n_write = int(match_write.group(1))
        else:
            n_read = 0
            n_write = 0

        if (n_read > 0) and (n_read == n_write):
            status = True
        else:
            Log.logger.error(f'splitting sra to single-end fastq for {srr_id}')
            status = False

        return status

 
    def sra_id_to_fastq_single_end_filename(self,srr_id):   
        fastq_name = '%s.%s' % (srr_id,'fastq')
        return fastq_name
 

    def split_paired_end_sra(self,srr_id):
        srr_filename = '%s.sra' % srr_id
        srr_path = os.path.join(self.sra_path, srr_filename)
        fastq_split_cmd = ['fastq-dump','--split-files',srr_path,'-O',self.fastq_path]
        cmd_output = subprocess.run(fastq_split_cmd,capture_output=True)
        result_stdout = cmd_output.stdout.decode('utf-8')
        match_read  = re.search(r'Read[\s+]([0-9]+)[\s+]spots',result_stdout)
        match_write = re.search(r'Written[\s+]([0-9]+)[\s+]spots',result_stdout)

        if match_read and match_write:
            n_read  = int(match_read.group(1)) 
            n_write = int(match_write.group(1))
        else:
            n_read = 0
            n_write = 0

        if (n_read > 0) and (n_read == n_write):
            status = True
        else:
            Log.logger.error(f'splitting sra to paired-end fastq for {srr_id}')
            status = False
     
        return status


    def sra_id_to_fastq_paired_end_filenames(self,srr_id):  
        fastq_split_names = ['%s_%d.%s' % (srr_id,i,'fastq') for i in [1,2]]
        return fastq_split_names


    def extract_single_end_fastq_from_sra(self,gsm_id,srr_list):
        status = True
        fastq_filename_list = []        
        self.download_fastq_srr_by_prefetch(srr_list)

        for srr_id in srr_list:
            srr_filename = '%s.sra' % srr_id
            status = self.check_prefetch(srr_filename)
            if status == True:
                status = self.single_end_fastq_from_sra(srr_id)
            else:
                Log.logger.error(f'sra prefetch failed for {srr_id} in {gsm_id}')
                break  # write error to log: gsm_id ssr time prefetch error

            fastq_filename = self.sra_id_to_fastq_single_end_filename(srr_id)  
            fastq_full_filename = os.path.join(self.fastq_path,fastq_filename)
            if os.path.exists(fastq_full_filename):
                fastq_filename_list += [fastq_full_filename]
 
            if status == False:
                Log.logger.error(f'error in conversion to fastq for {srr_id} in {gsm_id}')
                delete_files(fastq_filename_list) # something wrong in conversion to fastq
                break

        if status == True:
            fastq_concat_filename = '%s.fastq' % (gsm_id)
            print('concatenating to fastq files %s' % fastq_concat_filename)

            if len(fastq_filename_list) == 1:
                rename_file( fastq_filename_list[0], os.path.join( self.fastq_path, fastq_concat_filename) )
            elif len(fastq_filename_list) > 1:
                concatenate_files(fastq_filename_list, os.path.join( self.fastq_path, fastq_concat_filename))
                delete_files(fastq_filename_list)

        return status


    def extract_paired_end_fastq_from_sra(self,gsm_id,srr_list):
        status = True
        fastq_filename_1_list = []        
        fastq_filename_2_list = []        

        print('download by prefetch ...')
        self.download_fastq_srr_by_prefetch(srr_list)

        for srr_id in srr_list:
            srr_filename = '%s.sra' % srr_id
            print('check prefetch %s' % srr_filename)
            status = self.check_prefetch(srr_filename)

            if status == False:
                Log.logger.error(f'sra prefetch failed for {srr_id} in {gsm_id}')
                break
            else:
                print('convert to fastq %s' % srr_filename)
                status = self.split_paired_end_sra(srr_id)

            if status == False:
                Log.logger.error(f'error in conversion to fastq for {srr_id} in {gsm_id}')
                break
            else:
                fastq_filenames = self.sra_id_to_fastq_paired_end_filenames(srr_id)  
 
            if len(fastq_filenames) == 2:
                fastq_filename_1,fastq_filename_2 = fastq_filenames[0],fastq_filenames[1]
                fastq_filename_1_list += [os.path.join(self.fastq_path, fastq_filename_1)]        
                fastq_filename_2_list += [os.path.join(self.fastq_path, fastq_filename_2)]        
            else:
                Log.logger.error(f'error in conversion to fastq for {srr_id} in {gsm_id}')
                # something wrong in conversion to fastq
                delete_files(fastq_filenames)
                status = False
                break

        if status == True and check_files_exist(fastq_filename_1_list) == False:
            status = False

        if status == True and check_files_exist(fastq_filename_2_list) == False:
            status = False

        if status == True:
            fastq_concat_filename_1 = '%s_R1.fastq' % (gsm_id)
            fastq_concat_filename_2 = '%s_R2.fastq' % (gsm_id)

            print('concatenating to fastq files %s' % fastq_concat_filename_1)
            if len(fastq_filename_1_list) == 1:
                rename_file( fastq_filename_1_list[0], os.path.join( self.fastq_path, fastq_concat_filename_1) )
                rename_file( fastq_filename_2_list[0], os.path.join( self.fastq_path, fastq_concat_filename_2) )
            elif len(fastq_filename_1_list) > 1:
                concatenate_files( fastq_filename_1_list, os.path.join( self.fastq_path, fastq_concat_filename_1) )
                concatenate_files( fastq_filename_2_list, os.path.join( self.fastq_path, fastq_concat_filename_2) )
                delete_files(fastq_filename_1_list)
                delete_files(fastq_filename_2_list)

        return status


    def write_fastq_checkfile(self,gsm_id):
        checkfile = os.path.join( self.fastq_path, f'{gsm_id}.check' )
        Path(checkfile).touch()


def get_layout_type(srx_html,gsm):
    layout_type = re.search('<div>Layout: <span>.{6}</span>',srx_html)
    if layout_type:
        layout_type = layout_type.group()
        layout_type = layout_type[-13:-7]
        sys.stderr.write(layout_type + "\n")
    if layout_type not in ["SINGLE","PAIRED"]:
        layout_type = "OTHER"
        Log.logger.error(f'{gsm}: sequence file layout type neither single not paired-end')
    return layout_type


# get run accession SRR
# https://www.ncbi.nlm.nih.gov/books/NBK56913/
# first letter: S = NCBI-SRA, E = EMBL-SRA, D = DDBJ-SRA
def get_run_accession(experiment_accession_html):
    accession_regexp = re.compile('>[S|E|D]RR[0-9]*</a></td><td')
    accession = accession_regexp.findall(experiment_accession_html)
    accession = [i.lstrip(">").split("</a")[0] for i in accession]
    return accession


#def get_srr(srx_html):
#    return get_run_accession(srx_html)


def get_srx_html(gsm_html):
    srx_info = re.search('https://www.ncbi.nlm.nih.gov/sra\S*"',gsm_html)
    if srx_info:
        srx = srx_info.group().rstrip('"').lstrip('https://www.ncbi.nlm.nih.gov/sra?term=')
        # get the SRR id('>SRR1588518</a></td><td') and find the type of layout
        srx_url = 'http://www.ncbi.nlm.nih.gov/sra?term=%s' % srx
        srx_html = urllib.request.urlopen(srx_url).read().decode('utf-8')
        return srx_html
    else:
        Log.logger.error(f'srx file not found')
        return None


def get_gsm_html(gsm):
    gsm_url = 'http://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=%s' % gsm
    gsm_html = urllib.request.urlopen(gsm_url).read().decode('utf-8')
    return gsm_html


def main():
    class MyParser(argparse.ArgumentParser):
        def error(self, message):
            sys.stderr.write('error: %s\n' % message)
            self.print_help()
            sys.exit(2)

    parser = MyParser()
    parser.add_argument('-c', '--config', help='Configuration file', required=True)
    parser.add_argument('-i', '--id',     help='GSM id', required=True)
    parser.add_argument('-o', '--output', help='path with links to fastq', required=False, default=None)
    parser.add_argument('-g', '--gzip',   help='flag for fastq file gzip compression', action = "store_true", default=False)
    args = parser.parse_args()

    sra_tool = SRA_Tools(args.config)
    sra_tool.set_logger(args.id)
    
    gsm_id = args.id
    compress = args.gzip

    gsm_html = get_gsm_html(gsm_id)
    srx_html = get_srx_html(gsm_html)
    experiment_accession_html = get_srx_html(gsm_html)
    print('srx_html',srx_html)

    if srx_html:
        srr_list = get_run_accession(experiment_accession_html)
        #get_srr(srx_html)
    else:
        srr_list = []

    print('srr_list',srr_list)

    if len(srr_list) > 0:
        layout_type = get_layout_type(srx_html,gsm_id)
    else:
        layout_type = ''

    if layout_type == "SINGLE":
        status = sra_tool.extract_single_end_fastq_from_sra(gsm_id,srr_list)
    elif layout_type == "PAIRED":
        status = sra_tool.extract_paired_end_fastq_from_sra(gsm_id,srr_list)
    else:
        status = False

    if status == True:
       sra_tool.write_fastq_checkfile(gsm_id)



if __name__ == "__main__":
    main()
