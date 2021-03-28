import os
import sys
import yaml
import path_parser
import filename_pattern_regex
import pyBigWig # in chips environment
import json
import subprocess
import argparse
import configparser
from cistrome_logger import cistrome_logger 


class bigwig():
    def __init__(self,filename):
        self.filename = filename
        self.bw = None

    def open_file(self):
        self.bw = pyBigWig.open(self.filename)

    def close_file(self):
        self.bw.close()

    def is_open(self):
        return not(self.bw == None)

    def get_chroms(self):
        return self.bw.chroms()

    def get_header(self):
        return self.bw.header()


def validate_bam(filename):
    cmd = f'samtools quickcheck {filename}'
    cmd_output = subprocess.run(cmd,shell=True,capture_output=True)
    result_stdout = cmd_output.stdout.decode('utf-8')
    if ('missing EOF block' in result_stdout):
        message = f'missing EOF block: {filename}'
        Config.logger.error(message)
        remove_file(filename)
        return False
    else:
        return True


def validate_json(filename):
    with open(filename) as file:
        try:
            json.load(file)
            return True
        except:
            remove_file(filename)
            return False


def countlines(fp):
    num_lines = sum(1 for line in fp)
    return num_lines  


class chips_check_function():

    def __init__(self):
        self.sample_config = {}
        self.min_lines = 100
        self.chroms = {}
        # Note, alternative chromosomes are excluded because they create unresolved mapping ambiguity 
        self.chroms['hg19'] = [f'chr{i}' for i in range(1,23)] + ['chrM','chrX','chrY'] # hg19, hg38, mm9, mm10 'assembly' keywords used in chips config  
        self.chroms['hg38'] = [f'chr{i}' for i in range(1,23)] + ['chrM','chrX','chrY'] 
        self.chroms['GDC_hg38'] = [f'chr{i}' for i in range(1,23)] + ['chrM','chrX','chrY'] 
        self.chroms['mm9']  = [f'chr{i}' for i in range(1,20)] + ['chrM','chrX','chrY']
        self.chroms['mm10'] = [f'chr{i}' for i in range(1,20)] + ['chrM','chrX','chrY']

        self.optional_chroms = {}
        self.optional_chroms['hg19'] = ['chrM','chrY'] # hg19, hg38, mm9, mm10 'assembly' keywords used in chips config  
        self.optional_chroms['hg38'] = ['chrM','chrY'] 
        self.optional_chroms['GDC_hg38'] = ['chrM','chrY'] 
        self.optional_chroms['mm9']  = ['chrM','chrY']
        self.optional_chroms['mm10'] = ['chrM','chrY']

        self.min_bw_coverage = 1e9

    def read_chips_sample_yaml(self,chips_sample_yaml):
        # read yaml to get sample specific settings like motif analysis
        with open(chips_sample_yaml,'r') as fp:
            self.sample_config = yaml.safe_load(fp)

    def check_peak_bed_file(self,filename):
        with open(filename,'r') as fp:
            nlines = countlines(fp)
            if ( nlines > self.min_lines ):
                message = f'{filename}: ok'
                Config.logger.info(message)
                #return True
                #warning only
            else:
                message = f'Peak bed file {filename} has too few lines {nlines}'
                Config.logger.warning(message)
                #return False
                #warning only
            return True

    def check_narrow_peak_bed_file(self,filename):
        if self.sample_config['macs2_broadpeaks'] ==  True: 
            return True
        else:
            with open(filename,'r') as fp:
                nlines = countlines(fp)

            if ( nlines < self.min_lines ):
                message = f'Narrow peak bed file {filename} has too few lines {nlines}'
                Config.logger.warning(message)
                #return False
                #warning only
            else:
                message = f'{filename}: ok'
                Config.logger.info(message)
                #return True
                #warning only
            return True

    def check_broad_peak_bed_file(self,filename):
        if self.sample_config['macs2_broadpeaks'] == False:
            return True
        else:
            with open(filename,'r') as fp:
                nlines = countlines(fp)

            if nlines < self.min_lines:
                message = f'Broad peak bed file {filename} has too few lines {nlines}'
                Config.logger.warning(message)
                #return False
                #warning only
            else:
                message = f'{filename}: ok'
                Config.logger.info(message)
                #return True
                #warning only
            return True

    def check_peak_xls_file(self,filename):
        with open(filename,'r') as fp:
            nlines = countlines(fp)
        if nlines > self.min_lines:
            message = f'{filename}: ok'
            Config.logger.info(message)
            #warning only: return True
        else:
            message = f'Peak xls file {filename} has too few lines {nlines}'
            Config.logger.warning(message)
            #warning only: return False
        return True

    def check_peak_summit_bed_file(self,filename): 
        with open(filename,'r') as fp:
            nlines = countlines(fp)
        if ( nlines > self.min_lines ):
            message = f'{filename}: ok'
            Config.logger.info(message)
            #warning only: return True
        else:
            message = f'Summit bed file {filename} has too few lines {nlines}'
            Config.logger.warning(message)
            #warning only: return False

        return True

    def check_motif_files(self,filename):
        if 'motif' not in self.sample_config:
            message = f'{filename} skipping'
            Config.logger.info(message)
            return True

        if os.path.exists(filename):
            message = f'{filename}: ok'
            Config.logger.info(message)
            return True
        else:
            message = f'Motif file {filename} not found'
            Config.logger.warning(message)
            return False

    def check_exists(self,filename):
        if os.path.exists(filename):
            message = f'{filename}: found'
            Config.logger.info(message)
            return True
        else:
            message = f'chips check: {filename} not found'
            Config.logger.error(message)
            return False

    def check_motif_json(self,filename):
        if 'motif' not in self.sample_config:
            message = f'{filename} skipping'
            Config.logger.info(message)
            return True

        if self.check_exists(filename) and validate_json(filename):
            message = f'{filename}: ok'
            Config.logger.info(message)
            return True
        else:
            message = f'chips check: json file {filename} not validated'
            Config.logger.warning(message)
            return False

    def check_ChIP_frag_json(self,filename):
        if ('ChIP_model' not in self.sample_config) or (self.sample_config['ChIP_model'] == False):
            message = f'{filename} skipping'
            Config.logger.info(message)
            return True

        if self.check_exists(filename) and validate_json(filename):
            message = f'{filename}: ok'
            Config.logger.info(message)
            return True
        else:
            message = f'chips check: json file {filename} not validated'
            Config.logger.warning(message)
            return False
  
    def check_json(self,filename):
        if self.check_exists(filename) and validate_json(filename):
            message = f'{filename}: ok'
            Config.logger.info(message)
            return True
        else:
            message = f'chips check: json file {filename} not validated'
            Config.logger.error(message)
            return False

    def check_bigwig_file(self,filename):
        bw = bigwig(filename)
        bw.open_file()

        if bw.is_open() == False:
            message = f'chips check: failed to open bigwig file: {filename}'
            Config.logger.error(message)
            return False

        chroms_in_bw = bw.get_chroms()
        chroms_in_bw = chroms_in_bw.keys()
        test_chroms = self.chroms[self.sample_config['assembly']]

        # chrY and chrM might be different - the others should be there
        optional_chroms = self.optional_chroms[self.sample_config['assembly']]

        diff_set = set(test_chroms) - set(chroms_in_bw) - set(optional_chroms)
        if len(diff_set) > 0:
            message = f'chips check: missing chromosomes in bigwig {diff_set}'
            Config.logger.error(message)
            bw.close_file()
            return False

        diff_set = set(chroms_in_bw) - set(test_chroms)
        if len(diff_set) > 0:
            message = f'chips check: extra chromosomes in bigwig {diff_set}'
            Config.logger.error(message)
            bw.close_file()
            return False
 
        bw_header = bw.get_header()
        bw_coverage = bw_header['nBasesCovered'] 
        if bw_coverage < self.min_bw_coverage:
            message = f'chips check: low converage in bigwig {bw_coverage}'
            Config.logger.error(message)
            bw.close_file()
            return False       

        message = f'{filename}: ok'
        Config.logger.info(message)
        bw.close_file()
        return True


    def check_bam(self,filename):

        if self.check_exists(filename) and validate_bam(filename):
            message = f'{filename}: ok'
            Config.logger.info(message)
            return True
        else:
            message = f'chips check: bam file {filename} not validated'
            Config.logger.warning(message)
            return False
 

def get_files_in_subdirs(path):
    file_path_list = []
    for root,dirs,files in os.walk(path,topdown=False):
        for name in files:
            file_path_list += [os.path.join(root,name) for name in files]
    return file_path_list


def remove_file(path):
    try: 
        os.remove(path) 
    except OSError as error: 
        pass


def write_md5sum(sample_path,sample_id=None):
    md5_path = os.path.join(sample_path,f'{sample_id}.md5')
    remove_file(md5_path)
    file_list = get_files_in_subdirs(sample_path)
    file_str = ' '.join(file_list)
    #md5_cmd = f'md5sum {file_str} > {md5_path_tmp} && mv {md5_path_tmp} {md5_path}'
    md5_cmd = f'md5sum {file_str}'
    cmd_output = subprocess.run(md5_cmd,shell=True,capture_output=True)
    md5_str = cmd_output.stdout.decode('utf-8')
    md5_str = md5_str.replace(sample_path,'./')
    md5_str = md5_str.replace('//','/')
    with open(md5_path,'w') as fp:
        print(md5_str,file=fp)
 


class Config():
    configpath = ''
    logger = None
    sys_config = None

    def __init__(self,configpath):
        Config.sys_config = configparser.ConfigParser()
        Config.sys_config.optionxform=str
        Config.sys_config.read(configpath)
        Config.configpath = configpath


class StdoutLog():
    def debug(self,message):
        print(message)

    def warning(self,message):
        print(message)

    def critical(self,message):
        print(message)

    def error(self,message):
        print(message)

    def info(self,message):
        print(message)


def main(configpath,sample_id,output_to_stdout):

    Config(configpath)
    if output_to_stdout == True:
        Config.logger = StdoutLog()
    else:
        log = cistrome_logger(f'__{sample_id}__check_chips:',Config.sys_config['paths']['log_file'])
        Config.logger = log.logger
        Config.logger.debug('This is a test')
 
    chips_check_yaml  = Config.sys_config['process_server']['chips_check_yaml']
    path_root         = Config.sys_config['paths']['data_collection_runs']
    sample_path       = os.path.join( path_root, sample_id )
    chips_sample_yaml = os.path.join( path_root, sample_id, 'config.yaml')

    check = chips_check_function()
    check.read_chips_sample_yaml(chips_sample_yaml)
    path_check = path_parser.paths_from_yaml( chips_check_yaml, sample_path, sample_id )
    path_list_to_check = path_check['path_list']
    path_check_funcs   = path_check['check_register']
    files_matching_path_list = filename_pattern_regex.match_file_list_patterns( path_list_to_check )
  
    passed_all_checks = True

    for key,val in path_check_funcs.items():

        file_check_method = getattr(check, val)
        filename_list = files_matching_path_list[key]
        if len(filename_list) > 0:
            filename = filename_list[0]
        else:
            filename = ''

        message = f'checking: {key} {filename} {val}'
        Config.logger.info(message)

        result = file_check_method(filename) # missing files might be ok - depending on the sample type
        if result == False:
            passed_all_checks = False
            message = f'error found during {key} check'
            Config.logger.error(message)
         
     
    # md5 sum file indicates all checks passed
    cistrome_result_path = os.path.join( path_root, sample_id, Config.sys_config['paths']['cistrome_result'] )
    if passed_all_checks == True and output_to_stdout == False:
        write_md5sum( cistrome_result_path, sample_id )


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="""checks for chips result""")
    parser.add_argument( '-c', dest='configpath', type=str,  required=True, help='the path of config file')
    parser.add_argument( '-i', dest='sample_id', type=str,  required=True, help='sample id')
    parser.add_argument( '-v', dest='output_to_stdout', action='store_true', help='write to stdout instead of log')
    args = parser.parse_args()

    main( args.configpath, args.sample_id, args.output_to_stdout )    
