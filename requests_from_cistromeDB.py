import urllib.request
import urllib.parse
import json
import configparser
import argparse
import os


class SystemConfig():
    def __init__(self,config_filename):
        self.config_filename = config_filename
        self.read_config()

    def read_config(self):
        self.config = configparser.ConfigParser()
        self.config.optionxform=str
        self.config.read(self.config_filename)


class SampleQueue():

    def __init__(self,system_config_filename):
        tmp_conf = SystemConfig(system_config_filename)
        tmp_conf.read_config()
        self.sys_conf = tmp_conf.config


    def download_cistromedb_json(self):
        baseurl = self.sys_conf['home_server']['domain']
        baseurl = f'http://{baseurl}/'
        query = self.sys_conf['home_server']['requested_sample_file']
        url = urllib.parse.quote(baseurl+query,safe=':)(][&?=/')
        #print(url)
        request = urllib.request.Request(url)

        with urllib.request.urlopen(request) as response:
            docString = response.read()
            docString = docString.decode('UTF8')
            self.requested_samples = json.loads(docString)
            #print(self.requested_samples)


    def read_local_queue(self):
         local_queue_file = self.sys_conf['home_server']['local_queue_file']
         try:
             with open(local_queue_file) as fp:
                self.local_samples = json.load(fp)
                #print(self.local_samples)
         except:
             self.local_samples = {}


    def get_local_queue(self):
        assert( isinstance(self.local_samples,dict) )
        if 'samples_to_be_processed' not in self.local_samples:
            self.local_samples['samples_to_be_processed'] = {}
        return self.local_samples['samples_to_be_processed']
 

    # set dictionary of run events to empty
    def clear_sample_info(self,sample_id='',info_key=''):
        local_queue = self.get_local_queue()
        info_key = info_key.upper()
        if sample_id in local_queue:
            sample = self.local_samples['samples_to_be_processed'][sample_id]
            sample[info_key] = {}


    def set_sample_info(self,sample_id='',info_key='',info_val={}):
        local_queue = self.get_local_queue()
        info_key = info_key.upper()
        if sample_id in local_queue:
            sample = self.local_samples['samples_to_be_processed'][sample_id]

            if not info_key in sample:
                sample[info_key] = {}

            if not isinstance(sample[info_key],dict):
                sample[info_key] = {}

            for key,val in info_val.items():               
                sample[info_key][key] = val


    def increment_sample_restart_count(self,sample_id=''):
        local_queue = self.get_local_queue()
        if sample_id in local_queue:
            sample = self.local_samples['samples_to_be_processed'][sample_id]
            if not 'RESTARTS' in sample:
                sample['RESTARTS'] = 1
            else:
                sample['RESTARTS'] += 1


    def get_sample_restart_count(self,sample_id=''):
        local_queue = self.get_local_queue()
        restart_count = 0
        if sample_id in local_queue:
            sample = self.local_samples['samples_to_be_processed'][sample_id]
            if 'RESTARTS' in sample:
                restart_count = sample['RESTARTS']
        return restart_count


    def get_sample_fail_count(self,sample_id='',info_key=''):
        local_queue = self.get_local_queue()
        info_key = info_key.upper()

        if sample_id in local_queue:
            sample = self.local_samples['samples_to_be_processed'][sample_id]
        else:
            sample = None

        count = 0
        if (sample and (info_key in sample) and
            isinstance(sample[info_key],dict)):

            for key,val in sample[info_key].items():
                if val == "FAILED":          
                    count += 1
        return count


    # TODO extract function to look for status
    def get_sample_status_count(self,sample_id='',info_key='',status=''):
        local_queue = self.get_local_queue()
        info_key = info_key.upper()

        if sample_id in local_queue:
            sample = self.local_samples['samples_to_be_processed'][sample_id]
        else:
            sample = None

        count = 0
        if (sample and (info_key in sample) and
            isinstance(sample[info_key],dict)):

            for key,val in sample[info_key].items():
                if status == '':
                    count += 1
                elif val == status: 
                    count += 1
        return count


    def write_local_queue(self):             
        local_queue_file = self.sys_conf['home_server']['local_queue_file']
        with open(local_queue_file,'w') as fp:
            json.dump(self.local_samples,fp)

 
    def update_local_queue(self):
        self.read_local_queue()
        self.download_cistromedb_json()
        #print(self.requested_samples)
        if 'samples_to_be_processed' in self.requested_samples:
            updated_sample_queue = self.requested_samples 
        # combine dictionaries of samples in local queue and requests
        if 'samples_to_be_processed' in self.local_samples:
            # note: local sample parameters overwrite requested 
            updated_sample_queue['samples_to_be_processed'] = { **self.requested_samples['samples_to_be_processed'], **self.local_samples['samples_to_be_processed'] } 
 
        self.local_samples['samples_to_be_processed'] =  updated_sample_queue['samples_to_be_processed'] 
        self.write_local_queue()


if __name__ == "__main__":
 
    try:
        parser = argparse.ArgumentParser(description="""Get sample processing requests from CistromeDB""")
        parser.add_argument( '-c', dest='configpath', type=str,  required=True, help='the path of config file')
        args = parser.parse_args()
        sample_queue = SampleQueue(args.configpath)
        sample_queue.update_local_queue()
    except KeyboardInterrupt:
        sys.stderr.write("User interrupted me!\n")

