#!/usr/bin/env python
import datetime
import argparse
import pexpect
import subprocess
import json
import os,sys,time
import google_auth
import configparser 

DEBUG = True

class Config:

    @classmethod
    def read_config(cls,config_filename,server):
        conf = configparser.ConfigParser()
        conf.optionxform=str
        conf.read(config_filename)
        cls.user         = conf[server]['user'] 
        cls.domain       = conf[server]['domain']
        cls.remote_login = f'{cls.user}@{cls.domain}'

        if 'port' in conf[server]:        
            cls.port = conf[server]['port']
        else:
            cls.port = ''

        cls.path      = conf[server]['path']
        cls.server    = conf[server]['name']
        cls.auth_mode = conf[server]['authentication_mode'] # password or key

        if cls.auth_mode == 'password':
            cls.password_file = conf[server]['password_file']
        elif cls.auth_mode == 'password_google':
            cls.key_file = conf[server]['google_auth_file']
            cls.password_file = conf[server]['password_file']
        elif cls.auth_mode == 'google_cloud':
            pass
 
        cls.data_collection_runs = conf['paths']['data_collection_runs']
        cls.cistrome_result      = conf['paths']['cistrome_result']
        cls.timeout = 60*60


    @classmethod
    def password(cls):
        with open(cls.password_file,'r') as fp:
            password_lookup = json.load(fp) 

        if cls.server in password_lookup:
            password = password_lookup[cls.server]
        else:
            password = ''

        return password 


def rsync_to_key_auth_server(path):
    # facilitates special port
    if Config.port != '':
        port_str = f'-e "/usr/bin/ssh -p {Config.port}"'
    else: 
        port_str = ''
 
    if os.path.isfile(path) == False: 
        stdout_path = os.path.join(path,f'{Config.server}_rsync_stdout.txt')
        stderr_path = os.path.join(path,f'{Config.server}_rsync_stderr.txt')
    else:
        path_head,path_tail = os.path.split(path) 
        stdout_path = os.path.join(path_head,f'{Config.server}_rsync_stdout.txt')
        stderr_path = os.path.join(path_head,f'{Config.server}_rsync_stderr.txt')

    try:
        transfer_cmd  = f'rsync -aPL {port_str} {path} {Config.remote_login}:{Config.path}/'
        fp_stdout = open(stdout_path,'w')
        fp_stderr = open(stderr_path,'w')
        subprocess.call(transfer_cmd,shell=True,stdout=fp_stdout,stderr=fp_stderr)
        time.sleep(5)
        fp_stdout.close()
        fp_stderr.close()
        return True
    except:
        return False


def rsync_to_passwd_auth_server(path):
    password = Config.password()
    args = ['-avPL','--progress',path,f'{Config.remote_login}:{Config.path}/']

    if DEBUG:
        print(args)
        return

    try:
        # For Python 3 compatibility spawnu is used, importing unicode_literals. 
        # spawnu accepts Unicode input and unicode_literals makes all string literals in this script Unicode by default.
        child = pexpect.spawnu("rsync", args, timeout=Config.timeout)
        child.expect('.*password:.*',timeout=Config.timeout)
        response = f'{password}'
        child.sendline(response)
        time.sleep(5)
        child.expect(pexpect.EOF)
        return True
    except pexpect.EOF:
        return False
    except pexpect.TIMEOUT:
        return False


def rsync_to_google_authenticated_server(path):
    validator = google_auth.Validator(Config.key_file)
    password = Config.password()
    # args = ['-avPL','--progress',',path,f'{Config.remote_login}:{Config.path}/','2>/dev/null']
    args = ['-avPL','--progress',path,f'{Config.remote_login}:{Config.path}/']

    if DEBUG:
        print(args)
        return

    try:
        # For Python 3 compatibility spawnu is used, importing unicode_literals. 
        # spawnu accepts Unicode input and unicode_literals makes all string literals in this script Unicode by default.
        child = pexpect.spawnu("rsync", args, timeout=Config.timeout)
        child.expect('.*Password & verification code:.*',timeout=Config.timeout)
        code = validator.get_totp_token(Config.server)
        response = f'{password}{code}'
        #print(response)
        child.sendline(response)
        time.sleep(1)
        child.expect('.* password:.*',timeout=Config.timeout)
        response = f'{password}'
        #print(response)
        child.sendline(response)
        #print(child.before)
        time.sleep(1)
        child.expect(pexpect.EOF)
        return True
    except pexpect.EOF:
        return False
    except pexpect.TIMEOUT:
        return False


def copy_to_google_cloud_server(path, sample_id_stub=''):

    if os.path.isfile(path) == False: 
        stdout_path = os.path.join(path,f'{Config.server}_rsync_stdout.txt')
        stderr_path = os.path.join(path,f'{Config.server}_rsync_stderr.txt')
    else:
        path_head,path_tail = os.path.split(path) 
        stdout_path = os.path.join(path_head,f'{Config.server}_rsync_stdout.txt')
        stderr_path = os.path.join(path_head,f'{Config.server}_rsync_stderr.txt')

    remote_path = os.path.join( Config.path, sample_id_stub )

    try:
        transfer_cmd = f'gsutil rsync -rc {path} {remote_path}/'
        fp_stdout = open(stdout_path,'w')
        fp_stderr = open(stderr_path,'w')
        subprocess.call(transfer_cmd,shell=True,stdout=fp_stdout,stderr=fp_stderr)
        time.sleep(1)
        fp_stdout.close()
        fp_stderr.close()
        return True
    except:
        return False
 

def transfer_to_server(sample_id,attempts=5):
    sample_path = os.path.join( Config.data_collection_runs, sample_id, Config.cistrome_result, f'dataset{sample_id}')
    sample_md5_path = os.path.join( Config.data_collection_runs, sample_id, Config.cistrome_result, f'{sample_id}.md5')
    sample_id_stub = sample_id[:-3]
    auth_mode = Config.auth_mode
 
    for i in range(attempts):
        if DEBUG == True:
            print(sample_path)

        if auth_mode == 'password':
            status     = rsync_to_passwd_auth_server(sample_path)
            md5_status = rsync_to_passwd_auth_server(sample_md5_path)
        elif auth_mode == 'key':
            status     = rsync_to_key_auth_server(sample_path)
            md5_status = rsync_to_key_auth_server(sample_md5_path)
        elif auth_mode == 'password_google':
            status     = rsync_to_google_authenticated_server(sample_path)
            md5_status = rsync_to_google_authenticated_server(sample_md5_path)
        elif auth_mode == 'google_cloud':
            status     = copy_to_google_cloud_server(sample_path, sample_id_stub = sample_id_stub )
            md5_status = copy_to_google_cloud_server(sample_md5_path, sample_id_stub = sample_id_stub )
        else:
            status = False
            md5_status = False

        if status == True and md5_status == True:
            break

    return status


def write_transfer_ok_file(sample_id,server=''):
    sample_path = os.path.join( Config.data_collection_runs, sample_id )
    if server == 'backup_server':
        transfer_ok = os.path.join( sample_path,f'{sample_id}_backup_ok.txt' )
    else:
        transfer_ok = os.path.join( sample_path,f'{sample_id}_rsync_ok.txt' )
    subprocess.call(f'touch {transfer_ok}',shell=True)
 

def main():
    try:
        parser = argparse.ArgumentParser(description="""Transfer chips result to data server""")
        parser.add_argument( '-c', dest='config', type=str, required=True, help='the path of config file')
        parser.add_argument( '-a', dest='attempts', type=int, default=5, required=False, help='number of transfer attempts')
        parser.add_argument( '-i', dest='samplename', type=str, required=True, help='name of sample that needs to be transferred')
        parser.add_argument( '-s', dest='server', choices=['data_server','home_server','backup_server'], required=True, default='data_server',help='where to send files to')

        args = parser.parse_args()
        Config.read_config(args.config,args.server)

        transfer_succeeded = transfer_to_server(args.samplename,attempts=args.attempts)
        if transfer_succeeded:
            write_transfer_ok_file(args.samplename,server=args.server)
 
    except KeyboardInterrupt:
        sys.stderr.write("User interrupted me!\n")
        sys.exit(0)


if __name__ == '__main__':
    main()
