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
    def read_config(cls,config_filename):
        conf = configparser.ConfigParser()
        conf.optionxform=str
        conf.read(config_filename)
        cls.user = conf['backup_server']['backup_server_user'] 
        cls.domain = conf['backup_server']['backup_server_domain']
        cls.remote_login = f'{cls.user}@{cls.domain}'
        cls.path = conf['backup_server']['backup_server_path']
        cls.server = conf['backup_server']['backup_server_name']
        cls.key_file = conf['backup_server']['backup_server_google_auth_file']
        cls.password_file = conf['backup_server']['backup_server_password_file']
        cls.data_collection_runs = conf['paths']['data_collection_runs']
        cls.cistrome_result = conf['paths']['cistrome_result']
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


def rsync_to_google_authenticated_server(path):
    validator = google_auth.Validator(Config.key_file)
    password = Config.password()
    # args = ['-avPL','--progress',',path,f'{Config.remote_login}:{Config.path}/','2>/dev/null']
    args = ['-avPL','--progress',path,f'{Config.remote_login}:{Config.path}/']

    if DEBUG:
        print(args)

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


def transfer_to_backup_server(sample_id,attempts=5):
    sample_path = os.path.join( Config.data_collection_runs, sample_id, Config.cistrome_result, f'dataset{sample_id}')
    
    for i in range(attempts):
        if DEBUG == True:
            print(sample_path)
        status = rsync_to_google_authenticated_server(sample_path)
        if status == True:
            break

    return status


def write_backup_ok_file(sample_id):
    sample_path = os.path.join( Config.data_collection_runs, sample_id )

    backup_ok = os.path.join( sample_path,f'{sample_id}_backup_ok.txt' )
    subprocess.call(f'touch {backup_ok}',shell=True)
 

def main():
    try:
        parser = argparse.ArgumentParser(description="""Transfer chips result to backup server""")
        parser.add_argument( '-c', dest='config', type=str, required=True, help='the path of config file')
        parser.add_argument( '-i', dest='samplename', type=str, required=True, help='name of sample that needs to be transferred')
        parser.add_argument( '-a', dest='attempts', type=int, default=5, required=False, help='number of transfer attempts')
        args = parser.parse_args()
        Config.read_config(args.config)

        backup_succeeded = transfer_to_backup_server(args.samplename,args.attempts)
        if backup_succeeded:
            write_backup_ok_file(args.samplename)
 
    except KeyboardInterrupt:
        sys.stderr.write("User interrupted me!\n")
        sys.exit(0)


if __name__ == '__main__':
    main()
