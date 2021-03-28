import os
import sys
import yaml
import subprocess
import argparse
import configparser
from cistrome_logger import cistrome_logger 

class Config():
    configpath = ''
    logger = None
    sys_config = None

    def __init__(self,configpath):
        Config.sys_config = configparser.ConfigParser()
        Config.sys_config.optionxform=str
        Config.sys_config.read(configpath)
        Config.configpath = configpath


def main(configpath,someword=''):

    Config(configpath)
    print(Config.sys_config['paths']['log_file'])
    log = cistrome_logger(f'__{someword}__test:',Config.sys_config['paths']['log_file'])
    Config.logger = log.logger
    print(Config.logger)
    Config.logger.debug('This is a test')


if __name__ == '__main__':
    configpath = './config/rc-fas-harvard.conf'
    main(configpath,someword='parrot')
