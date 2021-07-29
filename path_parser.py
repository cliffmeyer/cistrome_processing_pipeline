"""
This file contains functions used to check each file in a directory structure.
The structure and check type for each file are specified in a yaml file.
See check_chips.py for its application.
"""

import os
import sys
import yaml


def dict_to_dir(folders, path=str(), path_list=[], check_register={} ):
    """Convert dictionary, list structure into paths
    - folder1:
       - file1:
          check_file1
       - file2:
          check_file2
    - folder2:
       - file3
    """

    if isinstance(folders, dict):
        for key, val in folders.items():
            path_list += [(os.path.join(path, key))]
            dict_to_dir(val, path=os.path.join(path,str(key)), path_list=path_list, check_register=check_register)

    elif isinstance(folders, list):
        for elem in folders:
            if isinstance(elem, dict):
                dict_to_dir(elem, path=path, path_list=path_list, check_register=check_register )
            else:
                path_list += [(os.path.join(path, elem))]

    elif isinstance( folders, str):
        check_function = folders
        check_register[path] = check_function  

    return {'path_list':path_list,'check_register':check_register}


def replace_sample_name(path_list, check_register={}, place_holder='', sample_id=''):
    new_path_list = []
    new_check_register = {}        
    for old_path in path_list:
        new_path = old_path.replace(place_holder,sample_id)
        new_path_list += [new_path]
        if old_path in check_register.keys():
            new_check_register[new_path] = check_register[old_path]
    return {'path_list':new_path_list, 'check_register':new_check_register}


def match_filename_pattern(path_list):
    head_path_list = []
    for path in path_list:
        head,tail = os.path.split(path)
        head_path_list += [head]

    head_path_set = set(head_path_list)

    base_path_list = []
    for path in head_path_set:
        base_path_list += os.listdir(path)

    base_path_set = set(base_path_list)
    match_list = [ re.match(path,elem) for elem in base_path_set ]


def paths_from_yaml( config_file, path_root=None, sample_id=None):
    with open(config_file,"r") as fp:
        folder_struct = yaml.load(fp,Loader=yaml.Loader)
    path_and_check = dict_to_dir(folders=folder_struct, path=path_root)
    path_list = path_and_check['path_list']
    check_register = path_and_check['check_register']
    if sample_id != None:
        path_and_check = replace_sample_name( path_list, check_register=check_register, place_holder='SAMPLEID', sample_id=sample_id )
    path_list = path_and_check['path_list']
    check_register = path_and_check['check_register']
    return path_and_check


def main():
    """
    if len(sys.argv) == 1:
        sys.stderr.write("Too few arguments\n")
        sys.exit(1)
    elif len(sys.argv) == 2:
        config_file = os.path.abspath(sys.argv[1])
    else:
        sys.stderr.write("Unexpected argument {}\n".format(sys.argv[2:]))
        sys.exit(1)
    """
    path_root = './unit_test_dirs'
    sample_id = 'Mickey_M'
    config_file_path = './chips_test_dirs/test_layout.yaml' 

    path_and_check = paths_from_yaml( config_file_path, path_root=path_root, sample_id=sample_id)
    path_list = path_and_check['path_list']
    check_register = path_and_check['check_register']
    #for elem in path_list:
    #    print(elem)

  
if __name__ == "__main__":
    main()

