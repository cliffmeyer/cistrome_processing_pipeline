import os
import re


def get_non_terminal_paths(path_list):
    """path list includes paths with folders and file name patterns
    return folders in this list without the files"""
    head_path_list = []
    for path in path_list:
        head,tail = os.path.split(path)
        head_path_list += [head]

    head_path_set = set(head_path_list)
    return head_path_set


def get_files_in_folder_list(path_set):
    all_file_path_list = []
    for path in path_set:
        if os.path.exists(path):
            all_file_path_list += [os.path.join(path,elem) for elem in os.listdir(path)]
    all_file_path_set = set(all_file_path_list)
    return all_file_path_set


def is_file_pattern_in_file_path_set(file_pattern,file_path_set):
    """file name pattern matching"""
    is_match = False
    compiled_file_pattern = re.compile(f'{file_pattern}$')
    for elem in file_path_set:
        is_match = max( is_match, not(type(compiled_file_pattern.match(elem)) == type(None)) )
        if is_match == True:
            break
    return is_match


def match_filename_pattern(path_list):
    """folders are exact names but file names may be patterns"""
    head_path_set = get_non_terminal_paths(path_list)
    all_file_path_set = get_files_in_folder_list(head_path_set)
    path_dict = {}
    for file_pattern in path_list:
        path_dict[file_pattern] = is_file_pattern_in_file_path_set(file_pattern,all_file_path_set)
    return path_dict


def files_matching_pattern_in_file_path_set(file_pattern,file_path_set):
    """file name pattern matching"""
    file_list = []
    compiled_file_pattern = re.compile(f'{file_pattern}$') # match end of string
    for elem in file_path_set:
        is_match = not(type(compiled_file_pattern.match(elem)) == type(None))
        if is_match == True:
            file_list += [elem]
    return file_list
 

def match_file_list_patterns(path_list):
    """folders are exact names but file names may be patterns"""
    head_path_set = get_non_terminal_paths(path_list)
    all_file_path_set = get_files_in_folder_list(head_path_set)
    path_dict = {}
    for file_pattern in path_list:
        path_dict[file_pattern] = files_matching_pattern_in_file_path_set(file_pattern,all_file_path_set)
    return path_dict

 
def main():
    path_list = ['/Users/len/Projects/Cistrome_GEO_parser/tmp/test_path/b.txt']
    match_filename_pattern(path_list)

if __name__ == '__main__':
    main()
