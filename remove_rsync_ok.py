# one-off script for removing erroneous rsync_ok files
import os
import sys

def main():

    ids_to_remove = []
    with open(sys.argv[1]) as fp:
        for line in fp.readlines():
            field = line.strip()
            if len(field) > 0:
                ids_to_remove += [field]
   
    for GSMID in ids_to_remove:
        path = f'/n/holyscratch01/xiaoleliu_lab/cistrome_data_collection/runs/{GSMID}/{GSMID}_rsync_ok.txt'
        if os.path.exists(path):
            print(path)
            os.remove(path)


if __name__ == '__main__':
    main()
