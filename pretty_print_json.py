import json
import sys

def main(filename):
    with open(filename,'r') as fp:
        json_str = json.load(fp)
    print(json.dumps(json_str, indent=4, sort_keys=True))

if __name__ == '__main__':
    filename = sys.argv[1]
    main(filename)
