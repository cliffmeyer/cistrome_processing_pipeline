import pyBigWig


class bigwig():
    def __init__(self,filename):
        self.filename = filename
        self.bw = None

    def open_file(self):
        self.bw = pyBigWig.open(self.filename)

    def check_open(self):
        return(not(self.bw == None))

    def check_chroms(self):
        print(self.bw.chroms())

    def check_header(self):
        print(self.bw.header())


def main(filename):
    bw_test = bigwig(filename)
    bw_test.open_file()
    bw_test.check_open()
    bw_test.check_chroms()
    bw_test.check_header()


if __name__ == '__main__':
    filename = '/n/holyscratch01/xiaoleliu_lab/cistrome_data_collection/runs/GSM817182/cistrome/GSM817182/GSM817182_treat.bw'
    main(filename)
