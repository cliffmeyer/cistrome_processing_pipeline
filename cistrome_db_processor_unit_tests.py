import unittest
import os, sys
import urllib.request
import unittest
import sra_download as sra
import path_parser
import filename_pattern_regex
import requests_from_cistromeDB
import scheduler
import json


class TestRequests_from_cistromeDB(unittest.TestCase):


    def setUp(self):
        sample_queue_path = './chips_test_dirs/cistrome_pipeline_test.conf'
        sample_json_file  = './chips_test_dirs/test_collection.json'

        self.sample_queue = requests_from_cistromeDB.SampleQueue(sample_queue_path)
        with open(sample_json_file,'r') as fp:
            self.sample_queue.requested_samples = json.load(fp)
            print(self.sample_queue.requested_samples)
        self.sample_queue.read_local_queue()


    def tearDown(self):
        del self.sample_queue


    def test_set_job_info(self):
        self.sample_queue.set_sample_info(sample_id='TEST0000000001',info_key='sra',info_val={'12345':'COMPLETE'})
        self.sample_queue.set_sample_info(sample_id='TEST0000000001',info_key='chips',info_val={'123456':'RUNNING'})
        print('local sample queue:', self.sample_queue.local_samples)
        self.assertEqual( self.sample_queue.local_samples['samples_to_be_processed']['TEST0000000001']['CHIPS'], {'123456':'RUNNING'} )


    def test_sample_restart_count(self):
        self.sample_queue.increment_sample_restart_count(sample_id='TEST0000000001')
        self.sample_queue.increment_sample_restart_count(sample_id='TEST0000000001')
        self.assertEqual( self.sample_queue.get_sample_restart_count(sample_id='TEST0000000001'), 2)


    def test_write_local_queue(self):
        self.sample_queue.set_sample_info(sample_id='TEST0000000001',info_key='sra',info_val={'12344':'RUNNING'})
        self.sample_queue.write_local_queue()


    def test_sample_fail_count(self):
        self.sample_queue.set_sample_info(sample_id='TEST0000000001',info_key='chips',info_val={'0001':'FAILED'})
        self.sample_queue.set_sample_info(sample_id='TEST0000000001',info_key='chips',info_val={'0002':'FAILED'})
        self.sample_queue.set_sample_info(sample_id='TEST0000000001',info_key='chips',info_val={'0003':'FAILED'})
        print(self.sample_queue.local_samples)
        self.assertEqual( self.sample_queue.get_sample_fail_count(sample_id='TEST0000000001',info_key='chips'), 3 )




class TestSRAMethods(unittest.TestCase):


    def test_paired_end_layout_type(self):
        #gsm = 'GSM4443858'
        gsm = 'GSM4064182'
        gsm_html = sra.get_gsm_html(gsm)
        srx_html = sra.get_srx_html(gsm_html)
        layout_type = sra.get_layout_type(srx_html,gsm)
        print(layout_type)
        self.assertEqual(layout_type,'PAIRED')


    def test_single_layout_type(self):
        gsm = 'GSM4565966'
        gsm_html = sra.get_gsm_html(gsm)
        srx_html = sra.get_srx_html(gsm_html)
        layout_type = sra.get_layout_type(srx_html,gsm)
        print(layout_type)
        self.assertEqual(layout_type,'SINGLE')


    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')


    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())


    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)


class TestChipsTest(unittest.TestCase):

    def setUp(self):
        path_root = './'
        sample_id = 'James_Bond_007'
        config_file_path = './chips_test_dirs/layout_test.yaml'
        path_and_check = path_parser.paths_from_yaml(config_file_path,path_root=path_root,sample_id=sample_id)
        self.path_list = path_and_check['path_list'] 
        self.check_register = path_and_check['check_register'] 


    def test_paths_from_yaml(self):
        self.assertTrue('./chips_test_dirs/file1.txt' in self.path_list)  
        self.assertTrue('./chips_test_dirs/file2.txt' in self.path_list)  
        self.assertTrue('./chips_test_dirs/level1/file(\w+).txt' in self.path_list)  


    def test_path_regex(self):
        path_dict = filename_pattern_regex.match_filename_pattern(self.path_list)
        self.assertTrue(path_dict['./chips_test_dirs/file1.txt'])  
        self.assertTrue(path_dict['./chips_test_dirs/file2.txt'])  
        self.assertTrue(path_dict['./chips_test_dirs/level1/file(\w+).txt'])


    def test_file_list_in_path_regex(self):
        path_dict = filename_pattern_regex.match_file_list_patterns(self.path_list)
        for key,val in path_dict.items():
            print(key,val)
        self.assertTrue(len(path_dict['./chips_test_dirs/level1/file(\w+).txt'])==2)


class TestProcessStatusFile(unittest.TestCase):
    def setUp(self):
        configpath = './chips_test_dirs/cistrome_pipeline_test.conf' 
        scheduler.Config(configpath)

    def test_write_process_status_file(self):
        scheduler.write_process_status_file( external_id='GSM4565966', external_id_type='GEO', process_status='COMPLETE')
        ref_file = './chips_test_dirs/runs/GSM4565966/cistrome/datasetGSM4565966_status_ref.json'
        with open(ref_file,'r') as fp:
            process_status_ref = json.load(fp)
        test_file = './chips_test_dirs/runs/GSM4565966/cistrome/datasetGSM4565966_status.json'
        with open(test_file,'r') as fp:
            process_status = json.load(fp)
        self.assertTrue( process_status == process_status_ref )


if __name__ == '__main__':
    unittest.main()
