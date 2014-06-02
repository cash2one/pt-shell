#! /usr/bin/env python
#coding:utf8

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. 基本测试用例，测试Job/Step的类方法使用
 History:
     1. 2013/12/14 
"""

import os
import random
import unittest
import sys
import logging

from ptshell.job import Job, MJob, PJob

class TestJobClass(unittest.TestCase):
    def setUp(self):
        Job.log_file = "tmp_my_test_file.%s.log" % random.randint(0, 10000)       
    
    def tearDown(self):
        logging.shutdown()
        if os.path.exists(Job.log_file):
            os.remove(Job.log_file)        
    #----------------------------------------------------------------------
    def test_Job_Class(self):
        """"""
        class MyTestJob(Job):
            """The test job blablabla"""
            def config(self, i, j=10):
                self.i = i
                self.j = j
            #----------------------------------------------------------------------
            def run(self):
                """"""
                self.total = i + j
                
 
        
        A = MyTestJob()
        s = A.get_line_help()
        self.assertEqual(s, "MyTestJob.config < i, j=10 >")
        s = A.get_config_str()
        self.assertEquals(s, 
                          "def config(self, i, j=10):\n" + \
                          "    self.i = i\n" + \
                          "    self.j = j\n" + \
                          "\n"
                          )        
        
        test_strings = [
            "test the 1",
            "test the 2",
            "test the 3"
        ]
        A._raw_log(True)
        A._logger.info(test_strings[0])
        A._logger.info(test_strings[1])
        A._logger.info(test_strings[2])
        del A
        self.assertEqual(
            open(Job.log_file).read(), 
            "".join(map(lambda x: x+"\n", test_strings)))
        
             
		
		
        
    
    def test_MJob_Class(self):
        
        class MyTestMJob(MJob):
            """
            """
            def config(self, date):
                self.command  = "streaming"
                self.inputs    = "/ps/ubs/pengtao/20131210-ubs-shell-tutorial/input/data1"
                self.output   = "/tmp/ubs/pengtao/output/%s" % date 
                self.mapper   = "cat"
                self.reducer  = "cat"
                self.map_tasks =  1
                self.reduce_tasks =  2
                self.map_capacity = 10
                self.reduce_capacity = 10
                self.priority = "VERY_HIGH"
                self.job_name = "ubs_shell-demo-streaming-simple-cat"
                self.optional = ["-jobconf mapred.min.split.size=20000000000"]
                
        A = MyTestMJob()
        A.config(12345)
        s = A.get_line_help()
        self.assertEqual(s, "MyTestMJob.config < date >")
        
        # 打印多行命令，参见函数说明
        demo_string = \
"""/home/work/hadoop-client-stoff//hadoop/bin/hadoop streaming  \\
    -input /ps/ubs/pengtao/20131210-ubs-shell-tutorial/input/data1 \\
    -output /tmp/ubs/pengtao/output/12345 \\
    -mapper "cat" \\
    -reducer "cat" \\
    -outputformat org.apache.hadoop.mapred.TextOutputFormat \\
    -jobconf mapred.map.tasks=1 \\
    -jobconf mapred.reduce.tasks=2 \\
    -jobconf mapred.job.map.capacity=10 \\
    -jobconf mapred.job.reduce.capacity=10 \\
    -jobconf mapred.min.split.size=20000000000 \\
    -jobconf mapred.job.name=ubs_shell-demo-streaming-simple-cat \\
    -jobconf mapred.job.priority=VERY_HIGH"""

        cmd_string = A.to_formatted_string()
        self.assertEqual(cmd_string, demo_string)
        del A

    def test_PJob_Class(self):
        
        class MyTestPJob(PJob):
            """
            """
            def config(self, date, rand):
                self.input_project  = "udwetl_snapshotclick.event_day=%s.event_hour=04" % date
                self.input_cols     = "event_baiduid,event_time,event_ip,event_query,event_urlparams"
                self.input_info_file = "b-info-file"
            
                self.command        = "streaming"
                self.inputs          = "happy"  # input参数是hadoop客户端需要的。但papi输入通过input_info_file获取，input实际不生效
                self.output         = "/tmp/ubs/output/%s" % rand
                self.mapper         = "cat"
                self.reducer        = "cat"
                self.files          = [self.input_info_file]
                self.priority       = "VERY_HIGH"
                self.map_tasks      = 3
                
                self.job_name       = "my-papi-job"
                
        A = MyTestPJob()
        A.config("20131204", 5793)
        s = A.get_line_help()
        self.assertEqual(s, "MyTestPJob.config < date, rand >")        
          
        # 打印多行命令，参见函数说明
        # -libjars取值字符串与lib路径和jar文件名， test时不做检测。下面是一个例子。
        # demo_string = """/home/work/hadoop-client-stoff//hadoop/bin/hadoop streaming -libjars /home/work/ubs/lib/papi/commons-httpclient-3.0.1.jar,/home/work/ubs/lib/papi/commons-lang-2.4.jar,/home/work/ubs/lib/papi/commons-logging-1.1.1.jar,/home/work/ubs/lib/papi/dtmetaclient-2.3.1.jar,/home/work/ubs/lib/papi/gson-udw.jar,/home/work/ubs/lib/papi/hadoop-2-core.jar,/home/work/ubs/lib/papi/hive-common-2.2.0.jar,/home/work/ubs/lib/papi/hive-ql-2.2.0.jar,/home/work/ubs/lib/papi/hive-serde-0.7.1.plus.jar,/home/work/ubs/lib/papi/junit-4.4.jar,/home/work/ubs/lib/papi/libthrift-0.8.0.jar,/home/work/ubs/lib/papi/log4j-1.2.14.jar,/home/work/ubs/lib/papi/protobuf-java-2.4.1.jar,/home/work/ubs/lib/papi/slf4j-api-1.5.8.jar,/home/work/ubs/lib/papi/slf4j-log4j12-1.5.8.jar,/home/work/ubs/lib/papi/snappy-0.2.jar,/home/work/ubs/lib/papi/udw-program-api.jar,/home/work/ubs/lib/papi/udwstorage-core-1.0.0.jar,/home/work/ubs/lib/papi/udwstorage-meta-1.0.0.jar,/home/work/ubs/lib/papi/udwstorage-ohio-1.0.0.jar,/home/work/ubs/lib/papi/zookeeper-3.4.3.jar \\
        demo_string = """/home/work/hadoop-client-stoff//hadoop/bin/hadoop streaming -libjars  \\
    -jobconf udw.mapred.input.info=b-info-file \\
    -input happy \\
    -output /tmp/ubs/output/5793 \\
    -mapper "cat" \\
    -reducer "cat" \\
    -inputformat com.baidu.udw.mapred.MultiTableInputFormat \\
    -outputformat org.apache.hadoop.mapred.TextOutputFormat \\
    -jobconf mapred.map.tasks=3 \\
    -jobconf mapred.reduce.tasks=1 \\
    -jobconf mapred.job.map.capacity=300 \\
    -jobconf mapred.job.reduce.capacity=100 \\
    -file b-info-file \\
    -jobconf mapred.job.name=my-papi-job \\
    -jobconf mapred.job.priority=VERY_HIGH"""
        
        cmd_string = A.to_formatted_string()
        # print demo_string
        # print cmd_string
        self.assertEqual(cmd_string, demo_string)
        del A

    def test_PJobOtherInnput_Class(self):
        class MyTestPJob2(PJob):
            """
            """
            def config(self, date, rand):
                
                self.input_project  = "udwetl_snapshotclick.event_day=%s.event_hour=04" % date
                self.input_cols     = "event_baiduid,event_time,event_ip,event_query,event_urlparams"
                self.input_info_file = "c-info-file"
                # self.get_job_info()
                
                self.command        = "streaming"
                self.inputs          = "happy"  # input参数是hadoop客户端需要的。但papi输入通过input_info_file获取，input实际不生效
                self.output         = "/tmp/ubs/output/%s" % rand
                self.mapper         = "cat"
                self.reducer        = "cat"
                self.files          = [self.input_info_file]
                self.priority       = "VERY_HIGH"
                self.reduce_tasks   = 3
                self.map_tasks      = 10    
                self.input_other_num = 2
                self.input_other_cfgs = [ ("/log/20682/bws_access/20131203/0000/szwg-ecomon-hdfs.dmop/2350/tc-www-ipv6pr0.tc.baidu.com_20131203235000.log",
                                            "org.apache.hadoop.mapred.TextInputFormat"),
                                        "/log/20682/browser_log_to_stoff/20131209/2100/hz01-sw-dr00.hz01/201312092155.txt"]
                self.optional       = ["-jobconf udw.mapred.combine.inputformat=true"]
                self.job_name       = "ubs-job-9358"
                
        C = MyTestPJob2()
        C.config("20131204", 7784)
        s = C.get_line_help()
        self.assertEqual(s, "MyTestPJob2.config < date, rand >")  

        # -libjars取值字符串与lib路径和jar文件名， test时不做检测。下面是一个例子。
        # demo_string = """/home/work/hadoop-client-stoff//hadoop/bin/hadoop streaming -libjars /home/work/ubs/lib/papi/commons-httpclient-3.0.1.jar,/home/work/ubs/lib/papi/commons-lang-2.4.jar,/home/work/ubs/lib/papi/commons-logging-1.1.1.jar,/home/work/ubs/lib/papi/dtmetaclient-2.3.1.jar,/home/work/ubs/lib/papi/gson-udw.jar,/home/work/ubs/lib/papi/hadoop-2-core.jar,/home/work/ubs/lib/papi/hive-common-2.2.0.jar,/home/work/ubs/lib/papi/hive-ql-2.2.0.jar,/home/work/ubs/lib/papi/hive-serde-0.7.1.plus.jar,/home/work/ubs/lib/papi/junit-4.4.jar,/home/work/ubs/lib/papi/libthrift-0.8.0.jar,/home/work/ubs/lib/papi/log4j-1.2.14.jar,/home/work/ubs/lib/papi/protobuf-java-2.4.1.jar,/home/work/ubs/lib/papi/slf4j-api-1.5.8.jar,/home/work/ubs/lib/papi/slf4j-log4j12-1.5.8.jar,/home/work/ubs/lib/papi/snappy-0.2.jar,/home/work/ubs/lib/papi/udw-program-api.jar,/home/work/ubs/lib/papi/udwstorage-core-1.0.0.jar,/home/work/ubs/lib/papi/udwstorage-meta-1.0.0.jar,/home/work/ubs/lib/papi/udwstorage-ohio-1.0.0.jar,/home/work/ubs/lib/papi/zookeeper-3.4.3.jar \\
        demo_string = """/home/work/hadoop-client-stoff//hadoop/bin/hadoop streaming -libjars  \\
    -jobconf udw.mapred.input.info=c-info-file \\
    -input happy \\
    -output /tmp/ubs/output/7784 \\
    -mapper "cat" \\
    -reducer "cat" \\
    -inputformat com.baidu.udw.mapred.MultiTableInputFormat \\
    -outputformat org.apache.hadoop.mapred.TextOutputFormat \\
    -jobconf mapred.map.tasks=10 \\
    -jobconf mapred.reduce.tasks=3 \\
    -jobconf mapred.job.map.capacity=300 \\
    -jobconf mapred.job.reduce.capacity=100 \\
    -file c-info-file \\
    -jobconf udw.mapred.combine.inputformat=true \\
    -jobconf udw.mapred.input.other.num=2 \\
    -jobconf udw.mapred.input.other.file.0=/log/20682/bws_access/20131203/0000/szwg-ecomon-hdfs.dmop/2350/tc-www-ipv6pr0.tc.baidu.com_20131203235000.log \\
    -jobconf udw.mapred.input.other.format.0=org.apache.hadoop.mapred.TextInputFormat \\
    -jobconf udw.mapred.input.other.file.1=/log/20682/browser_log_to_stoff/20131209/2100/hz01-sw-dr00.hz01/201312092155.txt \\
    -jobconf udw.mapred.input.other.format.1=org.apache.hadoop.mapred.TextInputFormat \\
    -jobconf mapred.job.name=ubs-job-9358 \\
    -jobconf mapred.job.priority=VERY_HIGH"""
        cmd_string = C.to_formatted_string()
        self.assertEqual(cmd_string, demo_string)
        del C

        
        
if __name__ == "__main__":
    s1 = unittest.TestLoader().loadTestsFromTestCase(TestJobClass)
    suite = unittest.TestSuite([s1])  # [s1 s2 s3]
    unittest.TextTestRunner(verbosity=2).run(suite)
        
