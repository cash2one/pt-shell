#! /usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. 测试ubs_shell的基本demo，测试实例方式的使用。
 History:
     1. 2013/12/11
"""

    
import unittest
import os
import random
import logging

from ptshell.job import Job, MJob, PJob

class TestJobInstance(unittest.TestCase):
    def setUp(self):
        Job.log_file = "tmp_my_test_file.%s.log" % random.randint(0, 10000)
    
    def tearDown(self):
        logging.shutdown()
        if os.path.exists(Job.log_file):
            os.remove(Job.log_file)	        
            

    #----------------------------------------------------------------------
    def test_Job_Ins(self):
        """"""
        class MyJob(Job):
            def config(self, var):
                return var
        
        test_strings = [
            "test the 1",
            "test the 2",
            "test the 3"
        ]
        
       
        A = MyJob()
        s = A.get_line_help()
        self.assertEqual(s, "MyJob.config < var >")
        s = A.get_config_str()
        self.assertEquals(s, 
                          "def config(self, var):\n    return var\n\n")
        
        A._raw_log(True)
        A._logger.info(test_strings[0])
        A._logger.info(test_strings[1])
        A._logger.info(test_strings[2])
        del A
        self.assertEqual(
            open(Job.log_file).read(), 
            "".join(map(lambda x: x+"\n", test_strings)))
        
	
		
        
    
    def test_MJob_Ins(self):
        
        A = MJob()
        A.command  = "streaming"
        A.inputs    = "/ps/ubs/pengtao/20131210-ubs-shell-tutorial/input/data1"
        A.output   = "/tmp/ubs/pengtao/output/%s" % 12345
        A.mapper   = "cat"
        A.reducer  = "cat"
        A.map_tasks =  1
        A.reduce_tasks =  2
        A.map_capacity = 10
        A.reduce_capacity = 10
        A.priority = "VERY_HIGH"
        A.job_name = "ubs_shell-demo-streaming-simple-cat"
        A.optional = ["-jobconf mapred.min.split.size=20000000000"]
        
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
        # print cmd_string
        self.assertEqual(cmd_string, demo_string)
        del A

    def test_PJob_Ins(self):
        
        B = PJob()
        B.input_project  = "udwetl_snapshotclick.event_day=%s.event_hour=04"  % "20131208"
        B.input_cols     = "event_baiduid,event_time,event_ip,event_query,event_urlparams"
        B.input_info_file = "b-info-file"
    
        B.command        = "streaming"
        B.inputs         = "happy"  
        B.output         = "/tmp/ubs/output/%s" % 5793
        B.mapper         = "cat"
        B.reducer        = "cat"
        B.files          = [B.input_info_file]
        B.priority       = "VERY_HIGH"
        B.mapper_tasks   = 3
        B.job_name       = "my-papi-job"
        
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
    -jobconf mapred.map.tasks=1 \\
    -jobconf mapred.reduce.tasks=1 \\
    -jobconf mapred.job.map.capacity=300 \\
    -jobconf mapred.job.reduce.capacity=100 \\
    -file b-info-file \\
    -jobconf mapred.job.name=my-papi-job \\
    -jobconf mapred.job.priority=VERY_HIGH"""
        
        cmd_string = B.to_formatted_string()
        # print cmd_string
        self.assertEqual(cmd_string, demo_string)
        del B

    def test_PJobOtherInnput_Ins(self):

        C = PJob()
        C.input_project  = "udwetl_snapshotclick.event_day=%s.event_hour=04" % 20131208
        C.input_cols     = "event_baiduid,event_time,event_ip,event_query,event_urlparams"
        C.input_info_file = "c-info-file"
        
	    
        C.command        = "streaming"
        C.inputs         = "happy"  # input参数是hadoop客户端需要的。但papi输入通过input_info_file获取，input实际不生效
        C.output         = "/tmp/ubs/output/%s" % 7784
        C.mapper         = "cat"
        C.reducer        = "cat"
        C.files          = [C.input_info_file]
        C.priority       = "VERY_HIGH"
        C.reduce_tasks   = 3
        C.map_tasks      = 10    
        C.input_other_num = 2
        C.input_other_cfgs = [ ("/log/20682/bws_access/20131203/0000/szwg-ecomon-hdfs.dmop/2350/tc-www-ipv6pr0.tc.baidu.com_20131203235000.log",
                                "org.apache.hadoop.mapred.TextInputFormat"),
                               "/log/20682/browser_log_to_stoff/20131209/2100/hz01-sw-dr00.hz01/201312092155.txt"]
        C.optional       = ["-jobconf udw.mapred.combine.inputformat=true"]
        C.job_name       = "ubs-job-9358"

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
        # print cmd_string
        self.assertEqual(cmd_string, demo_string)
        del C
    
        
        
if __name__ == "__main__":
    s1 = unittest.TestLoader().loadTestsFromTestCase(TestJobInstance)
    suite = unittest.TestSuite([s1])  # [s1 s2 s3]
    unittest.TextTestRunner(verbosity=2).run(suite)
        
