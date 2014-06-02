#! /usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. ubs_shell的类使用方式。
 History:
     1. 2013/12/11
"""



import sys
import random
from datetime import datetime, timedelta


from ubs_shell.job import Job, MJob
from ubs_shell.utils import hadoop_cmd

MJob.hadoop_home = "/home/work/hadoop-client-stoff"

class SimpleCatDemoData(MJob):
    """
    mapper将输入cat， reducer再cat输出
    """
    def config(self, fn, rand):
        self.command  = "streaming"
        self.inputs    = "/ps/ubs/pengtao/20131210-ubs-shell-tutorial/input/%s" % fn
        self.output   = "/tmp/ubs/pengtao/output/%s" % rand
        self.mapper   = "cat"
        self.reducer  = "cat"

class SimpleGrepDemoData(MJob):
    """
    mapper进行grep，reducer直接cat输出。
    """
    def config(self, date):
        self.command  = "streaming"
        self.inputs    = "/log/20682/newcookiesort/%s/0000/szwg-*-hdfs.dmop/part-00000" % date
        self.output   = "/tmp/ubs/pengtao/output/%s" % random.randint(0, 10000)
        self.mapper   = "grep tudou; cd ."
        self.reducer  = "cat"    
        self.map_tasks =  3
        self.reduce_tasks =  1
        self.map_capacity = 10
        self.reduce_capacity = 10
        self.priority = "VERY_HIGH"
        self.job_name = "ubs_shell-demo-mapper-grep-reducer-cat"
        

class DownloadHdfsOutput(Job):
    """
    local job，下载第一个MJob输出的文件
    """

    def config(self, rand):
        self.hdfs_path = "/tmp/ubs/pengtao/output/%s" % rand
        self.local_path = "./local_bak"
        
    def run(self):
        """"""
        cmds = ["dfs", "-get", self.hdfs_path, self.local_path]
        hadoop_cmd(cmds, hadoop=MJob.hadoop_home)
        

if __name__=='__main__':
    
    fn = "data1"
    rand = random.randint(0, 10000)
    
    A = SimpleCatDemoData()
    A.config(fn, rand)
    print A.to_formatted_string()
    A.run()
    
    B = DownloadHdfsOutput()
    B.config(rand)
    B.run()
    
    C = SimpleGrepDemoData()
    C.config("20131202")
    print C.to_formatted_string()
    C.run()
    
    
    
    
