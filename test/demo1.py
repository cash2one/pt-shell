#! /usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. ubs_shell中MJob的基本demo。
 History:
     1. 2013/12/8 
"""



import sys
import random
from datetime import datetime, timedelta

from ubs_shell.job import Job, MJob, PJob

Job.log_file = "tmp.pengtao.shell.log"
MJob.hadoop_home = "/home/work/hadoop-client-stoff"

if __name__=='__main__':
            
    # MJob 基本配置
    A = MJob()    
    A.command  = "streaming"
    A.inputs   = "/ps/ubs/pengtao/20131210-ubs-shell-tutorial/input/data1"
    A.output   = "/tmp/ubs/pengtao/output/%s" % random.randint(0, 10000)
    A.mapper   = "cat"
    A.reducer  = "cat"
    A.run()
    
    # MJob完整配置
    
    A = MJob(hadoop_home="/home/work/hadoop-client-rank")
    A.command     = "streamoverhce"
    A.inputs      = [ "/ps/ubs/pengtao/20131210-ubs-shell-tutorial/input/data1",
                      "/ps/ubs/pengtao/20131210-ubs-shell-tutorial/input/data2"]
    A.output      = "/tmp/ubs/pengtao/output/%s" % random.randint(0, 10000)
    A.mapper      = "cat"
    A.reducer     = "cat"
    A.input_format  = "org.apache.hadoop.mapred.TextOutputFormat"
    A.output_format = "org.apache.hadoop.mapred.TextOutputFormat"
    A.map_tasks     =  2
    A.reduce_tasks  =  1
    A.map_capacity  = 10
    A.reduce_capacity = 10
    A.priority    = "VERY_HIGH"
    A.job_name    = "ubs_shell.demo.streaming.simple.cat"
    A.optional    = ["-jobconf mapred.min.split.size=20000000000",
                     "-jobconf abaci.split.remote=true"]
    A.files       = []  # ["a.py", "b.py"]
    
    print A.to_formatted_string()
    A.run()
