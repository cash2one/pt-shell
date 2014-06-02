#! /usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. ubs_shell的基本demo, 展示PAPI支持
 History:
     1. 2013/12/8 
"""



import sys
import random
from datetime import datetime, timedelta
from ubs_shell.utils import today
from ubs_shell.job import Job, MJob, PJob

Job.log_file = "tmp.pengtao.shell.log"
MJob.hadoop_home = "/home/work/hadoop-client-stoff"
PJob.papi_java_lib = "/home/work/ubs/lib/papi"



if __name__=='__main__':
            

    B = PJob()
    
    B.input_project  = "udwetl_snapshotclick.event_day=%s.event_hour=04" % today(-3)
    B.input_cols     = "event_baiduid,event_time,event_ip,event_query,event_urlparams"
    B.input_info_file = "b-info-file"    
    B.get_job_info()
    
    
    B.command        = "streaming"
    B.inputs          = "happy"  # input参数是hadoop客户端需要的。但papi输入通过input_info_file获取，input实际不生效
    B.output         = "/tmp/ubs/output/%s" % random.randint(0, 10000)
    B.mapper         = "cat"
    B.reducer        = "cat"
    B.files          = [B.input_info_file]
    B.priority       = "VERY_HIGH"
    B.map_tasks      = 3    
    print B.to_formatted_string()
    # B.run()


    C = PJob()
    C.input_project  = "udwetl_snapshotclick.event_day=%s.event_hour=04" % today(-3)
    C.input_cols     = "event_baiduid,event_time,event_ip,event_query,event_urlparams"
    C.input_info_file = "c-info-file"
    C.get_job_info()
    
    C.command        = "streaming"
    C.inputs         = "happy"  # input参数是hadoop客户端需要的。但papi输入通过input_info_file获取，input实际不生效
    C.output         = "/tmp/ubs/output/%s" % random.randint(0, 10000)
    C.mapper         = "cat"
    C.reducer        = "cat"
    C.files          = [C.input_info_file]
    C.priority       = "VERY_HIGH"
    C.reduce_tasks   = 3
    C.map_tasks      = 10    
    C.input_other_num = 2
    C.input_other_cfgs = [ ("/log/20682/bws_access/%s/0000/szwg-ecomon-hdfs.dmop/2350/tc-www-ipv6pr0.tc.baidu.com_*.log" % today(-3),
                                "org.apache.hadoop.mapred.TextInputFormat"),
                            "/log/20682/browser_log_to_stoff/%s/2100/hz01-sw-dr00.hz01/*.txt" % today(-3) ]
    C.optional       = ["-jobconf udw.mapred.combine.inputformat=true"]
    
    
    
    print C.to_formatted_string()
    # C.run()
    
