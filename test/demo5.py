#!/usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. PAPI的类方式demo
 History:
     1. 2013/12/24 created
"""


import sys
import random

from ubs_shell.job import Job, MJob, PJob
from ubs_shell.router import router


Job.log_file = "tmp.my.pipeline.log"
MJob.hadoop_home = "/home/work/hadoop-client-stoff"
PJob.papi_java_lib = "/home/work/ubs/lib/papi"
PJob.udw_meta_server = "db-dt-udw10.db01:2183,jx-dt-udw06.jx:2183,jx-dt-udw07.jx:2183,tc-dt-udw01.tc:2183,tc-dt-udw02.tc:2183/biglog/metadebug"
PJob.udw_user = "InternalUser"

        
@router("job1")
class SimplePAPIJob(PJob):
    """
    
    """
    def config(self, date):
        self.input_project  = "udwetl_snapshotclick.event_day=%s.event_hour=04" % date
        self.input_cols     = "event_baiduid,event_time,event_ip,event_query,event_urlparams"
        self.input_info_file = "tmp-info-file"
        
        self.command        = "streaming"
        self.inputs         = "happy"  # make hadoop happy
        self.output         = "/tmp/ubs/output/%s" % random.randint(0, 10000)
        self.mapper         = "cat"
        self.reducer        = "cat"
        self.files          = [self.input_info_file]
        self.priority       = "VERY_HIGH"
        self.reduce_tasks   = 3
        self.map_tasks      = 10    
        self.optional       = ["-jobconf udw.mapred.combine.inputformat=true"]
        
        


if __name__=='__main__':
    router.main()
