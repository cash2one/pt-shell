#!/usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. bfe相关问题追查： 360识别流量下降，对无线框进行了拒绝
 History:
     1. 2014/1/10 
"""


import sys
from ubs_shell.job import Job, MJob, PJob
from ubs_shell.router import router



Job.log_file = "tmp.my.pipeline.log"
MJob.hadoop_home = "/home/work/hadoop-client-stoff"
PJob.papi_java_lib = "db-dt-udw10.db01:2183,jx-dt-udw06.jx:2183,jx-dt-udw07.jx:2183,tc-dt-udw01.tc:2183,tc-dt-udw02.tc:2183/biglog/metadebug"
PJob.udw_meta_server = "/home/work/ubs/lib/papi"
PJob.udw_user = "InternalUser"

@router("cat")
class MergePid326Data2SinglePart(MJob):
    """
    pid=326的文件是分钟级数据，太碎，汇成一个文件，一天1G
    """
    def config(self, date):
        self.command            = "streaming"
        # /log/11669/ps_ubs_pid326_1min/20140109/0810/hz01-bae-static25.hz01/pid326_access_log.201401090812
        self.inputs             = "hdfs://szwg-ston-hdfs.dmop.baidu.com:54310/log/11669/ps_ubs_pid326_1min/%s/????/*/" % date
        self.output             = "/ps/ubs/pengtao/20140110-bfe-anti-360-review/pid326data/%s" % date
        self.mapper             = "cat"
        self.reducer            = "cat"    
        self.map_tasks          =  300
        self.reduce_tasks       =  3
        self.map_capacity       = 300
        self.reduce_capacity    = 3
        self.files              = []
        self.priority           = "HIGH"
        self.job_name           = "merge-pid326-%s" % date
        self.optional           = ["-inputformat org.apache.hadoop.mapred.CombineTextInputFormat"]
        
        
#@router("job2")
#class SimplePAPIJob(PJob):
    #"""
    
    #"""
    #def config(self, date):
        #self.input_project      = "udwetl_snapshotclick.event_day=%s.event_hour=04" % date
        #self.input_cols         = "event_baiduid,event_time,event_ip,event_query,event_urlparams"
        #self.input_info_file    = "c-info-file"
        
        #self.command            = "streaming"
        #self.inputs             = "happy"  # make hadoop happy
        #self.output             = "/tmp/ubs/output/%s" % random.randint(0, 10000)
        #self.mapper             = "cat"
        #self.reducer            = "cat"
        #self.files              = [self.input_info_file]
        #self.priority           = "VERY_HIGH"
        #self.reduce_tasks       = 3
        #self.map_tasks          = 10    
        #self.files              = []
        #self.input_other_num    = 2
        #self.input_other_cfgs   = [ ("/log/20682/bws_access/%s/0000/szwg-ecomon-hdfs.dmop/2350/tc-www-ipv6pr0.tself.baidu.com_%s235000.log" % (date, date),
                                     #"org.apache.hadoop.mapred.TextInputFormat"),
                                    #"/log/20682/browser_log_to_stoff/%s/2100/hz01-sw-dr00.hz01/%s2155.txt" % date]
        #self.optional           = ["-jobconf udw.mapred.combine.inputformat=true"]
        
        
#@router("download")
#class DownloadHdfsOutput(Job):
    #"""
    #local job
    #"""

    #def config(self, rand):
        #self.hdfs_path    = "/tmp/ubs/pengtao/output/%s" % rand
        #self.local_path   = "./local_bak"
        
    #def run(self):
        #""""""
        #cmds = ["dfs", "-get", self.hdfs_path, self.local_path]
        #hadoop_cmd(cmds, hadoop=MJob.hadoop_home)
        

#@router("msg")
#def print_welcome_msg(msg, title="Mr. ubs"):
    #""" simplely print msg with the title"""
    #print "Hello %s" % title
    #print "%s" % msg


if __name__=='__main__':
    router.main()