#! /usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. 测试ubs_shell.utils的基本case。
 History:
     1. 2013/12/11
"""

import sys    
import unittest
import os
import random
from ptshell.utils import hadoop_cmd, get_hdfs_parts, today

hadoop_home = "/home/work/hadoop-client-stoff"

class TestHadoop(unittest.TestCase):
    def setUp(self):
        pass
    
    def tearDown(self):
        pass

    #----------------------------------------------------------------------
    def test_hadoop_cmd(self):
        """"""
        
        #cmds = ["dfs", "-ls", "/log/20682/querylog"]
        cmds = ["dfs", "-ls", "/log/20682/"]
        retcode, stdout = hadoop_cmd(cmds, hadoop_home)
        self.assertEquals(retcode, 0)
        self.assertNotEqual(stdout.find("/log/20682/querylog"), -1)
        
    #----------------------------------------------------------------------
    def test_get_part_simple(self):
        """
        /log/20682/querylog/20131212/0000/szwg-rank-hdfs.dmop/querylog_20131212
        
        """

        path = "/log/20682/autorank/%s/0000/szwg-ecomon-hdfs.dmop/result/" % today(-3)
        returns = get_hdfs_parts(path, 2, hadoop_home)
        standards = [path+"part-00000", path+"part-00001"]
        self.assertListEqual(returns, standards)
    
    def test_get_part_complex(self):
        """
        /log/20682/querylog/20131212/0000/szwg-rank-hdfs.dmop/querylog_20131212
        
        """

        # pdb.set_trace()
        path = "/log/20682/autorank/%s/????/szwg-*-hdfs.dmop/result/" % today(-3)
        returns = get_hdfs_parts(path, 2, hadoop_home)
        print >> sys.stderr, returns
        sys.stderr.flush()
        returns = map(os.path.basename, returns)
        standards = ["part-00000", "part-00001"]
        self.assertListEqual(returns, standards)

        # /log/20682/bws_access/20131215/0000//szwg-ecomon-hdfs.dmop/2355/tc-www-ipv6pr0.tc.baidu.com_20131215235500.log
        path = "/log/20682/bws_access/%s/????//szwg-*-hdfs.dmop/????/*.log" % today(-3)
        returns = get_hdfs_parts(path, 2, hadoop_home)
        print >> sys.stderr, returns
        sys.stderr.flush()        
        returns = map(lambda x: os.path.basename(x)[-4:], returns)
        standards = [".log", ".log"]
        self.assertListEqual(returns, standards)

        path = "/log/20682/bws_access/%s/????//szwg-*-hdfs.dmop/????/" % today(-3)
        returns = get_hdfs_parts(path, 2, hadoop_home)
        print >> sys.stderr, returns
        sys.stderr.flush()        
        self.assertTrue(len(returns) == 2)
        
        

        # hdfs://szwg-ston-hdfs.dmop.baidu.com:54310/log/121/ps_www_us_to_ston/20131204/2300/yf-ps-wwwui67-j0.yf01/us.log.2013120423
        path = "hdfs://szwg-ston-hdfs.dmop.baidu.com:54310/log/121/ps_www_us_to_ston/%s/????/*/us.log.*" % today(-3)
        returns = get_hdfs_parts(path, 1, hadoop_home)
        print >> sys.stderr, returns
        sys.stderr.flush()        
        returns = map(lambda x: os.path.basename(x)[:6], returns)
        standards = ["us.log"]
        self.assertListEqual(returns, standards)
        
        
        
if __name__ == "__main__":
    s1 = unittest.TestLoader().loadTestsFromTestCase(TestHadoop)
    suite = unittest.TestSuite([s1])  # [s1 s2 s3]
    unittest.TextTestRunner(verbosity=2).run(suite)
        
