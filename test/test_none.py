#! /usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. ����ubs_shell�Ļ���demo������ʵ����ʽ��ʹ�á�
 History:
     1. 2013/12/11
"""

    
import unittest
import os
import random


class TestNone(unittest.TestCase):
    def setUp(self):
        pass
    
    def tearDown(self):
        pass

    #----------------------------------------------------------------------
    def test_None(self):
        """"""
        self.assertEquals(1,1)
        
if __name__ == "__main__":
    s1 = unittest.TestLoader().loadTestsFromTestCase(TestNone)
    suite = unittest.TestSuite([s1])  # [s1 s2 s3]
    unittest.TextTestRunner(verbosity=2).run(suite)
        
