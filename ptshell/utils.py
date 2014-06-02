#! /usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. util function for ubs_shell
 History:
     1. 2013/12/13 
"""

import sys
import os
import subprocess
import re
from datetime import datetime, timedelta

from urlparse import urlsplit

def _decode(sz):
    """
    脚本编码不同, 一次尝试用gbk, utf8, gb18030解码。
    """
    try:
        return sz.decode("gbk")
    except UnicodeDecodeError:
        try:
            return sz.decode("utf8")
        except UnicodeDecodeError:
            try:
                return sz.decode("gb18030")
            except UnicodeDecodeError:
                return sz




def today(n=0):
    """
    @type n: int 
    @param n: 0 -> today, 1 -> tomorrrow, -1 -> yestoday
    @rtype: string
    @return: string like "20131201"
    """
    target = datetime.today() + timedelta(days=n)
    return target.strftime("%Y%m%d")
    
#----------------------------------------------------------------------
def hadoop_cmd(cmds, hadoop='/home/work/hadoop-client-stoff/'):
    """
    wrapper of hadoop command
    @rtype: tuple
    @return: (retcode, stdout)
    """
    final_cmds = [hadoop+"/hadoop/bin/hadoop"] + filter(None, cmds)
    try:
        process = subprocess.Popen(final_cmds, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except OSError as e:
        print "system failed :"
        print "error: %s" % e
        print "command detail: %s" % " ".join(final_cmds)
        return (False, "")
    process.wait()
    res = process.stdout.read()
    return (process.returncode, res)


re_split = re.compile(r"[\*\?]+")
#----------------------------------------------------------------------
def _get_simple_hdfs_parts(path, size, hadoop='/home/work/hadoop-client-stoff', filter_prefix=None):
    """
    get the file from path without wildcard

    return path with filesystem information
    
    -rw-r--r--   3 ns-lsp ns-lsp         44 2013-12-14 02:09 /log/121/ps_www_us_to_ston/20131214/0000/@manifest.md5
    drwxr-xr-x   3 ns-lsp ns-lsp          0 2013-12-14 01:57 /log/121/ps_www_us_to_ston/20131214/0000/cq01-ps-wwwui0-t10.cq01

    filter_prefix = "d" or "-" to filter the files or directory
    
    """
    cmds = [hadoop+"/hadoop/bin/hadoop", "dfs", "-ls", path]
    try:
        process = subprocess.Popen(cmds, stdout=subprocess.PIPE)
    except OSError as e:
        print >> sys.stderr, "can not get hdfs parts : %s" % cmds
        print >> sys.stderr, e
        return []
    split_res = urlsplit(path)
    prefix = ""
    if split_res.scheme:
        # hdfs + :// +  szwg-rank-hdfs.dmop.baidu.com:54310
        prefix = split_res.scheme + "://" + split_res.netloc
    res = []
    while len(res) < size:
        # Found xxx items # this is possible
        # --rw-r--r-- xxxx  --> /log/20682/xxx              
        line = process.stdout.readline()
        if not line:
            print >> sys.stderr, " only %d parts in hdfs path %s" % (len(res), path)
            break
        if line.startswith("Found"):
            continue
        fields = line.strip().split()
        if filter_prefix:
            if not fields[0].startswith(filter_prefix):
                continue
        fields[-1] = prefix + fields[-1]
        res.append(fields)
    process.terminate()
    return res
    
def get_hdfs_parts(path, size, hadoop='/home/work/hadoop-client-stoff'):
    """
    get size parts from a single path. see get_multi_hdfs_parts

    the idea:
        ls /log/20682/querylog 比 ls /log/20682/querylog/* 要快很多，虽然两者返回的结果数量一致。/log/20682/querylog/20131204/0000
    Note:
        -input /mypath/* 用户输入是目录和文件混杂，本身存在问题
            -rw-r--r--   3 ns-lsp ns-lsp         44 2013-12-14 02:09 /mypath/@manifest.md5
            drwxr-xr-x   3 ns-lsp ns-lsp          0 2013-12-14 01:57 /mypath/input_dir
          
    """

    def split_wildcard(path):
        """
        # 将路径split为segs和wildcards
        # /a/b/c/*/d/e/szwg-????-dmop/f/*/h --> segs = ["/a/b/c/", "/d/e/", "/f/", "/h"], wildcars = ["*", "szwg-???-dmop", "*"]
        """
        split_res = urlsplit(path)
        prefix = ""
        if split_res.scheme:
            # hdfs + :// +  szwg-rank-hdfs.dmop.baidu.com:54310
            prefix = split_res.scheme + "://" + split_res.netloc
        segs = [prefix]
        wildcards = []
        # split_res.path 不安全, ?后的部分会被作为split_res.query
        scratch = path[len(prefix):].split("/")
        for p in scratch:
            if p == "":
                continue
            if p.find("*") != -1 or p.find("?") != -1:
                segs.append("")
                wildcards.append(p)
            else:
                segs[-1] += "/" + p
        return segs, wildcards

    def check_pattern(w, paths):
        """
        check wthether the basename of paths meet pattern
           201312?? --> /log/20682/querylog/20131205
           szwg-*-hdfs.dmop --> /log/20682/autorank/20131205/0000/szwg-ecomon-hdfs.dmop
        paths is like
          (
           ("-rw-r--r--", "3", "ns-lsp", "ns-lsp", "44", "2013-12-14", "02:09", "/mypath/@manifest.md5"),
           ("drwxr-xr-x", "3", "ns-lsp", "ns-lsp", "0", "2013-12-14", "01:57", "/mypath/input_dir")
          )

        
        """
        pattern = w.replace("*", ".*").replace("?", ".")
        res = []
        for f in paths:
            base = os.path.basename(f[-1])
            if re.match(pattern, base):
                res.append(f)
        return res


    segs, wildcards = split_wildcard(path)
        
    n = len(wildcards)
    if n == 0:
        # path with no wildcard
        paths_info = _get_simple_hdfs_parts(path, size, hadoop)
        return map(lambda x: x[-1], paths_info)
    else:
        for i in range(n-1):
            partials = _get_simple_hdfs_parts(segs[i], 4, hadoop)
            w = wildcards[i]
            ok_path = check_pattern(w, partials)
            # 路径中间必须为目录
            ok_path = filter(lambda x: x[0].startswith("d"), ok_path)
            if ok_path:
                segs[i+1] = ok_path[0][-1] + segs[i+1]            
            else:
                print >> sys.stderr, "could not find hdfs path deeper than %s " % segs[i] + w
                return []
                
        # 最后一个通配符
        partials = _get_simple_hdfs_parts(segs[n-1], max([4, size]), hadoop)
        w = wildcards[n-1]
        ok_path = check_pattern(w, partials)

        if segs[n]:
            # 存在非通配符后缀
            # /mypath/2013????/0000/part-00000
            ok_path = filter(lambda x: x[0].startswith("d"), ok_path)
            if ok_path:
                segs[n] = ok_path[0][-1] + segs[n]
                finals = _get_simple_hdfs_parts(segs[n], size, hadoop, filter_prefix="-")
                if finals:
                    # 处理 /mypath/201312??/part-00000 的情况，
                    # finals 只有一个路径["/mypath/20131201/part-00000"]
                    # 回溯上一级目录，寻找更多目录。
                    # 返回类似 /mypath/20131202/part-00000, /mypath/20131203/part-00000,
                    # 但不校验文件是否真实存在
                    if len(finals) < size and finals[0].startswith("-"):
                        finals = map(lambda x: x[-1]+segs[n], ok_path)
                    return map(lambda x: x[-1], finals)
                else:
                    print >> sys.stderr, "could not find hdfs path deeper than %s " % (segs[n])
                
            else:
                print >> sys.stderr, "could not find hdfs path deeper than %s/%s/%s " % (segs[n-1], w, segs[n])
                return []
        else:
            # 用户输入的通配符在路径最后，匹配文件和目录的可能性都存在
            # 比如 /mypath/*
            # 第一组
            #    dxxx /mypath/done
            #    -xxx /mypath/part-*
            # 第二组
            #    -xxx /mypath/@manifest
            #    dxxx /mypath/00000_0
            # 用一个简单策略，如果是part开头，或者数字开头，首选其作为文件，其他情况返回目录下的结果
            
            if ok_path:
                def filter_by_common_name(ok_path):
                    res = []
                    for f in ok_path:
                        if not x[0].startswith("-"):
                            continue
                        base = os.path.basename(f[-1])
                        if base.startswith("part") or (base[0] <= '9' and base[0] >= '0'):
                            res.append(f)
                    return res
                    
                path_dirs = filter(lambda x: x[0].startswith("d"), ok_path)
                path_files = filter(lambda x: x[0].startswith("-"), ok_path)
                if not (path_dirs and path_files):
                    if path_files:
                        return map(lambda x:x[-1], path_files)[:size]
                    else:
                        finals = _get_simple_hdfs_parts(path_dirs[0][-1], size, hadoop, filter_prefix="-")
                        return map(lambda x:x[-1], finals)
                else:
                    common_named_files = filter_by_common_name(path_files)
                    if common_named_files:
                        return map(lambda x:x[-1], common_named_files)
                    else:
                        finals = _get_simple_hdfs_parts(path_dirs[0][-1], size, hadoop, filter_prefix="-")
                        return map(lambda x:x[-1], finals)



def get_multi_hdfs_parts(paths, numofparts, hadoop='/home/work/hadoop-client-stoff'):
    """
    get the first n parts from paths. Equally divide n parts into number of paths.
    
    input is like
         ["hdfs://szwg-rank-hdfs.dmop.baidu.com:54310/app/ps/rank/ubs/monitor-target-out/????/all/newquerycube/part-*-A"]
         ["/app/ps/rank/ubs/monitor-target-out/????/all/newquerycube"]

    The returned paths is like
        -rw-r--r--   3 ns-lsp ns-lsp 1057134186 2013-03-02 03:41 /log/20682/newcookiesort/20130301/0000/szwg-ecomon-hdfs.dmop/part-00099
        
    如果路径中的通配符超过两组，则递归的解析路径。直接hadoop dfs -ls /path/*/*/* 太慢。 但是，对于这样的路径有风险
      /path/*/*/tc-click-log0, 避免写这样的路径。
      
    """
    n = len(paths)
    res = []
    for i in range(n):
        size = int((i+1)*numofparts/n) - int(i*numofparts/n)
        local_res = get_hdfs_parts(paths[i], size, hadoop)
        res += local_res
        
    return res




if __name__=='__main__':
    pass
    
