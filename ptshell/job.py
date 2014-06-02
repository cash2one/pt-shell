#! /usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. hadoop����ķ�װ��
 History:
     1. 2013/12/8 
"""



import sys
import os
import random
import logging
import glob
import subprocess
import StringIO
import inspect

from utils import _decode, hadoop_cmd, get_hdfs_parts


########################################################################
class Job(object):
    """
        ��������ķ�װ��
        Job����child classͨ��decorator����router�в���instance��ͳһ���ȡ�
        
        feature:
        =======
           1. Ϊ�����ṩlogger����mapreduce��heavy job�����ã�
           2. �ṩ�ӿڣ�ƴװdocstring����router��ӡhelp��Ϣ��ѹ����
        
        usage:
        ======
           1. ����config������д���ã������ⲿ�����������������ã������е��ã�shell�������á�
           2. ����run������ִ�к����߼���
        
            >> @router("step1")
            >> class MyJob(Job):
            >>     def config(self, date):
            >>         "you config input/output and other parameters"
            >>         self.input_file = "input.%s" % date
            >>         self.output_file = "output.%s % date
            >>
            >>     def run(self):
            >>         "you code here"
            >>         ifh = open(self.input_file)
            >>         ofh = open(self.output_file, "w")
            >>         ofh.write(ifn.read())
            >>         ifh.close()
            >>         ofh.close()
            
            �ر�򵥵�����£�ִ���߼�ȫ������config��Ҳ���ԡ�������self.input_file���ݲ�����
        
    """
    log_file = "tmp.ubs.shell.log"    
    loglevel = logging.DEBUG
    
    # router����shell����ʱ������ִ�еķ���������˳�� 
    # config�����ǵ�1����run���������1��
    __required_steps__ = ["config", "run"]


    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self._logger = self._setup_logger()
        
        # check __required_steps__ 
        n = len(self.__required_steps__)
        for i in range(n):
            attr = self.__required_steps__[i]
            if not hasattr(self, attr):
                raise Exception("class %s has no %s" % (self.__class__, attr) )
            if i == 0:
                if attr != "config":
                    raise Exception("\"config\" must be the 1st required step of %s. \n current is %s" % (self.__class__, attr))
            elif i == n-1:
                if attr != "run":
                    raise Exception("\"run\" must be the last required step of %s. \n current is %s" % (self.__class__, attr))

        
        
    #----------------------------------------------------------------------
    def run(self):
        """"""
        pass
    
    #----------------------------------------------------------------------
    def config(self):
        """"""
        raise TypeError(" config method is not implemented in %s" % self.__class__)
    
    #----------------------------------------------------------------------
    def invoke(self, args, kwargs, mode="normal", opt={}):
        """
        �������к�shell���õĽӿڣ��ڲ�ִ��__required_steps__.
        ��Ϊ����ģʽ��normal��debug���� opt��debug������
        """
        if mode == "normal":
            n = len(self.__required_steps__)
            self.config(*args, **kwargs)
            for i in range(1, n-1):
                attr = self.__required_steps__[i]
                getattr(self, attr)()
            self.run()
            return True
        else:
            print >> sys.stderr, "%s invoke does not support %s mode" % (self.__class__, mode)
            return False
    
      
    #----------------------------------------------------------------------
    def get_line_help(self, prefix = ""):
        """
            get the single line help string
            
            >> app = PlotJob()
            >> print app.get_line_help()
               PlotJob.config < date, hour="03" >
               
        """
        # spec = (args, varargs, varkw, defaults)
        spec = inspect.getargspec(self.config)
        varnames = list(spec.args)
        #  ���default��Ϣ
        if spec.defaults:
            defaults = list(spec.defaults)                
            for i in range(-1, -len(defaults)-1, -1):
                varnames[i] += _decode("=%s" % repr(defaults[i]))
        # ȥ��self
        if varnames[0] == 'self':
            varnames = varnames[1:]
        sz = "%s.config < %s >" % (self.__class__.__name__, ", ".join(varnames))
        return prefix + sz
    
    #----------------------------------------------------------------------
    @staticmethod
    def get_func_help(func, prefix=""):
        """
        return the similar single line help for a callable function.
        """
        if not callable(func):
            raise Exception("%s is not a function" % func)
        spec = inspect.getargspec(func)
        varnames = list(spec.args)
        #  ���default��Ϣ
        if spec.defaults:
            defaults = list(spec.defaults)                
            for i in range(-1, -len(defaults)-1, -1):
                varnames[i] += _decode("=%s" % repr(defaults[i]))
        # ȥ��self
        if varnames[0] == 'self':
            varnames = varnames[1:]
        sz = "%s < %s >" % (func.func_name, ", ".join(varnames))
        return prefix + sz

        
   
    #----------------------------------------------------------------------
    def get_config_str(self, prefix=""):
        """
        get the detailed class doc and source code of config method (IO interface)
        """
        sz = _decode(inspect.getsource(self.config))
        # ��def��������Ϊ��׼��ȥ�����ڿո����prefix
        lines = sz.split("\n")
        i = 0
        while lines[0][i] == ' ':
            i += 1
        res = []
        for l in lines:
            res.append(prefix+l[i:]+"\n")
        return "".join(res)
    
    @staticmethod
    def get_func_config(func, prefix="", top=10):
        """
        get the detailed func doc and source
        """
        sz = _decode(inspect.getsource(func))
        # ��def��������Ϊ��׼��ȥ�����ڿո����prefix
        lines = sz.split("\n")
        i = 0
        while lines[0][i] == ' ':
            i += 1
        res = []
        n = top
        if n > len(lines):
            n = len(lines)
        j = 0
        while j < n: 
            l = lines[j]
            res.append(prefix+l[i:]+"\n")
            j += 1
        return "".join(res)        
        
        
    #----------------------------------------------------------------------
    def _setup_logger(self):
        """ Ŀǰ���е�job����һ��logger����������������� """
        
        logger = logging.getLogger(self.__class__.__name__)
        if not logger.handlers:
            logger.setLevel(self.__class__.loglevel)
            
            self._complex_formatter = logging.Formatter("%(asctime)s-[%(name)s]-[%(levelname)s]: %(message)s")     
            self._simple_formatter = logging.Formatter("%(message)s")     
            
            handler = logging.FileHandler(self.__class__.log_file)    
            handler.setFormatter(self._complex_formatter)
            logger.addHandler(handler)
            
            handler = logging.StreamHandler(sys.stderr)
            handler.setFormatter(self._complex_formatter)
            logger.addHandler(handler)
        
        return logger
    
    #----------------------------------------------------------------------
    def _raw_log(self, switch):
        """
        @type switch: bool
        @param switch: True to simple formatter, False to complex formatter
        """
        if switch:
            for handler in self._logger.handlers:
                handler.setFormatter(self._simple_formatter)
        else:
            for handler in self._logger.handlers:
                handler.setFormatter(self._complex_formatter) 
            
 
               

        
        
    
    

########################################################################
class MJob(Job):
    """
    ����MapReduce job �ķ�װ��
    """
    hadoop_home = "/home/work/hadoop-client-stoff/"
    
    # �Ƿ�ɾ��hadoop job�Ѿ����ڵ����Ŀ¼
    is_remove_output = False
    
    #----------------------------------------------------------------------
    def __init__(self, hadoop_home=""):
        """
        """
        Job.__init__(self)
        
        # hadoop�����л������ã���������
        #   ���ַ����Ĳ��ֿ�����check���������ã����� papi��input_formatĬ��Ϊ com.baidu.udw.mapred.MultiTableInputFormat
        self.command = ""
        self.mapper = ""
        self.reducer = ""
        self.inputs = []
        self.output = ""
        
        self.command_more = ""  # command�����Ϣ������-mapinstream binary�����ustreaming
        self.input_format = ""
        self.map_tasks = 1
        self.reduce_tasks = 1        
        self.files = []
        self.optional = []        
        
        #   �������õ�Ĭ��ֵ
        
        self.output_format = "org.apache.hadoop.mapred.TextOutputFormat"
        self.map_capacity = 300
        self.reduce_capacity = 100        
        self.job_name = "ubs-job-%s" % random.randint(0, 10000)
        self.priority = "NORMAL"
        

        
        # ��Ⱥ����
        self._hadoop_home = hadoop_home if hadoop_home else MJob.hadoop_home
        



    
    #----------------------------------------------------------------------
    def to_string(self):
        """
        ����hadoop�����ַ���
        """
        self._check()
        cmds = self._to_string()
        return " ".join(filter(None, cmds))
    
    def to_formatted_string(self):
        """
        ����hadoop�����ַ����������˹��鿴�� ����
        /home/work/.../hadoop streaming -libjars a,b,c \
            -input abc \
            -output cde \
            ...
        """
        self._check()
        cmds = self._to_string()
        (hadoop, command, more) = cmds[:3]
        sz = " ".join([hadoop, command, more]) + " \\\n    "
        sz += " \\\n    ".join(filter(None, cmds[3:]))
        return sz
    
    #----------------------------------------------------------------------
    def _to_string(self):
        """
        ����һ��hadoop�������ַ������顣
        e.g.
           ["hadoop", "streaming", "-libjars mylib", "-input path1", "-output path2", "-mapper cat", ...]
        """
        input_sz = ["-input %s" % self.inputs] if type(self.inputs) == type("") else map(lambda x: "-input %s" % x, self.inputs)
        file_sz = ["-file %s" % self.files] if type(self.files) == type("") else map(lambda x: "-file %s" % x, self.files)
        cmds = [self.hadoop_home+"/hadoop/bin/hadoop",  self.command, self.command_more,
                        # ����PJob
                        "-jobconf udw.mapred.input.info=%s" % self.input_info_file if hasattr(self, "input_info_file") else ""] + \
                        input_sz + \
                        ["-output %s" % self.output,
                        "-mapper \"%s\"" % self.mapper, "-reducer \"%s\"" % self.reducer,
                        "-inputformat %s" % self.input_format if self.input_format else "",
                        "-outputformat %s" % self.output_format if self.output_format else "",
                        "-jobconf mapred.map.tasks=%s" % self.map_tasks if self.map_tasks else "",
                        "-jobconf mapred.reduce.tasks=%s" % self.reduce_tasks if self.reduce_tasks else "",
                        "-jobconf mapred.job.map.capacity=%s" % self.map_capacity if self.map_capacity else "",
                        "-jobconf mapred.job.reduce.capacity=%s" % self.reduce_capacity if self.reduce_capacity else ""] \
             +  file_sz \
             + self.optional \
             + [ "-jobconf mapred.job.name=%s" % self.job_name, "-jobconf mapred.job.priority=%s" % self.priority ]
        return cmds
        
    
    def run(self, debug=False, opt={}):
        """ 
        ִ��hadoop����
        
        @type debug: bool 
        @param debug: run single part debug if True
        @type opt: dict
        @param opt: extra debug info like {"n":2, "m":1, "r":1}
        @rtype: bool
        @return: True if success. False otherwise
        """

        cmd_string = ""

        if not debug:
            cmd_string = self.to_string() # _check is invoked inside
            self._logger.info("in run: " + cmd_string)
            if MJob.is_remove_output:
                if hadoop_cmd(["dfs", "-ls", self.output], self.hadoop_home):
                    hadoop_cmd(["dfs", "-rmr", self.output], self.hadoop_home)                
        else:
            # 1. ��ȡdebug����
            # 2. �޸�job���ԣ�input, ouput, map_tasks, reduce_tasks)
            # 3. ��ȡcmds�ַ���
            # 4. �ָ�job���ԣ�����hadoopʧ�������ʵ���Զ�ʧ
            self._check()
            
            inputs = get_hdfs_parts(self.inputs, opt["n"], hadoop=self.hadoop_home)
            old_inputs = self.inputs
            old_output = self.output
            old_map_tasks = self.map_tasks
            old_reduce_tasks = self.reduce_tasks
            
            self.inputs = inputs
            self.output = "/tmp/" + self.output
            self.map_tasks = opt["m"]
            self.reduce_tasks = opt["r"]
            
            # remove output path
            if hadoop_cmd(["dfs", "-ls", self.output], self.hadoop_home):
                hadoop_cmd(["dfs", "-rmr", self.output], self.hadoop_home)
            self._raw_log(True)
            self._logger.info("run single part debug job with:")
            for f in inputs:
                self._logger.info("    -input = %s" % f)
            self._logger.info("    -output = %s" % self.output)
            self._raw_log(False)
            
            cmd_string = self.to_string()
            
            self.inputs = old_inputs
            self.output = old_output
            self.map_tasks = old_map_tasks
            self.reduce_tasks = old_reduce_tasks
            
            self._logger.info("in debug: " + cmd_string)
            
        try:
            process = subprocess.Popen(cmd_string, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        except OSError as e:
            self._logger.fatal("in run: %s" % e)
            return False
        
        # ��ӡhadoop client���������debug
        self._raw_log(True)
        self._logger.debug(">>>>>>>>>>>>>>> captured stdout/stderr output <<<<<<<<<<<<<<<")
        while True:
            line = process.stdout.readline() # �ܵ���parent��child����ͬ��
            if not line:
                break
            self._logger.debug(line.strip())
        process.wait()
        self._logger.debug(">>>>>>>>>>>>>>> captured stdout/stderr output <<<<<<<<<<<<<<<")
        self._raw_log(False)        
        
        
        if process.returncode == 0:
            return True
        else:
            self._logger.fatal("run failed with retcode=%s" % process.returncode)
            return False
    
    #----------------------------------------------------------------------
    def invoke(self, args, kwargs, mode="normal", opt={}):
        """
        �������к�shell���õĽӿڣ��ڲ�ִ��__required_steps__.
        ��Ϊ����ģʽ��normal��debug���� opt��debug������
        """
        if mode == "normal":
            n = len(self.__required_steps__)
            self.config(*args, **kwargs)
            for i in range(1, n-1):
                attr = self.__required_steps__[i]
                getattr(self, attr)()
            self.run()
            return True
        elif mode == "debug":
            n = len(self.__required_steps__)
            self.config(*args, **kwargs)
            for i in range(1, n-1):
                attr = self.__required_steps__[i]
                getattr(self, attr)()
            self.run(debug=True, opt=opt)
            return True
        else:
            print >> sys.stderr, "%s invoke does not support %s mode" % (self.__class__, mode)
            return False   

        
    #----------------------------------------------------------------------
    def _cmds_split(self, cmds):
        """
        Popen(cmds) ������ ["hadoop", "-input abc"], ת��Ϊ ["hadoop", "-input", "abc"]
        """
        cmds = filter(None, cmds)
        subcmds = map(lambda x:x.strip().split(), cmds)
        newcmds = []
        for s in subcmds:
            newcmds += s        
        return newcmds
        
    #----------------------------------------------------------------------
    def _check_basic(self):
        """"""
        if not self.command or \
           not self.inputs or \
           not self.output or \
           not self.mapper :
        #  self.reducer == ""
            raise Exception("\n".join([
                "missing command, inputs or output or mapper",
                "  command = %s" % self.command,
                "  inputs  = %s" % self.inputs,
                "  output  = %s" % self.output,
                "  mapper  = %s" % self.mapper ])
                            )

        if self.job_name:
            if self.job_name.find(" ") != -1 or self.job_name.find("\t") != -1:
                raise Exception("there are whitespace in job name : %s" % self.job_name)
        
        
    #----------------------------------------------------------------------
    def _check(self):
        """ ����command�����ú�����һЩĬ�ϲ�����
            
            ��Ŀ���Ǿ�������coder�����á����ԣ�û���ϸ��׼���Ǹ����ճ�ʹ�õ�ϰ�����������磺
            
               - bistreaming��SequenceFileAsBinaryInputFormat��ϡ�
               - ustreaming��-mapinstream binary���
            
            ��Ϊ_check���û����أ����ܱ���ε��ã���Ҫȷ��������ε��ý��һ�¡���������ظ�������
            
        """
        self._check_basic()
        
        if self.command in ("streaming", "hce", "streamoverhce"):
            pass
        elif self.command == "ustreaming":
            if not self.input_format:
                self.input_format = "org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat"
            if not self.command_more:
                self.command_more = "-mapinstream binary"
        elif self.command == "bistreaming":
            if not self.input_format:
                self.input_format = "org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat"
            if not self.output_format:
                self.output_format = "org.apache.hadoop.mapred.lib.SequenceFileAsBinaryOutputFormat"
        else:
            self._logger.warning("in _check: unsupported command %s" % self.command)
        
        
########################################################################
class PJob(MJob):
    """
    papi MapReduce job �����еķ�װ��
    """
    
    # udw_meta_server = "db-dt-udw10.db01:2183,jx-dt-udw06.jx:2183,jx-dt-udw07.jx:2183,tc-dt-udw01.tc:2183,tc-dt-udw02.tc:2183/biglog/dtmeta"
    udw_meta_server = "db-dt-udw10.db01:2183,jx-dt-udw06.jx:2183,jx-dt-udw07.jx:2183,tc-dt-udw01.tc:2183,tc-dt-udw02.tc:2183/biglog/metadebug"
    papi_java_lib = "/home/work/ubs/lib/papi"
    udw_user = "InternalUser"
    

    __required_steps__ = ["config", "get_job_info", "run"]
    
    #----------------------------------------------------------------------
    def __init__(self, hadoop_home="", udw_meta_server="", papi_java_lib="", user=""):
        
        self._udw_meta_server = udw_meta_server if udw_meta_server else PJob.udw_meta_server
        self._papi_java_lib = papi_java_lib if papi_java_lib else PJob.papi_java_lib
        self._user = user if user else PJob.udw_user
        
        self.input_project = ""
        self.input_cols = ""
        self.input_info_file = ""
        
        
        self.input_other_num = 0
        self.input_other_cfgs = []
        
        MJob.__init__(self, hadoop_home=hadoop_home)
    
    #----------------------------------------------------------------------
    def _check(self):
        """ �ο� MJob._check˵��
        """
        self._check_basic()
        
        if not self.command_more:
            libs = glob.glob(self.papi_java_lib + "/*.jar")
            self.command_more = "-libjars %s" % ",".join(libs)
            #     map(lambda x: self.papi_java_lib+"/"+x, ["udw-program-api.jar", 
            #                                              "libthrift-0.8.0.jar", 
            #                                              "hive-common-2.2.0.jar", 
            #                                              "gson-udw.jar", 
            #                                              "hive-serde-0.7.1.plus.jar", 
            #                                              "hive-ql-2.2.0.jar", 
            #                                              "protobuf-java-2.4.1.jar"])
        if not self.input_format:
            self.input_format = "com.baidu.udw.mapred.MultiTableInputFormat"
            
        if not self.input_info_file:
            self.input_info_file = "udw_info_%s" % random.randint(0, 10000)
            
        self._check_other_input()
        
        if self.command in ("streaming", "hce"):
            pass
        elif self.command == "bistreaming":
            if not self.input_format:
                self.input_format = "org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat"
            if not self.output_format:
                self.output_format = "org.apache.hadoop.mapred.lib.SequenceFileAsBinaryOutputFormat"
        else:
            self._logger.warning("in _check: unsupported command %s" % self.command)

        
    #----------------------------------------------------------------------
    def _check_udw_basic(self):
        """"""        
        if self.input_project == "": 
            # self.input_info_file == ""
            raise Exception("missing input_project")
        if self.input_cols == "":
            print >> sys.stderr, "Warning: missing input_cols. All fields in the table will be used."
    
    #----------------------------------------------------------------------
    def _check_other_input(self):
        """����udw���hdfs�ļ���ͬ��ȡ���������papi������е����á�
        
        @type optional: list
        @param optional: hadoop�����optional����
        
        """        
        self.input_other_num = int(self.input_other_num)
        if self.input_other_num == 0:
            return False
        
        more = ["-jobconf udw.mapred.input.other.num=%s" % self.input_other_num]
        if len(self.input_other_cfgs) != self.input_other_num:
            raise Exception("input other hdfs file num %d and configure mismatch : %s " % (self.input_other_num, self.input_other_cfgs))

        i = 0      
        while i < self.input_other_num:
            v = self.input_other_cfgs[i]
            if type(v) == type(()): # v = ("/path/2/input", "org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat")
                if len(v) > 1:
                    more += ["-jobconf udw.mapred.input.other.file.%s=%s" % (i, v[0]),
                             "-jobconf udw.mapred.input.other.format.%s=%s" % (i, v[1])
                             ]
                else:
                    more += ["-jobconf udw.mapred.input.other.file.%s=%s" % (i, v[0]),
                             "-jobconf udw.mapred.input.other.format.%s=org.apache.hadoop.mapred.TextInputFormat" % i
                             ]
            else: # v = "path/2/input"
                more += ["-jobconf udw.mapred.input.other.file.%s=%s" % (i, v),
                         "-jobconf udw.mapred.input.other.format.%s=org.apache.hadoop.mapred.TextInputFormat" % i
                         ]
            i += 1 
            
        self.optional = filter(lambda x: x.find("udw.mapred.input.other") == -1, self.optional) + more
        
        return True
    
    #----------------------------------------------------------------------
    def get_job_info(self, ifile=None):
        """
        """
        if ifile is not None:
            self.input_info_file = ifile
        else:
            if not self.input_info_file:
                self.input_info_file = "tmp.input.info.file.%s" % random.randint(0, 10000)
            
        self._check_udw_basic()

        cmds = [self.hadoop_home+"/java6/bin/java", "-jar %s/udw-program-api.jar" % self.papi_java_lib, "GetJobInfo",
                "-user %s" % self._user,
                "-server %s" % self._udw_meta_server,
                "-inputProj %s" % self.input_project,
                "-inputCols %s" % self.input_cols if self.input_cols else "",                
                "-ifile %s" % self.input_info_file
                ]
        cmds = self._cmds_split(cmds)
        self._logger.info("GetJobInfo: " + " ".join(cmds))
        try:
            process = subprocess.Popen(cmds, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        except OSError as e:
            self._logger.fatal("in get_job_info: %s" % e)
            return False

        self._raw_log(True)            
        self._logger.debug(">>>>>>>>>>>>>>> captured stdout/stderr output <<<<<<<<<<<<<<<")
        while True:
            line = process.stdout.readline()
            if not line:
                break
            self._logger.debug(line.strip())
        process.wait()
        self._logger.debug(">>>>>>>>>>>>>>> captured stdout/stderr output <<<<<<<<<<<<<<<")
        self._raw_log(False)        
        
        if process.returncode == 0:
            return True
        else:
            self._logger.fatal("run failed with retcode=%s" % process.returncode)
            return False         
            
    #----------------------------------------------------------------------
    def invoke(self, args, kwargs, mode="normal", opt={}):
        """
        �������к�shell���õĽӿڣ��ڲ�ִ��__required_steps__.
        ��Ϊ����ģʽ��normal��debug���� opt��debug������
        """
        if mode == "normal":
            n = len(self.__required_steps__)
            self.config(*args, **kwargs)
            for i in range(1, n-1):
                attr = self.__required_steps__[i]
                getattr(self, attr)()
            self.run()
            return True
        elif mode == "debug":
            # TODO
            print >> sys.stderr, "%s invoke does not support %s mode" % (self.__class__, mode)
            return False            
        else:
            print >> sys.stderr, "%s invoke does not support %s mode" % (self.__class__, mode)
            return False

            
        
########################################################################
class Tag:
    """
    hadoop���ܷ�����tag��
    """

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        pass
    
        
        
    
    
