#!/usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. 管理和交互式调用hadoop Job的框架
 History:
     1. 2013/12/11 created
"""



import sys
import inspect
import cmd
import readline
#import argparse
#from optparse import (OptionParser, BadOptionError, AmbiguousOptionError)

from job import Job, MJob, PJob
from utils import _decode, hadoop_cmd

class ArgParseError(Exception):
    """透传给job的参数， 解析失败"""
    pass

class Router(object):  
    """
    
    核心框架，管理脚本中每一个job/step。供shell进行调度。
    
    Usage
    =====
    
      1. import
      
         >> from ubs_shell.router import router
         >> if __name__ == "__main__":
         >>     router.main()
         
      2. decorate the functions & classes
      
        >> @router("plot")
        >> def plot_dwell_time_disribution():
        >>     import matplotlib
        >>     ...
        
        >> @router("step2")
        >> class process_groupby_data(Step):
        >>     def run(self, fn):
        >>         fh = open(fn,)
        >>         ...
        
        >> @router("grep1")
        >> class grep_newcookiesort_url(PJob):
        >>     def config(self):
        >>         self.input_project = "udwetl_ps_query.event_day=20131209"
        >>         ...
        
        >> @router("sum")
        >> class sum_hao123_and_union(Job):
        >>     def config(self):
        >>         self.command = "streaming"
        >>         ...       
        
      3. usage on command line
        >> python script.py -h
        >> python script.py -f step1 --ifn=a.txt --ofn=b.txt
        >> python script.py -f step2 a -c la -d laaa bvalue
    
       
    """
    def __init__(self):  
        self.app_path = {}
        self.app_order = [] # remember the order of path
        
    def __call__(self, path):
        """
        prepare a named wrapper function. Just register and return the orginal function          
        """
        def wrapper(application):
            self.register(path, application)
            return application
        return wrapper
    #----------------------------------------------------------------------
    def register(self, path, app_class):
        """
        1. find the type of application: Job/Step/function
        2. register the path to self.app_path with type info.
        
        @type path: string
        @param path: the path (short name) of an application. eg. "A", "step1"
        @type app: object
        @param app: a function/Job Object
        
        """
        s = path[0].lower()
        if  s < 'a' or s > "z":
            raise ValueError("path (short name) must start with character : %s" % path)
        if path in self.app_path:
            raise ValueError("duplicated path (short name) %s" % path)
        if inspect.isfunction(app_class):
            self.app_path[path] = (app_class, "func")
            self.app_order.append(path)
        elif inspect.isclass(app_class):
            fathers = inspect.getmro(app_class)
            if Job in fathers:
                type = "Job"
                if PJob in fathers:
                    type = "PJob"
                elif MJob in fathers:
                    type = "MJob"
                self.app_path[path] = (app_class(), type)
                self.app_order.append(path)
            else:
                raise Exception("unknown class : %s" % app_class)
        else:
            raise Exception("unknown object : %s" % app_class)
        
        return True
                
        
    def route(self, func, mixed_args=[], mode="normal", opt={}):
        """
         根据func的类型，执行应用逻辑。
        @type func: string 
        @param func: app's short name
        @type mixed_args: list 
        @param mixed_args: args passed to func
        @type mode: string 
        @param mode: normal or debug
        @type opt: dict
        @param opt: other info or debug mode
        """
        app, type = self.app_path[func]
        try:
            args, kwargs = self._arg2kw(mixed_args)
        except ArgParseError:
            return False
        
        if type == "func":
            return app(*args, **kwargs)
        elif type in ("Job", "MJob", "PJob"):
            return app.invoke(args, kwargs, mode, opt)
        else:
            raise TypeError("unknown type: %s" % application)
        
        return True    
    
    def check_path(self, path):
        """
        check whether the input is in self.app_path
        """
        if path in self.app_path:
            return True
        else:
            return False    

    #----------------------------------------------------------------------
    def _arg2kw(self, mixed_args):
        """
        convert 
            "a b --k=v -i input ok" 
        into 
            ["a", "b", "ok"] {"k":"v", "i":"input"}
        """
        def insert(dict_, k, v):
            if k in dict_:
                print "duplicated args : %s " % kv[0]
                raise ArgParseError
            dict_[k] = v
            
        opts = []
        args = {}

        n = len(mixed_args)
        i = 0
        while i < n:
            a = mixed_args[i]
            if a == '-' or a == '--' :
                opts.append(a)
            elif a.startswith("---"):
                print "invalid args: %s" % mixed_args
                print "only the following formats are supported:"
                print "  arg1"
                print "  --input=name1"
                print "  --output name3"
                print "  -oname2"
                print "  -o name4"
                raise ArgParseError
            elif a.startswith("--"):
                kv = a[2:].split("=", 1)
                if len(kv) == 2:
                    insert(args, kv[0], kv[1])
                else:
                    i += 1
                    insert(args, kv[0], mixed_args[i])
            elif a.startswith("-"):
                if len(a) > 2:
                    insert(args, a[1], a[2:])
                else:
                    i += 1
                    insert(args, a[1], mixed_args[i])
            else:
                opts.append(a)
            i += 1
       
        return opts, args    
            
    def _parse_args(self):
        """
        返回值的例子
           - ("shell", "", [], {})
           - ("help", "step1", [], {})
           - ("run",  "step1", ["arg1", "arg2"], {"k1":"v1", "k2":"v2"})
        """       

            
        def print_paths():
            """"""
            print "The available Job are:"
            for path in self.app_order:
                (app, type) = self.app_path[path]
                if type == "func":
                    print "  %-12s [%4s]  -->  %s" % (path, type, Job.get_func_help(app))
                elif type in ("Job", "MJob", "PJob"):
                    print "  %-12s [%4s]  -->  %s" % (path, type, app.get_line_help())
                else:
                    raise Exception("unknown Object type = %s of %s" % (type, app) )
            
        def print_general_help():
            """"""
            print "usage: %s [COMMAND]" % sys.argv[0]
            print "where COMMAND is one of :"
            print "  shell    enter the interactive shell mode. The DEFAULT value"
            print "  run      execute a specific Step/Job/func in script"
            print "           %s run TAG [[ARG1] [ARG2] ...] " % sys.argv[0]
            print "  help     print this help or detailed info about the specific Step/Job/func"
            print "           %s help TAG " % sys.argv[0]            
            # print "Most commands print help when invoked with w/o parameters."
            print 
            print_paths()
            sys.exit(0)
            
        def print_run_help():
            """"""
            print "usage: %s run TARGET [[ARG1] [ARG2]]" % sys.argv[0]
            print "  TARGET      the registered Step/Job/func"
            print "  ARG1/ARG2   all are passed to TARGET as args/kwargs"
            print 
            print_paths()
            sys.exit(0)
            
        
        
        
        argv = sys.argv[1:]
        # default
        (cmd, path, args) = ("shell", "", [])
        
        if len(argv) == 0:
            return (cmd, path, args)
        elif len(argv) == 1:
            if argv[0] == "shell":
                return (cmd, path, args)
            elif argv[0] == "run":
                print_paths()
                sys.exit(0)
            else:  # help or unknown
                print_general_help()
        else:
            # this will capture the -h args of invoked function
            #if "-h" in argv or "--help" in argv:
                #print_help()
            if argv[0] in ("shell", "help", "run"):
                cmd = argv[0]
                path = argv[1]
                if cmd == "shell":
                    return (cmd, path, args)
                else:
                    if self.check_path(path):
                        return (cmd, path, argv[2:])
                    else:
                        print_paths()
                        sys.exit(0)
            else:
                print_general_help()    
    
    #----------------------------------------------------------------------
    def print_path_help(self, path):
        """print the help of certain path"""
        (app, type) = self.app_path[path]
        if type == "func":
            print "  %s [%s] --> %s" % (path, type, Job.get_func_help(app))
            for line in _decode(app.__doc__).split("\n"):
                print "    " + line
            print Job.get_func_config(app, prefix="    ")
        elif type in ("Job", "MJob", "PJob"):
            print "  %s [%s] --> %s" % (path, type, app.get_line_help())
            for line in _decode(app.__doc__).split("\n"):
                print "    " + line
            print app.get_config_str(prefix="    ")
        else:
            raise Exception("unknown Object type = %s of %s" % (type, app) )
        print ""        
    
    #----------------------------------------------------------------------
    def shell(self):
        """run interactive shell"""
        RouterShell(self).cmdloop()
        
    
    #----------------------------------------------------------------------
    def main(self):
        """
        entrance for Router object.        
        usage:
           >> if __name__ == "__main__":
           >>     router.main()
           
        """
        cmd, path, args = self._parse_args()
        if cmd == "shell":
            print "You are now in ubs shell."
            print "Use \"python %s help\" to see other choice." % sys.argv[0]
            self.shell()
        elif cmd == "help":
            self.print_path_help(path)
            sys.exit(0)            
        elif cmd == "run":
            self.route(path, args)
        else:
            raise Exception("unknown CMD %s" % cmd)
        




# the Router instance for importing
router = Router()


class RouterShell(cmd.Cmd):
    """
    Simple shell command processor for ubs_hell jobs.
    
    TODO
    """
    prompt = "(ubs):"
    intro = "interactively run ubs_shell job."
    
    # 单part进行debug的优先级
    _DEBUG_PRIORITY = "VERY_HIGH"

    #----------------------------------------------------------------------
    def __init__(self, router):
        """"""
        cmd.Cmd.__init__(self)
        self.router = router
        self.job_queue = []
        
        # autocomplete will ignore "-"
        # http://mail.python.org/pipermail/python-list/2011-March/599475.html
        delims = readline.get_completer_delims().replace("-", "")
        readline.set_completer_delims(delims)

        # status variable list
        # all status variable must begin with "v_"
        self.v_remove = MJob.is_remove_output

    #----------------------------------------------------------------------
    def do_ls(self, pattern=""):
        """
        list all jobs. similar to print_paths in Router._parse_args.
        """
        if pattern:
            print "The available jobs with substring %s are:" % pattern
        else:
            print "The available jobs are:"
        
        app_order = self.router.app_order
        app_path = self.router.app_path
        n = len(self.router.app_order)
        j = 0
        for i in range(n):
            path = app_order[i]
            if path.find(pattern) != -1:
                j += 1
                app, type = app_path[path]
                if type == "func":
                    print "  %d. %-12s [%4s]  -->  %s" % (i, path, type, Job.get_func_help(app))
                elif type in ("Job", "MJob", "PJob"):
                    print "  %d. %-12s [%4s]  -->  %s" % (i, path, type, app.get_line_help())
                else:
                    raise Exception("unknown Object type = %s of %s" % (type, app) )
        if pattern:
            print "There are %d/%d including '%s'" % (j, n, pattern)

    #----------------------------------------------------------------------
    def help_ls(self):
        """"""
        print "\n".join(["ls [pattern]",
                         "ls all jobs with pattern substring."])        
        
    #----------------------------------------------------------------------
    def _do_run(self, path, args):
        """
        run job with args
        """
        try:
            self.router.route(path, args)
        except TypeError, e:
            # To catch the follow errors
            # TypeError: xxxx got an unexpected  keyword argument 'k'
            # TypeError: 'print_my_good() takes at least 1 argument (0 given)'
            print "run job %s with arg < %s > error:" % (path, ", ".join(args))
            print "%s" % e 
        
        
    def do_run(self, line):
        """
        run job
        """
        args = filter(None, line.strip().split())
        if args: # []
            self._do_run(args[0], args[1:])
        else:
            self.help_run()


    def help_run(self):
        print "\n".join(["run jobname [[ARG1] [ARG2] ...]",
                         "    run the job with arguments.",
                         "    use 'ls' to see available jobs"])


    def complete_run(self, text, line, begidx, endidx):
        if not text:
            completions = self.router.app_order
        else:
            completions = [ f
                            for f in self.router.app_order
                            if f.startswith(text)
                            ]
        return completions
    
    #----------------------------------------------------------------------
    def do_queue(self, line):
        """"""
        line = line.strip()
        if not line:
            if self.job_queue:
                print "current jobs in queue are:"
                for i in range(len(self.job_queue)):
                    ele = self.job_queue[i]
                    print "    %d. %-12s < %s >" % (i, ele[0], " ,".join(ele[1]))
            else:
                print "NO job in queue."
        else:
            parts = filter(None, line.split())
            if parts[0] == "clear":
                if len(parts) != 2:
                    self.help_queue()
                    return
                target = parts[1]
                if target == 'all':
                    print "clear all jobs..."
                    self.job_queue = []
                else:
                    try:
                        target = int(target)
                        if target >= len(self.job_queue):
                            print "NO %th job in queue" % target
                            return
                        print "clear %dth job: %s" % (target, self.job_queue[target])
                        del self.job_queue[target]
                    except ValueError:
                        print "invalid number %s" % target
                        self.help_queue()
                        return
            elif parts[0] == "start":
                n = len(self.job_queue)
                i = 0
                while self.job_queue:
                    ele = self.job_queue.pop(0)
                    i += 1
                    print "==== run %d/%d jobs in queue ====\n" % (i, n)
                    self._do_run(ele[0], ele[1])
            elif parts[0] == "add":
                if len(parts) > 1:
                    ele = (parts[1], parts[2:])
                    self.job_queue.append(ele)
                else:
                    self.help_queue()
            else:
                print "unknown command %s" % parts
                self.help_queue()
                return
            
    #----------------------------------------------------------------------
    def help_queue(self):
        """"""
        print "\n".join([
            "queue usage                     : manipulate the job queue.",
            "    queue                       : show current jobs.",
            "    queue start                 : start the job queue.",
            "    queue clear [N|all]         : clear the Nth/all job. 0-based.",
            "    queue add job [[ARG1]...]   : add job into queue."
        ])

    #----------------------------------------------------------------------
    def complete_queue(self, text, line, begidx, endidx):
        """
        """
        completions = []
        parts = filter(None, line.strip().split())
        n = len(parts)
        if n == 1:
            completions = ["add", "clear", "start"]
        elif n == 2:
            if text:
                completions = [f for f in ["add", "clear", "start"] if f.startswith(text)]
            else:
                # begin with the 3rd fields
                completions = self.router.app_order
        elif n == 3:
            completions = [ f
                            for f in self.router.app_order
                            if f.startswith(text)
                            ]
        else:
            pass
        return completions        
        

    #----------------------------------------------------------------------
    def _debug_parse_args(self, args):
        """
        input: 
            -m 10 -r 2 step1 -input fff -output xxx
        output:
            opt, path, args = {"m":10, "r":2, "n":1}, "step1", ["-input", "fff", "-output", "xxx"]
        """
        arg_list = list(args)
        n = len(arg_list)
        i = 0
        opt = {"n":1, "m":None, "r":1}
        path = ""
        others = []
        while i < n:
            cur = arg_list[i]
            if cur.startswith("-"):
                if cur in ('-n', "-m", "-r"):
                    opt[cur[1]] = arg_list[i+1]
                    i = i + 1
                else:
                    raise ArgParseError
            else:
                path = arg_list[i]
                others = arg_list[i+1:]
                break
            i += 1
        if path == "":
            raise ArgParseError
        
        if opt["m"] is None:
            opt["m"] = opt["n"]

        return (opt, path, others)

    def do_debug(self, line):
        """
        RouterShell中的特有接口，在单步执行hadoop job时，选择hdfs上的一个part作为输入，进行debug。
        debug [-n numofparts] [-m numofmapers] [-r numofreducers] job-name [[ARG1] ...] 
        说明见 help_debug.
        """
        fields = line.strip().split()
        n = len(fields)
        if n == 0 :
            self.help_debug()
        else:
            try:
                (opt, path, args) = self._debug_parse_args(fields)
            except ArgParseError:
                self.help_debug()
                return 
            if path not in self.router.app_path:
                print "invalid job name : %s" % path
                print "use \"ls\" to see all job name "
                return 
            # def route(self, func, mixed_args=[], mode="normal", opt={}):
            self.router.route(path, args, "debug", opt)
            
    #----------------------------------------------------------------------
    def help_debug(self):
        """"""
        print "\n".join(["debug [-n numofparts] [-m numofmappers] [-r numofreducers] job-name [[ARG1] ...] ",
                         "run the debug job with HIGH priority.",
                         "    -n number of hdfs parts, default 1.",
                         "    -m number of mappers, default == numofparts.",
                         "    -r number of reducers, default 1."])

    #----------------------------------------------------------------------
    complete_debug = complete_run

    #----------------------------------------------------------------------
    def do_dfs(self, line):
        """invoke hadoop dfs commands"""
        args = filter(None, line.strip().split())
        if not args:
            self.help_dfs()
        else:
            cmds = ["dfs"]+args
            (retcode, stdout) = hadoop_cmd(cmds, MJob.hadoop_home)
            if retcode is False:
                pass # Popen failed
            else:
                print stdout
                if retcode != 0:
                    print "hadoop dfs retcode=%s" % retcode

    #----------------------------------------------------------------------
    def help_dfs(self):
        """"""
        print "dfs [COMMAND [ARGS]...]" 
        print "      [-ls <path>]"
        print "      [-lsr <path>]"
        print "      [-du <path>]"
        print "      [-dus <path>]"
        print "      [-count[-q] <path>]"
        print "      [-mv <src> <dst>]"
        print "      [-cp <src> <dst>]"
        print "      [-ln <src> <dst>]"
        print "      [-rm <path>]"
        print "      [-rmr <path>]"
        print "      [-expunge]"
        print "      [-put <localsrc> ... <dst>]"
        print "      [-copyFromLocal <localsrc> ... <dst>]"
        print "      [-moveFromLocal <localsrc> ... <dst>]"
        print "      [-get [-ignoreCrc] [-crc] [-repair] <src> <localdst>]"
        print "      [-getmerge [-addnl] <src> <localdst> | -getmerge <src> <localdst> [addnl]]"
        print "      [-cat <src>]"
        print "      [-text <src>]"
        print "      [-copyToLocal [-ignoreCrc] [-crc] [-repair] <src> <localdst>]"
        print "      [-copySeqFileToLocal [-ignoreLen] <srcFile> <localDstFile>]"
        print "      [-moveToLocal [-crc] <src> <localdst>]"
        print "      [-mkdir <path>]"
        print "      [-setrep [-R] [-w] [-d] <rep> <path/file>]"
        print "      [-touchz <path>]"
        print "      [-test -[ezd] <path>]"
        print "      [-stat [format] <path>]"
        print "      [-tail [-f] <file>]"
        print "      [-chmod [-R] <MODE[,MODE]... | OCTALMODE> PATH...]"
        print "      [-chown [-R] [OWNER][:[GROUP]] PATH...]"
        print "      [-chgrp [-R] GROUP PATH...]"
        print "      [-help [cmd]]"
        
        print "invoke the hadoop dfs of Mjob.hadoop_home=%s" % MJob.hadoop_home
        
    #----------------------------------------------------------------------
    def complete_dfs(self, text, line, begidx, endidx):
        """ """
        cmds = ("-ls", "-lsr", "-du", "-dus", "-count", "-mv", "-cp", "-ln", "-rm", "-rmr", "-expunge", "-put", "-copyFromLocal", "-moveFromLocal", "-get", "-getmerge", "-cat", "-text", "-copyToLocal", "-copySeqFileToLocal", "-moveToLocal", "-mkdir", "-setrep", "-touchz", "-test", "-stat", "-tail", "-chmod", "-chown", "-chgrp", "-help")
        if not text:
            completions = cmds
        else:
            completions = [ f
                            for f in cmds
                            if f.startswith(text)
                            ]
        return completions        
        
        
    #----------------------------------------------------------------------
    def do_info(self, line):
        """
        print help for each job. 
        """
        args = filter(None, line.strip().split())
        if len(args) != 1 or args[0] not in self.router.app_order:
            self.help_info()
        else:
            self.router.print_path_help(args[0])
            
    #----------------------------------------------------------------------
    def help_info(self):
        """"""
        print "info job-name"
        print "    print the detailed information of job"
        print "    use \"ls\" to see all jobs."
        
    #----------------------------------------------------------------------
    complete_info = complete_run
            
    def do_string(self, line):
        """print cmd string with config args"""
        args = filter(None, line.strip().split())
        if not args or args[0] not in self.router.app_order:
            self.help_string()
        else:
            app, type = self.router.app_path[args[0]]            
            if hasattr(app, "to_formatted_string"):
                try:
                    v, kv = self.router._arg2kw(args[1:])
                except ArgParseError:
                    return
                try:
                    app.config(*v, **kv)
                except TypeError, e:
                    print "TypeError: %s" % e
                    return
                print app.to_formatted_string()
            else:
                print "%s do not support to_formatted_string" % args[0]
                
    #----------------------------------------------------------------------
    def help_string(self):
        """"""
        print "string job-name [[ARG1]...]"
        print "    print the hadoop command string of job"
        print "    your show provide the config arguments if needed"
        print "    use \"ls\" to see all jobs."

        
    #----------------------------------------------------------------------
    complete_string = complete_run        




    def do_set(self, line):
        """set the status variable
        """
        vs = filter(None, line.strip().split())
        
        if len(vs) == 2 :
            if vs[0] == 'remove' :
                if vs[1] in ("True", "T", "False", 'F'):
                    if vs[1].startswith("T"):
                        self.v_remove = True
                        print "    now remove = True"
                    elif vs[1].startswith("F"):
                        self.v_remove = False
                        print "    now remove = False"
                    else:
                        pass
                else:
                    print "known value of remove: %s" % vs[1] 
            else:
                print "unknow status variable %s=%s" % (vs[0], vs[1])
                self.help_set()
        else:
            self.help_set()
            
    #----------------------------------------------------------------------
    def help_set(self):
        """
        """
        print "\n".join([
            "set var value",
            "    set the status variable.",
            "    the avaible variables are:",
            "        %-8s - T[rue]/F[alse]" % "remove"
        ])
        
    #----------------------------------------------------------------------
    def complete_set(self, text, line, begidx, endidx):
        """
        """
        workline = line[:begidx]
        parts = filter(None, workline.strip().split())
        
        n = len(parts)
        completions = []

        if n == 1:  # set xxx
            if text:  # part[1]
                completions = [f for f in ["remove"] if f.startswith(text)]
            else:   # part[2]
                completions = ["remove"]
        elif n == 2:  # set verbose xxx
            if parts[1] == "remove":
                if text: 
                    completions = [f for f in ["False", "True"] if f.startswith(text)]
                else:
                    completions = ["False", "True"]
            else:  
                completions = []
        else:
            completions = []
            
        return completions   

    #----------------------------------------------------------------------
    def do_show(self, line):
        """"""
        args = filter(None, line.strip().split())
        
        for arg in args:
            name = "v_" + arg
            if hasattr(self, name):
                print "    %s = %s" % (arg, getattr(self, name))
            else:
                print "    %s = None" % (arg)
        if not args:
            var_list = filter(lambda x: x.startswith("v_"), dir(self))
            var_list = map(lambda x: x[2:], var_list)
            print "    Availabe variables include:"
            for var in var_list:
                print    "        %s" % var     
    

    #----------------------------------------------------------------------
    def help_show(self):
        """"""
        print "\n".join([
            "show [VAR1] [VAR2] ...",
            "   print the varaible values."
        ])
        
            
    #----------------------------------------------------------------------
    def complete_show(self, text, line, begidx, endidx):
        """"""
        var_list = filter(lambda x: x.startswith("v_"), dir(self))
        var_list = map(lambda x: x[2:], var_list)
        completion = []
        if text:
            completion = [f for f in var_list if f.startswith(text)]
        else:
            completion = var_list
            
        return completion
        

    def do_EOF(self, line):
        return True

    #----------------------------------------------------------------------
    def help_EOF(self):
        """"""
        print "exit the shell"

    #----------------------------------------------------------------------
    def do_quit(self, line):
        """"""
        return True
    
    #----------------------------------------------------------------------
    def help_quit(self):
        """"""
        print "exit the shell"

    do_q = do_quit
    help_q = help_quit
    do_exit = do_quit
    help_exit = help_quit
    
    #----------------------------------------------------------------------
    def help_help(self):
        """"""
        print "print this help"

    #----------------------------------------------------------------------
    def emptyline(self):
        """
        The default emptyline function is to repeat last command, which will cause trouble.
        So overide it here.
        """
        self.do_ls("")    
