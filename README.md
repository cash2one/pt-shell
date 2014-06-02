# HISTORY

    1. 2013-12-07, beta release0.0.1: 基本功能可用，基础数据内部测试。




# DESCRIPTION

    此python模块是一个调研脚本框架，设计目的是：
    1. 纯python style，凸显调研job的主线，容易了解脚本的功能（目的）和IO（使用）。
    2. 轻量级，无其他依赖。测试机上随意安装。

# INSTALL

    模块采用了setuptools管理安装。依赖setuptools, nose。
    1. python setup.py install         # 安装入site-packages
    2. python setup.py -q bdist_egg    # 生成egg文件，供调用
    3. python setup.py test            # 部分测试需要配置hadoop，测试时间较长



# TODO

	1. 添加PAPI的debug支持，类似于streaming方式的单part。
	2. logger提供关闭屏幕的配置。
	3. shell环境可以配置dfs命令的不同hadoop_home。
	4. 给job_name添加dpf的TAG支持
	5. 支持 MJob.cacheAchives = []
	6. shell环境的输出添加color支持，特别是在ls，info以及string命令的输出
	7. shell中支持"hello world!" 作为一个参数传递给job
	8. BUGFIX: debug没能获取单part文件时，会继续执行run，此事inputs为空，造成结果退出。
	9. 工具，检查配置是否正确，比如hadoop_home, 比如metaserver的权限
	10. 并行执行，debug的目录/tmp/xxxx存在冲突，增加随机函数
	11. debug的priorority默认要修改为VERY_HIGH
	12. pipe_kuang.py在linux下，info cat命令会引发decode异常。 windows下没有问题。
	13. 如果reducer为空。目前的命令行生成有问题。 且默认reduce数量为1， self.reduce_tasks="0" 才可以， =0不行
          -reducer "" \
          -outputformat org.apache.hadoop.mapred.TextOutputFormat \
          -jobconf mapred.map.tasks=211 \
          -jobconf mapred.reduce.tasks=0 \
	14. 在papi方式下，col为空表示所有字段，但框架不允许为空。
	15. papi需要-file xxxx-info-file。考虑框架自动添加一个。（如果streaming -file A -file A是OK的）
	16. 在多次执行Job.run()的时候，会多次输出屏幕日志。 可能多次addlogger
