# HISTORY

    1. 2013-12-07, beta release0.0.1: �������ܿ��ã����������ڲ����ԡ�




# DESCRIPTION

    ��pythonģ����һ�����нű���ܣ����Ŀ���ǣ�
    1. ��python style��͹�Ե���job�����ߣ������˽�ű��Ĺ��ܣ�Ŀ�ģ���IO��ʹ�ã���
    2. �����������������������Ի������ⰲװ��

# INSTALL

    ģ�������setuptools����װ������setuptools, nose��
    1. python setup.py install         # ��װ��site-packages
    2. python setup.py -q bdist_egg    # ����egg�ļ���������
    3. python setup.py test            # ���ֲ�����Ҫ����hadoop������ʱ��ϳ�



# TODO

	1. ���PAPI��debug֧�֣�������streaming��ʽ�ĵ�part��
	2. logger�ṩ�ر���Ļ�����á�
	3. shell������������dfs����Ĳ�ͬhadoop_home��
	4. ��job_name���dpf��TAG֧��
	5. ֧�� MJob.cacheAchives = []
	6. shell������������color֧�֣��ر�����ls��info�Լ�string��������
	7. shell��֧��"hello world!" ��Ϊһ���������ݸ�job
	8. BUGFIX: debugû�ܻ�ȡ��part�ļ�ʱ�������ִ��run������inputsΪ�գ���ɽ���˳���
	9. ���ߣ���������Ƿ���ȷ������hadoop_home, ����metaserver��Ȩ��
	10. ����ִ�У�debug��Ŀ¼/tmp/xxxx���ڳ�ͻ�������������
	11. debug��priororityĬ��Ҫ�޸�ΪVERY_HIGH
	12. pipe_kuang.py��linux�£�info cat���������decode�쳣�� windows��û�����⡣
	13. ���reducerΪ�ա�Ŀǰ�����������������⡣ ��Ĭ��reduce����Ϊ1�� self.reduce_tasks="0" �ſ��ԣ� =0����
          -reducer "" \
          -outputformat org.apache.hadoop.mapred.TextOutputFormat \
          -jobconf mapred.map.tasks=211 \
          -jobconf mapred.reduce.tasks=0 \
	14. ��papi��ʽ�£�colΪ�ձ�ʾ�����ֶΣ�����ܲ�����Ϊ�ա�
	15. papi��Ҫ-file xxxx-info-file�����ǿ���Զ����һ���������streaming -file A -file A��OK�ģ�
	16. �ڶ��ִ��Job.run()��ʱ�򣬻��������Ļ��־�� ���ܶ��addlogger
