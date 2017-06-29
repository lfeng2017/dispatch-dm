# -*- coding: utf-8 -*-

'''
一些工具模块的封装
'''

import os.path as op
import logging
import subprocess
from pyspark.sql import SparkSession

NAME = op.splitext(op.basename(__file__))[0]


def get_logger(name=None, log_level=logging.DEBUG):
    '''
    获取utils模块的logger handler

    :param log_level:  log等级
    :return:  logger
    '''

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers or len(logger.handlers) == 0:
        if log_level is None:
            logger.handlers = [logging.NullHandler()]
            return logger
        else:
            ch = logging.StreamHandler()

        ch.setLevel(log_level)
        formatter = logging.Formatter("%(asctime)s [utils] : %(message)s", "%Y-%m-%d %H:%M:%S")
        ch.setFormatter(formatter)
        logger.handlers = [ch]

    return logger


def runShell(cmd):
    '''
    使用子进程执行shell脚本

    :param cmd: 待执行的命令
    :return:
        retval => 状态码
        messages => 命令的输出字符串
    '''

    log = get_logger(NAME)

    log.info("CMD => " + cmd)

    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    messages = "\n".join([line.strip("\n") for line in proc.stdout.readlines()])
    retval = proc.wait()

    return retval, messages


def create_spark(job_name, mode="yarn-client", port=4040, driver_memery="1g", spark_parallel=70,
                 executor_memory="2g", execute_instances=3, executor_cores=1):
    '''
    创建spark session

    :return: None
    '''

    # hdfs上pyspark公共库的位置
    hdfs_python_lib_path = "hdfs:///libs/pyspark"
    yarn_dist_files = [
        "pyspark.zip",
        "py4j-0.10.3-src.zip",
    ]

    spark = SparkSession.builder \
        .master(mode) \
        .appName(job_name) \
        .config("spark.ui.port", str(port)) \
        .config("spark.driver.memory", driver_memery) \
        .config("spark.executor.instances", str(execute_instances)) \
        .config("spark.executor.cores", str(executor_cores)) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.default.parallelism", str(spark_parallel)) \
        .config("spark.sql.shuffle.partitions", str(spark_parallel)) \
        .config("spark.sql.crossJoin.enabled", "true") \
        .config("spark.yarn.dist.files", ','.join(map(lambda x: hdfs_python_lib_path + "/" + x, yarn_dist_files))) \
        .config("spark.yarn.appMasterEnv.PYTHONPATH", ':'.join(yarn_dist_files)) \
        .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/usr/local/bin/python") \
        .config("spark.executorEnv.PYTHONPATH", ':'.join(yarn_dist_files)) \
        .config("spark.executorEnv.PYSPARK_PYTHON", "/usr/local/bin/python") \
        .enableHiveSupport() \
        .getOrCreate()

    return spark


if __name__ == "__main__":
    code, msg = runShell("rm ttt")
    print "ret:", code
    print msg
