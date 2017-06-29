# -*- coding: utf-8 -*-

import sys
import os.path as op
import arrow

PROJECT_ROOT_DIR = op.abspath(op.join(op.abspath(__file__), "../../../../"))
sys.path.append(op.join(PROJECT_ROOT_DIR, "dispatchETL"))
from common.utils import runShell

# scp 拷贝的源服务器登录帐号及密码
SCP_USER = "lujin"
SCP_PSW = "123Qwe,./"
SRC_HOST = "172.17.0.57"
SRC_DIR = "~/tmp/"

# scp保持至本机及hdfs的目录地址
LOCAL_TSV_DIR = "/var/tmp/"
HDFS_TSV_DIR = "/tmp/bdp"

# 导入脚本的绝对路径
UPLOAD_SCRIPT = op.join(PROJECT_ROOT_DIR, "dispatchETL/main.py")

if __name__ == "__main__":

    # 入参判断
    if len(sys.argv) != (2 + 1):
        print "[ERROR] args is invalid, inputs={},  e.g. python loadTsv2Hive.py 20170323 ods_service_order".format(
            sys.argv)
        exit(-1)

    # step 0. 执行日期转换, azkaban全局变量中的execute_date作为hook，以便在web UI上通过指定execute_date执行backfill
    if sys.argv[1] == "hook":
        print "[TRACE] execute_date set by azkaban global.properties hook"
        # 默认情况下执行前一天的job
        execute_date = arrow.get().to("Asia/Shanghai").replace(days=-1).format("YYYYMMDD")
    else:
        print "[TRACE] execute_date set by azkaban input flow parameters"
        execute_date = sys.argv[1]
    job = sys.argv[2]
    print "[INFO] execute_date = {dt} job={job}".format(dt=execute_date, job=job)

    # step 1. 从dm1远程获取bdp导出的tsv文件
    filename = "{name}_{dt}.tsv".format(name=job, dt=execute_date)
    src_file_path = op.join(SRC_DIR, filename)
    dest_file_path = op.join(LOCAL_TSV_DIR, filename)
    bash = "sshpass -p {psw} scp {user}@{host}:{src} {dest}" \
        .format(user=SCP_USER, psw=SCP_PSW, host=SRC_HOST, src=src_file_path, dest=dest_file_path)
    retval, msg = runShell(bash)
    if retval != 0:
        print "[ERROR] scp tsv file from dm1 failed, msg={}".format(msg)
        exit(-1)

    # step 2. 将文件上传至hdfs，删除本地文件
    runShell("hadoop fs -rm -f {}".format(op.join(HDFS_TSV_DIR, filename)))
    bash = "hadoop fs -put {file} {hdfs}/".format(file=dest_file_path, hdfs=HDFS_TSV_DIR)
    retval, msg = runShell(bash)
    if retval != 0:
        print "[ERROR] upload tsv file to hdfs failed, msg={}".format(msg)
        exit(-1)
    runShell("rm -f {}".format(dest_file_path))

    # step 3. 执行导入脚本（pyspark）， 删除hdfs上的临时文件
    # cluster's python is python2.6.6
    bash = "python {main} BdpHiveImportor loadTsv2Hive -E prod --job {job} --execute_date {dt} --hdfs_dir {hdfs}" \
        .format(main=UPLOAD_SCRIPT, job=job, dt=execute_date, hdfs=HDFS_TSV_DIR)
    retval, msg = runShell(bash)
    if retval != 0:
        print "[ERROR] using pyspark to load tsv into hive failed, msg={}".format(msg)
        exit(-1)
    hdfs_file_path = op.join(HDFS_TSV_DIR, filename)
    runShell("hadoop fs -rm {hdfs}".format(hdfs=hdfs_file_path),)

    print "[NOTICE] all is done"
    exit(0)
