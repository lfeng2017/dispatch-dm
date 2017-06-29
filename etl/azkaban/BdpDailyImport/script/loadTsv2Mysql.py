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

# 0公里分控订单的scp地址
SRC_HOST_RC = "124.250.26.88"
SRC_DIR_RC = "/tmp/zero_order_new"

# scp保持至本机及hdfs的目录地址
LOCAL_TSV_DIR = "/var/tmp/"
HDFS_TSV_DIR = "/tmp/bdp"

# 导入脚本所在路径
UPLOAD_SCRIPT = "/home/lujin/jobs/DispatchETL/dispatchETL/main.py"

if __name__ == "__main__":

    # 入参判断
    if len(sys.argv) != (3 + 1):
        print "[ERROR] args is invalid, inputs={}, e.g. python loadTsv2Hive.py 20170323 ods_service_order OP_CMD".format(
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
    cmd = sys.argv[3]

    # 输入参数矫正
    if job == "zero_order":
        # 修正执行日期，其他任务均为 T+1，为保持日期格式一致，手工 +1 day
        # 风控组当天上午9点前将风控数据存放至跳板机的/tmp/zero_order_new下，对接人：冯东东
        execute_date = arrow.get(execute_date, "YYYYMMDD").replace(days=1).format("YYYYMMDD")
        # 修正文件后缀，及scp路径 （分控订单是推送到跳板机）
        filename = "{name}_{dt}.txt".format(name=job, dt=execute_date)
        src_file_path = op.join(SRC_DIR_RC, filename)
        remote_file = "{user}@{host}:{src}".format(user=SCP_USER, host=SRC_HOST_RC, src=src_file_path)
    else:
        # 设备数据是推送到DM1
        filename = "{name}_{dt}.tsv".format(name=job, dt=execute_date)
        src_file_path = op.join(SRC_DIR, filename)
        remote_file = "{user}@{host}:{src}".format(user=SCP_USER, host=SRC_HOST, src=src_file_path)

    print "[INFO] execute_date = {dt} job={job}".format(dt=execute_date, job=job)

    # step 1. 从dm1远程获取bdp导出的tsv文件

    dest_file_path = op.join(LOCAL_TSV_DIR, filename)
    bash = "sshpass -p {psw} scp {remote_file} {dest}".format(psw=SCP_PSW, remote_file=remote_file, dest=dest_file_path)
    retval, msg = runShell(bash)
    if retval != 0:
        print "[ERROR] scp tsv file from dm1 failed, msg={}".format(msg)
        exit(-1)

    # step 2. 执行导入脚本导入mysql
    # cluster's python is python2.6.6
    bash = "python {main} MysqlImportor {cmd} -E prod --job {job} --execute_date {dt}" \
        .format(main=UPLOAD_SCRIPT, cmd=cmd, job=job, dt=execute_date)
    retval, msg = runShell(bash)
    if retval != 0:
        print "[ERROR] python script to load tsv into mysql failed, msg={}".format(msg)
        exit(-1)
    runShell("rm -f {}".format(dest_file_path))

    print "[NOTICE] all is done"
    exit(0)
