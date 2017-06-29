# -*- coding: utf-8 -*-

import sys
import arrow
import os.path as op

PROJECT_ROOT_DIR = op.abspath(op.join(op.abspath(__file__), "../../../../"))
sys.path.append(op.join(PROJECT_ROOT_DIR, "dispatchETL"))
from common.utils import runShell

# 导入脚本的绝对路径
UPLOAD_SCRIPT = op.join(PROJECT_ROOT_DIR, "dispatchETL/main.py")

if __name__ == "__main__":

    # 入参判断
    if len(sys.argv) != (2 + 1):
        print "[ERROR] args is invalid, inputs={},  e.g. python hiveText2Parquet.py 20170323 service_order".format(
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

    bash = "python {main} KafkaHiveConverter convetText2Parquet -E prod --job {job} --execute_date {dt}" \
        .format(main=UPLOAD_SCRIPT, job=job, dt=execute_date)
    retval, msg = runShell(bash)
    if retval != 0:
        print "[ERROR] convert hive from kafka textfile to parquet failed, msg={}".format(msg)
        exit(-1)

    print "[NOTICE] all is done"
    exit(0)
