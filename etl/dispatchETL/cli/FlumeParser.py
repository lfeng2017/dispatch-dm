# -*- coding:utf-8 -*-

'''
batch mode:
python main.py KafkaHiveConverter convetText2Parquet -E prod --job ods_service_order  --batch --start_date=20170303 --end_date=20170305

single mode:
python main.py KafkaHiveConverter convetText2Parquet -E prod --job ods_service_order --execute_date 20170303

'''

import os.path as op
import sys
import time

import arrow
import pandas as pd
from cement.core.controller import CementBaseController, expose

sys.path.append(op.dirname(op.dirname(op.abspath(__file__))))
import core.kafka.KafkaConverter as etl
from common.utils import runShell


class KafkaHiveController(CementBaseController):
    """
    从bdp导出的tsv文件导入hive表
    """

    class Meta:
        label = 'KafkaHiveConverter'
        description = "kafka的textfile转为parquet"
        stacked_on = 'base'
        stacked_type = 'nested'
        config_defaults = dict(
            foo='bar',
            some_other_option='my default value',
        )
        arguments = [
            (
                ['--job'],
                dict(help='ETL任务名', type=str, required=True, action='store', nargs='?', choices=etl.JobMeta.keys())),
            (['--execute_date'],
             dict(help='执行日期（默认昨天）', type=str,
                  default=arrow.get().to(tz="Asia/Shanghai").replace(days=-1).format("YYYYMMDD"),
                  action='store')),
            (['--batch'], dict(help='批量模式开关', action='store_true')),
            (['--start_date'],
             dict(help='批量的开始日期（默认7天前）', type=str,
                  default=arrow.get().to(tz="Asia/Shanghai").replace(days=-8).format("YYYYMMDD"),
                  action='store')),
            (['--end_date'],
             dict(help='批量的结束日期（默认昨天）', type=str,
                  default=arrow.get().to(tz="Asia/Shanghai").replace(days=-1).format("YYYYMMDD"),
                  action='store')),
        ]

    @expose(hide=True)
    def default(self):
        self.app.log.info("no command specify, only show input parameters")
        self.app.log.info(self.app.pargs)

    @expose(help="将tsv加载至Hive表中")
    def convetText2Parquet(self):

        jobName = self.app.pargs.job

        log = self.app.log

        log.info("begin to convert hive table from textfile to parquet ...")
        start_time = time.time()
        script = op.join(op.abspath(op.join(op.abspath(__file__), "../../")), "core/kafka/KafkaConverter.py")

        # 执行ETL任务
        if self.app.pargs.batch:
            # 批量执行（逐天执行）
            for dt in pd.date_range(self.app.pargs.start_date, self.app.pargs.end_date, freq="D"):
                dt = dt.strftime("%Y%m%d")
                # cluster's python is python2.6.6
                bash = "python {main} --job={job} --date={dt}".format(main=script, job=jobName, dt=dt)
                # 采用shell方式执行pyspark，避免依赖文件过多在集群上容易出错
                retval, msg = runShell(bash)
                if retval != 0:
                    log.error("batch mode ETL job={job} dt={dt} failed msg={msg}".format(job=self.app.pargs.job, dt=dt, msg=msg))
                    exit(-1)

                log.info("batch mode ETL job={job} dt={dt} is complete".format(job=self.app.pargs.job, dt=dt))
        else:
            # 单日执行
            dt = self.app.pargs.execute_date
            # cluster's python is python2.6.6
            bash = "python {main} --job={job} --date={dt}".format(main=script, job=jobName, dt=dt)
            # 采用shell方式执行pyspark，避免依赖文件过多在集群上容易出错
            retval, msg = runShell(bash)
            if retval != 0:
                log.error("batch mode ETL job={job} dt={dt} failed msg={msg}".format(job=self.app.pargs.job, dt=dt, msg=msg))
                exit(-1)

        log.info("convert hive table from textfile to parquet is done, elasped:{}mins".format((time.time() - start_time) / 60.0))
