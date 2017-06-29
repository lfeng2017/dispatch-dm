# -*- coding:utf-8 -*-

'''
batch mode:
python main.py BdpImportor loadTsv2Hive -E prod --job ods_service_order --hdfs_dir /home/lujin/tmp --batch --start_date=20170303 --end_date=20170305

single mode:
python main.py BdpImportor loadTsv2Hive -E prod --job ods_service_order --hdfs_dir /home/lujin/tmp

'''

import os.path as op
import sys
import time

import arrow
import pandas as pd
from cement.core.controller import CementBaseController, expose

sys.path.append(op.dirname(op.dirname(op.abspath(__file__))))
import core.bdp.BdpImportManager as etl
from common.utils import runShell


class FeaturePreparatorController(CementBaseController):
    """
    从bdp导出的tsv文件导入hive表
    """

    class Meta:
        label = 'FeaturePreparator'
        description = "特征转换"
        stacked_on = 'base'
        stacked_type = 'nested'
        config_defaults = dict(
            foo='bar',
            some_other_option='my default value',
        )
        arguments = [
            (['--city'], dict(help='ETL任务名', type=str, required=True, action='store')),
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

    @expose(help="根据订单、派单、加价日志，生成加价训练数据")
    def generateBiddingTraining(self):

        city = self.app.pargs.city
        start_date = self.app.pargs.start_date
        end_date = self.app.pargs.end_date

        log = self.app.log

        log.info("begin to generate bidding training data, city={city} start={start} end={end} ..."
                 .format(city=city, start_date=start_date, end_date=end_date))
        start_time = time.time()
        script = op.join(op.abspath(op.join(op.abspath(__file__), "../../")), "core/preprocess/OrderBasedBiddingMerger.py")

        # 执行数据转换任务
        bash = "python {main} --city={city} --start_date={start_date} --end_date={end_date}" \
                .format(main=script, city=city, start_date=start_date, end_date=end_date)
        # 采用shell方式执行pyspark，避免依赖文件过多在集群上容易出错
        retval, msg = runShell(bash)
        if retval != 0:
            log.error("generate bidding training data failed, msg={msg}".format(msg=msg))
                exit(-1)

        log.info("load tsv file to hive is done, elasped:{}mins".format((time.time() - start_time) / 60.0))
