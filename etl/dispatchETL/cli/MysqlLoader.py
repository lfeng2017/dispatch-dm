# -*- coding:utf-8 -*-

'''

# 更新设备信息
python main.py DeviceImportor updateDeviceInfo -E prod --job fdcs_device [--date 20170329]

# 更新0公里风控订单
python main.py MysqlImportor updateDeviceInfo -E prod --job fdcs_device [--date 20170329]

'''

import os.path as op
import arrow
import time
import pandas as pd
from sqlalchemy import create_engine
from cement.core.controller import CementBaseController, expose

JobMeta = {
    "fdcs_device": {
        "suffix": "tsv"
    },
    "zero_order": {
        "suffix": "txt"
    },
}


class MysqlController(CementBaseController):
    class Meta:
        label = 'MysqlImportor'
        description = "Bdp文件导入mysql"
        stacked_on = 'base'
        stacked_type = 'nested'
        config_defaults = dict(
            foo='bar',
            some_other_option='my default value',
        )
        arguments = [
            (
                ['--job'],
                dict(help='ETL任务名', type=str, required=True, action='store', nargs='?', choices=JobMeta.keys())),
            (['--execute_date'],
             dict(help='执行日期（默认昨天）', type=str,
                  default=arrow.get().to("Asia/Shanghai").replace(days=-1).format("YYYYMMDD"), action='store')),
        ]

    @expose(hide=True)
    def default(self):
        self.app.log.info("no command specify, only show input parameters")
        self.app.log.info(self.app.pargs)

    @expose(help="将设备信息tsv更新至mysql中（只保留最新更新记录）")
    def updateDeviceInfo(self):
        jobName = self.app.pargs.job
        if jobName != "fdcs_device":
            raise ValueError("job name must be fdcs_device")
        date = self.app.pargs.execute_date

        tsv_dir = self.app.cfg['local_tsv_dir']
        db_uri = self.app.cfg['db_yongche_url']

        log = self.app.log

        log.info("begin to update device tsv to mysql's ods_device")
        start_time = time.time()

        # 表头字段
        TITLE = ['device_id', 'user_id', 'status', 'user_type', 'update_time', 'app_version', 'os_type', 'os_version',
                 'mac_address', 'device_type', 'device_serialno', 'exception_flag', 'os_name', 'app_series', 'dt']
        TABLE = "ods_device"

        # tsv -> pandas
        tsv_file = op.join(tsv_dir, "{name}_{dt}.tsv".format(name=jobName, dt=date))
        log.info("updateDeviceInfo -> tsvfile={}".format(tsv_file))
        df = pd.read_csv(tsv_file, delimiter='\t', names=TITLE, encoding="utf-8")
        log.info("read csv to dataframe size={}".format(df.shape[0]))
        df['dt'] = int(date)

        # pandas -> mysql
        engine = create_engine(db_uri, echo=False)
        df.to_sql(name=TABLE, con=engine, if_exists='replace', index=False)
        log.info("replace mysql table={}".format(TABLE))

        log.info("updateDeviceInfo is done, elapsed={}s".format(time.time() - start_time))

    @expose(help="将0公里的风控订单tsv更新至mysql中（保留10天），备注：实际执行日期为输入日期 +1 day")
    def updateRcOrder(self):

        jobName = self.app.pargs.job
        if jobName != "zero_order":
            raise ValueError("job name must be zero_order")
        date = self.app.pargs.execute_date

        tsv_dir = self.app.cfg['local_tsv_dir']
        db_uri = self.app.cfg['db_yongche_url']

        log = self.app.log

        log.info("begin to update device tsv to mysql's ods_device")
        start_time = time.time()

        # 表头字段
        TITLE_CN = ["订单号", "城市", "司机id", "用户id", "金额", "创建日期", "完成日期", "车型", "租赁公司", "产品类型", "计费里程", "计费时长", "系统记录里程",
                    "司机手动录入公里", "系统记录时长", "是否风控"]
        TITLE_EN = ["service_order_id", "city", "driver_id", "user_id", "price", "create_time", "complete_time",
                    "car_type",
                    "company", "product_type", "charge_distance", "charge_time_length", "system_distance", "mileage",
                    "actual_time_length", "is_rc"]
        TABLE = "rc_order"

        # tsv -> pandas
        tsv_file = op.join(tsv_dir, "{name}_{dt}.txt".format(name=jobName, dt=date))
        log.info("updateRisckControlledOrder -> tsvfile={}".format(tsv_file))
        df = pd.read_csv(tsv_file, delimiter='\t', names=TITLE_EN, index_col=0,
                         parse_dates=['create_time', 'complete_time'], encoding="utf-8")
        log.info("read csv to dataframe size={}".format(df.shape[0]))
        df = df[df.is_rc == "T"]
        df['dt'] = int(date)

        # 从数据库加载原有风控记录,与最新的进行合并
        today_len = df.shape[0]
        engine = create_engine(db_uri, echo=False)
        if engine.dialect.has_table(engine, TABLE):
            oldDF = pd.read_sql_table(TABLE, con=engine, index_col='service_order_id',
                                      parse_dates=['create_time', 'complete_time'])
            history_len = oldDF.shape[0]
            oldDF.drop(oldDF[oldDF.index.isin(df.index)].index, inplace=True)
            outDF = pd.concat([oldDF, df])
            new_len = outDF.shape[0]
            log.info("update hostory rc_order today={today} history={history} update={new}"
                     .format(today=today_len, history=history_len, new=new_len))
        else:
            outDF = df
            log.info("create rc_orders size={sz}".format(sz=outDF.shape[0]))

        # 更新至数据库
        outDF.to_sql(name='rc_order', con=engine, if_exists='replace', index=True)

        log.info("updateRcOrder is done, elapsed={}s".format(time.time() - start_time))
