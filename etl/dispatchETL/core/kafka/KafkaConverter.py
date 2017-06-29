# -*- coding: utf-8 -*-

'''
将flume的textfile格式外部表，转换为parquet格式的内部表
hive: flume.TABLE -> ods.TABLE

python KafkaConverter.py --job=pre_dispatch --batch=True --start_date=20170407 --end_date=20170409

'''

import sys
import os.path as op
import time
import arrow
import subprocess
import datetime
import click
import FlumeHiveDesc

NAME = op.splitext(op.basename(__file__))[0]

sys.path.append(op.abspath(op.join(op.abspath(__file__), "../../../")))
from common.utils import runShell, get_logger

# tsv to hive ETL任务的Meta信息
JobMeta = {
    "service_order": {
        "from": "flume.kafka_service_order",
        "to": "ods.service_order",
        "desc": FlumeHiveDesc.OrderDesc,
        "patitons": 1,
        "where": "where dt={dt} and service_order_id is not null",
        "dropDuplicates": True,
    },
    "service_order_ext": {
        "from": "flume.kafka_service_order_ext",
        "to": "ods.service_order_ext",
        "desc": FlumeHiveDesc.OrderExtDesc,
        "patitons": 1,
        "where": "where dt={dt} and service_order_id is not null",
        "dropDuplicates": True,
    },
    "dispatch_info": {
        "from": "flume.kafka_dispatch_info",
        "to": "ods.dispatch_info",
        "desc": FlumeHiveDesc.DispatchInfoDesc,
        "patitons": 1,
        "where": "where dt={dt} and service_order_id is not null",
        "dropDuplicates": False,
    },
    "dispatch_detail": {
        "from": "flume.kafka_dispatch_detail",
        "to": "ods.dispatch_detail",
        "desc": FlumeHiveDesc.DispatchDetailDesc,
        "patitons": 2,
        "where": "where dt={dt} and hour={hour} and service_order_id is not null",
        "dropDuplicates": False,
    },
    "bidding_access": {
        "from": "flume.kafka_bidding_access",
        "to": "ods.bidding_access",
        "desc": FlumeHiveDesc.BiddingAccessDesc,
        "patitons": 1,
        "where": "where dt={dt} and request is not null",
        "dropDuplicates": False,
    },
    "pre_dispatch": {
        "from": "flume.kafka_pre_dispatch",
        "to": "ods.pre_dispatch",
        "desc": FlumeHiveDesc.PreDispatch,
        "patitons": 1,
        "where": "where dt={dt} and bidding_id is not null",
        "dropDuplicates": False,
    },
    "system_dispatch": {
        "from": "flume.kafka_system_dispatch",
        "to": "ods.system_dispatch",
        "desc": FlumeHiveDesc.SystemDispatch,
        "patitons": 1,
        "where": "where dt={dt} and order_id is not null",
        "dropDuplicates": False,
    },
    "personal_dispatch": {
        "from": "flume.kafka_personal_dispatch",
        "to": "ods.personal_dispatch",
        "desc": FlumeHiveDesc.PersonalDispatch,
        "patitons": 1,
        "where": "where dt={dt} and order_id is not null",
        "dropDuplicates": False,
    },
    "order_track": {
        "from": "flume.kafka_order_track",
        "to": "ods.order_track",
        "desc": FlumeHiveDesc.OrderTrack,
        "patitons": 1,
        "where": "where dt={dt} and order_id is not null",
        "dropDuplicates": False,
    },
    "driver_api_access_gray": {
        "from": "flume.kafka_driver_api_access_gray",
        "to": "ods.driver_api_access_gray",
        "desc": FlumeHiveDesc.DriverApiAccessGray,
        "patitons": 1,
        "where": "where dt={dt} and datetime is not null",
        "dropDuplicates": False,
    },
}


def getTitles(hive_desc_str, patitons=1):
    if not hive_desc_str or len(hive_desc_str) == 0:
        raise ValueError("hive_desc_str  is null or empty")
    if not patitons or patitons < 1:
        raise ValueError("patitons must >= 1")

    segs = [line.split(' ')[0] for line in hive_desc_str.split("\n") if len(line.strip()) > 0]
    segs = segs[:-patitons]
    return ','.join(segs)


def text2parquet(job, date, hour=None):
    '''
    hive textfile to parquet，步骤如下：
    0、请检查parquet表已经创建
    1、从JobMeta字典获取任务的元信息，主要是转换的列desc，partition的个数
    2、给flume的外部表添加转换日期对应的partition
    3、可选：对应需要去重的数据源（主要是order和order_ext等mysql binlog的数据源），通过row_number()提取最后的值
    4、通过insert overwrite table XXX select COL1, COL2, ... COLn from XXX，进行数据转换

    :param job: 转换任务的名称，必须是JobMeta字典的key
    :param date: 转换日期，e.g. 20170323
    :param hour: 转换小时，可选项，目前仅供派单明细使用，e.g. 13
    :return:
    '''
    if job is None or len(job) == 0:
        raise ValueError("job is null or empty")

    if date is None or len(date) == 0:
        raise ValueError("date is null or empty")

    log = get_logger(NAME)
    t = time.time()

    table_from = JobMeta[job]['from']
    patitons = JobMeta[job]['patitons']
    table_to = JobMeta[job]['to']
    cols = getTitles(JobMeta[job]['desc'], JobMeta[job]['patitons'])
    where = JobMeta[job]['where']
    dropDuplicates = JobMeta[job]['dropDuplicates']

    log.info("hive textfile to parquet is start...")

    # 判断是否去重
    if dropDuplicates:

        if job in ["service_order", "service_order_ext"]:
            # flume source external table
            addPartitionSQL = "ALTER TABLE {table_from} ADD IF NOT EXISTS PARTITION (dt={dt})". \
                format(table_from=table_from, dt=date)

            # target parquet
            where = where.format(dt=date)
            distinctData = "(select *, ROW_NUMBER() over (distribute by service_order_id sort by update_time desc) as rank from {table_from} {where}) tmp where rank=1 and operation<>'DELETE'" \
                .format(table_from=table_from, where=where)
            convertSQL = "insert overwrite table {table_to} partition (dt={dt}) select {cols} from {distinctData}" \
                .format(table_to=table_to, dt=date, cols=cols, distinctData=distinctData)
        else:
            raise ValueError("unknow job name for dropDuplicates=True")
    else:

        if patitons > 1:

            if hour is None:
                raise ValueError("job=dispatch_detail hour is null")

            if job == "dispatch_detail":

                # flume接的数据会生成hour=01和hour=1，2个目录，实际数据在hour=01下面，手动mv数据
                if hour < 10:
                    cmd = "hadoop fs -mv /tmp/kafka/dispatch_detail/dt={dt}/hour=0{hour}/* /tmp/kafka/dispatch_detail/dt={dt}/hour={hour}/ ".format(dt=date, hour=hour)
                    log.info("mv flume sink data output -> {cmd}".format(cmd=cmd))
                    subprocess.call(cmd, shell=True)

                # flume source external table
                addPartitionSQL = "ALTER TABLE {table_from} ADD IF NOT EXISTS PARTITION (dt={dt}, hour={hour})".format(
                    table_from=table_from, dt=date, hour=hour)
            else:
                raise ValueError("unknow job name for patitons>1")

            where = where.format(dt=date, hour=hour)
            convertSQL = "insert overwrite table {table_to} partition (dt={dt}, hour={hour}) select {cols} from {table_from} {where}" \
                .format(table_to=table_to, dt=date, hour=hour, cols=cols, table_from=table_from, where=where)
        else:
            # flume source external table
            addPartitionSQL = "ALTER TABLE {table_from} ADD IF NOT EXISTS PARTITION (dt={dt})". \
                format(table_from=table_from, dt=date)

            where = where.format(dt=date)
            convertSQL = " insert overwrite table {table_to} partition (dt={dt}) select {cols} from {table_from} {where}" \
                .format(table_to=table_to, dt=date, cols=cols, table_from=table_from, where=where)

    # 组合最终的SQL
    auxlibSQL = "add jar hdfs:/libs/hive_auxlib/hive-contrib.jar"
    etlSQL = "{addAuxlib}; {addPartition}; {convert};".format(addAuxlib=auxlibSQL, addPartition=addPartitionSQL,
                                                              convert=convertSQL)
    # textfile -> parquet
    retval, msg = runShell("hive -e \"{sql}\"".format(sql=etlSQL))
    if retval != 0:
        log.error("convet hive from text to parquet failed, job={job} dt={dt} failed msg={msg}"
                  .format(job=job, dt=date, msg=msg))
        exit(-1)

    log.info("etl for {job} is complete! elasped: {time}".format(job=job, time=(time.time() - t)))


@click.command()
@click.option('--job', type=click.Choice(JobMeta.keys()), required=True, help='任务名')
@click.option('--date', type=str, default=arrow.get().to("Asia/Shanghai").replace(days=-1).format("YYYYMMDD"),
              help='开始任务的日期（默认为昨天）')
@click.option('--batch', type=bool, default=False, help='批量模式')
@click.option('--start_date', type=str, default=arrow.get().to(tz="Asia/Shanghai").replace(days=-8).format("YYYYMMDD"),
              help='批量的开始日期')
@click.option('--end_date', type=str, default=arrow.get().to(tz="Asia/Shanghai").replace(days=-1).format("YYYYMMDD"),
              help='批量的结束日期（不包括这一天）')
def main(job, date, batch, start_date, end_date):
    ' 将ods_service_order_charge导入本地hive '
    log = get_logger(NAME)

    if not batch:
        log.info("job={job} mode=daily date={dt}".format(job=job, dt=date))
        t = time.time()
        # 调用hive -e执行转换
        if job == "dispatch_detail":
            for hour in range(0, 24):
                text2parquet(job=job, date=date, hour=hour)
        else:
            text2parquet(job=job, date=date)

    else:
        log.info("job={job} mode=Batch start={start} end={end}".format(job=job, start=start_date, end=end_date))
        t = time.time()

        start = datetime.datetime.strptime(start_date, "%Y%m%d")
        end = datetime.datetime.strptime(end_date, "%Y%m%d")
        dates = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]
        for dt in dates:
            dt = dt.strftime("%Y%m%d")
            # dispatch_detail是按dt和hour分区，单独处理
            if job == "dispatch_detail":
                for hour in range(0, 24):
                    # 调用hive -e执行转换
                    text2parquet(job=job, date=dt, hour=hour)
            else:
                # 调用hive -e执行转换
                text2parquet(job=job, date=dt)

    log.info("all is done! elasped: " + str(time.time() - t))
    exit(0)


if __name__ == '__main__':
    main()
    # print getTitles(JobMeta['dispatch_detail']['desc'], JobMeta['dispatch_detail']['patitons'])
