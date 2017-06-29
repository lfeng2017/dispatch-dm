# -*- coding: utf-8 -*-

'''

此脚本可单独执行

batch mode:
python BdpImportManager.py --master=local[*] --job=ods_service_order --hdfs_dir=/tmp/bdp --batch --start_date=20170303 --end_date=20170305

single mode:
python BdpImportManager.py --master=local[*] --job=ods_service_order --hdfs_dir=/tmp/bdp --date=20170329

'''


import os
import os.path as op
import arrow
import subprocess
import traceback
import time
import click
import random
import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import desc
import BdpHiveDesc

# tsv to hive ETL任务的Meta信息
JobMeta = {
    "fo_service_order": {
        "db": "yc_bit",
        "table": "fo_service_order",
        "title": BdpHiveDesc.FoOrderDesc,
        "dropDuplicates": True
    },
    "fo_service_order_ext": {
        "db": "yc_bit",
        "table": "fo_service_order_ext",
        "title": BdpHiveDesc.FoOrderExtDesc,
        "dropDuplicates": True
    },
    "ods_service_order": {
        "db": "yc_bit",
        "table": "ods_service_order",
        "title": BdpHiveDesc.OdsOrderDesc,
        "dropDuplicates": False
    },
    "ods_service_order_charge": {
        "db": "yc_bit",
        "table": "ods_service_order_charge",
        "title": BdpHiveDesc.OrderChargeDesc,
        "dropDuplicates": False
    }
}


def getSchema(hive_desc_str):
    '''
    从hive表结构提取meta信息（dataframe的columns）
    '''
    schema = []
    for line in hive_desc_str.split('\n'):
        valid_segs = []
        for tag in line.split(' '):
            if tag != '':
                valid_segs.append(tag)
        if len(valid_segs) >= 2:
            colType = valid_segs[1]
            if colType.find("int") > 0:
                colType = "int"
            elif colType.find("timestamp") > 0:
                colType = "int"
            elif colType.find("string") > 0:
                colType = "str"
            elif colType.find("double") > 0:
                colType = "float"
            elif colType.find("float") > 0:
                colType = "float"
            elif colType.find("decimal") > 0:
                colType = "float"
            else:
                colType = "str"
            schema.append((valid_segs[0], colType))
    return schema


# udf for spark
def tuple2Row(segs, schema):
    ' 根据hive表的meta信息, 直接构造Row对象 '
    buff = dict()
    for i, meta in enumerate(schema):
        try:
            value = segs[i]
            if meta[1] == "int":
                value = int(value)
            elif meta[1] == "float":
                value = float(value)
        except IndexError, e:
            print "tsv out of index size={size} idx={idx}".format(size=len(segs), idx=i)
            exit(-1)
        except ValueError, e:
            # 类型转换错误，替换位默认值null
            value = None
        buff[meta[0]] = value
    return Row(**buff)


def tsv2Hive(jobName, tsvPath, date, sparkMaster, log):
    ' 将ods_service_order_charge导入本地hive '

    meta = JobMeta.get(jobName, None)
    if not meta:
        raise ValueError("invalid job name, can not get job meta data")

    db = meta['db']
    table = meta['table']
    dropDuplicates = meta['dropDuplicates']
    schema = getSchema(meta['title'])
    # schema = schema[:-1]  # 最后一个dt是partition, 需要删除

    # 判断文件是否存在
    tsv_file_path = op.join(tsvPath, "{tbl}_{dt}.tsv".format(tbl=table, dt=date))
    shell = "hadoop fs -test -f {hdfs}".format(hdfs=tsv_file_path)
    if subprocess.call(shell, shell=True) != 0:
        log.error("tsv file is not exist HDFS=" + tsv_file_path)
        exit(-1)

    port = 4040 + random.randint(100, 1000)

    # 指定yarn-client的master环境变量，spark版本为2.0
    os.environ['SPARK_MAJOR_VERSION'] = "2"

    spark = SparkSession.builder \
        .master(sparkMaster) \
        .appName("ETL_tsv2hive") \
        .enableHiveSupport() \
        .config("spark.ui.port", str(port)) \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.yarn.dist.files", "hdfs:///libs/pyspark/pyspark.zip,hdfs:///libs/pyspark/py4j-0.10.3-src.zip") \
        .config("spark.executorEnv.PYSPARK_PYTHON", "/usr/local/bin/python") \
        .config("spark.executorEnv.PYTHONPATH", "pyspark.zip:py4j-0.10.3-src.zip") \
        .config("spark.executorEnv.SPARK_MAJOR_VERSION", "2")   \
        .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/usr/local/bin/python") \
        .config("spark.yarn.appMasterEnv.PYTHONPATH", "pyspark.zip:py4j-0.10.3-src.zip") \
        .config("spark.yarn.appMasterEnv.SPARK_MAJOR_VERSION", "2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # 将依赖的文件以绝对路径添加至spark
    absDir = op.dirname(op.abspath(__file__))
    spark.sparkContext.addPyFile(op.join(absDir, "BdpImportManager.py"))
    spark.sparkContext.addPyFile(op.join(absDir, "BdpHiveDesc.py"))
    # dependencies = op.join(op.abspath(op.join(op.abspath(__file__), "../../common")), "utils.py")
    # spark.sparkContext.addPyFile(dependencies)

    try:
        df = spark.sparkContext.textFile(tsv_file_path) \
            .map(lambda line: line.split("\t")) \
            .map(lambda segs: tuple2Row(segs, schema)) \
            .toDF()

        if dropDuplicates:
            df = df.coalesce(20) \
                .orderBy("service_order_id", desc("update_time")) \
                .dropDuplicates(["service_order_id", "update_time"])
        else:
            df = df.coalesce(20)

        df.createOrReplaceTempView("tempDF")

        # df.printSchema()

        # 转换为parquet, 最后1列是dt
        cols = ','.join([seg[0] for seg in schema if seg[0] != "dt"])
        sql = "insert overwrite table {db}.{tbl} partition (dt={dt}) select {cols} from tempDF where dt like '{dt}%'" \
            .format(db=db, tbl=table, cols=cols, dt=date)
        log.info("tempView写入parquet: " + sql)
        spark.sql(sql)
        spark.catalog.dropTempView("tempDF")

        # 删除1个月之前的记录
        _30daysAgo = arrow.get(date, "YYYYMMDD").replace(days=-30).format("YYYYMMDD")
        sql = "ALTER TABLE {db}.{tbl} DROP IF EXISTS PARTITION(dt={dt})".format(db=db, tbl=table, dt=_30daysAgo)
        log.info("删除30天之前的partition: " + sql)
        spark.sql(sql)

        # 删除hdfs文件
        shell = "hadoop fs -rmr /user/hive/warehouse/{db}.db/{tbl}/dt={dt}".format(db=db, tbl=table, dt=_30daysAgo)
        log.info("删除30天之前的partition对应的HDFS: " + shell)
        subprocess.call(shell, shell=True)

    except Exception, e:
        traceback.print_exc()
        exit(-1)
    finally:
        if spark is not None:
            spark.stop()
            del spark


@click.command()
@click.option('--master', type=str, required=True, help='spark master uri')
@click.option('--job', type=str, required=True, help='ETL任务名')
@click.option('--hdfs_dir', type=str, required=True, help='tsv文件所在HDFS目录（文件名根据meta拼接）')
@click.option('--date', type=str, default=arrow.get().to("Asia/Shanghai").replace(days=-1).format("YYYYMMDD"),
              help='开始任务的日期（默认为昨天）')
@click.option('--batch', type=bool, default=False, help='批量模式')
@click.option('--start_date', type=str, default=arrow.get().to(tz="Asia/Shanghai").replace(days=-8).format("YYYYMMDD"),
              help='批量的开始日期')
@click.option('--end_date', type=str, default=arrow.get().to(tz="Asia/Shanghai").replace(days=-1).format("YYYYMMDD"),
              help='批量的结束日期')
def main(master, job, hdfs_dir, date, batch, start_date, end_date):
    ' 将ods_service_order_charge导入本地hive '
    import logging
    logging.basicConfig()
    log = logging.getLogger("ETLJobManager")

    if job not in JobMeta.keys():
        log.error("input job=" + job + " not in meta, please check your job name first!")
        exit(-1)

    if not batch:
        log.info("job=" + job + " tsv=" + hdfs_dir + " date=" + date)
        t = time.time()
        # 执行具体的导入任务
        tsv2Hive(job, hdfs_dir, date, master, log)
        log.info("etl for" + job + " is complete! elasped: " + str(time.time() - t))
    else:
        log.info("job=" + job + " tsv=" + hdfs_dir + " start=" + start_date + " end=" + end_date)
        t = time.time()

        start = datetime.datetime.strptime(start_date, "%Y%m%d")
        end = datetime.datetime.strptime(end_date, "%Y%m%d")
        dates = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]
        for dt in dates:
            dt = dt.strftime("%Y%m%d")
            tsv2Hive(job, hdfs_dir, dt, master, log)
            log.info("etl for " + job + "dt=" + dt + " is complete!")

    log.info("all is done! elasped: " + str(time.time() - t))
    exit(0)


if __name__ == '__main__':
    main()
