# -*- coding: utf-8 -*-

'''

用户画像计算，目前实现模块如下：
1. stat_profile，基于历史统计量的画像信息

批量执行：python UserFeature.py --city bj --start_date 20170407 --END_date 20170409

'''

from __future__ import absolute_import

import sys
import os.path as op

from optparse import OptionParser
import time
import datetime
import pytz

from pyspark.sql.functions import asc, desc, expr, udf

# 添加项目根目录至path
sys.path.append(op.abspath(op.join(op.abspath(__file__), "../../../../")))

import dispatchETL.common.utils as Utils

# 获取文件名
NAME = op.splitext(op.basename(__file__))[0]


class UserFeature:
    @staticmethod
    def stat_profile(spark_session, city, dates):
        '''
        离线统计的用户画像（对加价倍率的敏感程度）
        1. 总体加价接受率：sum(加价且成单) / sum(加价订单)，float，建议分箱
        2. 平均成功加价倍率：avg(加价且成单的倍率)
        3. 25 % 成功加价倍率（近似下界）：percentile_approx(0.25)
        4. 50 % 成功加价倍率（近似均值）：percentile_approx(0.25)
        5. 75 % 成功加价倍率（近似上界）：percentile_approx(0.25)
        6. 拒绝所有加价flag：1 -> 拒绝所有加价, 0 -> 接受过任意加价，2 -> 无记录

        :param spark_session: spark session对象
        :param city: 城市简码
        :param dates: 使用的数据日期
        :return: spark dataframe
        '''

        # 若未传入spark session对象，则直接构造
        if not spark_session:
            raise ValueError("spark session can not be None")

        if not city or len(city) == 0:
            raise ValueError("input city code is None or empty")

        if not dates or len(dates) == 0:
            raise ValueError("input dates is None or empty")

        # dates转换为字符串，供partition过滤使用
        dates = ','.join(dates)

        # step 1. 依赖的基础数据准备 （依赖dispatch_info，dispatch_detail）
        # 1-1 dispatch_info中的最后一轮加价倍率信息
        sql = """
                SELECT
                    user_id,
                    service_order_id,
                    CASE WHEN decision_time>0 THEN decision_time else unix_timestamp(datetime, 'yyyy-MM-dd HH:mm:ss') END as final_decision_time,
                    decision_driver_id as final_decision_driver_id,
                    driver_bidding_rate as final_driver_bidding_rate
                FROM ods.dispatch_info
                WHERE dt in ({dates}) and city='{city}'
                """.format(dates=dates, city=city)
        dispatchInfoDF = spark_session.sql(sql) \
            .orderBy("service_order_id", desc("final_decision_time")) \
            .dropDuplicates(['service_order_id']) \
            .select(['user_id', 'service_order_id', 'final_decision_time', 'final_decision_driver_id',
                     'final_driver_bidding_rate'])
        # 1-2 派单明细中记录的最大加价倍率
        sql = """
                SELECT
                    service_order_id as oid,
                    max(driver_bidding_rate) as final_max_bidding_rate,
                    min(dispatch_time) as min_dispatch_time
                FROM ods.dispatch_detail
                WHERE dt in ({dates}) and city='{city}'
                GROUP BY service_order_id
                """.format(dates=dates, city=city)
        dispatchDetailDF = spark_session.sql(sql)
        # 合并加价倍率：dispatch_info left join dispatch_detail
        dispatchInfoDF.join(dispatchDetailDF, dispatchInfoDF.service_order_id == dispatchDetailDF.oid, "left") \
            .drop(dispatchDetailDF.oid) \
            .createOrReplaceTempView("v_user_profile")
        spark_session.catalog.cacheTable("v_user_profile")

        # step 2. 用户统计特征提取
        sql = """
        SELECT
            a.user_id as uid,
            a.user_order_count,
            a.user_order_decision_ok_count,
            a.user_order_decision_ok_count / a.user_order_count as order_decision_ok_pct,
            a.user_mean_wait_time,
            c.user_mean_wait_time_topk,
            a.user_bidding_count,
            a.user_bidding_ok_count,
            a.user_bidding_accept_flag,
            -- 无加价记录的设置为-1，此字段需要做箱后才可使用
            CASE WHEN a.user_bidding_accept_pct is null THEN -1.0 else a.user_bidding_accept_pct END as user_bidding_accept_pct,
            CASE WHEN b.user_accept_avg_bidding_rate is null THEN -1.0 else b.user_accept_avg_bidding_rate END as user_accept_avg_bidding_rate,
            CASE WHEN b.user_accept_25pct_bidding_rate is null THEN -1.0 else b.user_accept_25pct_bidding_rate END as user_accept_25pct_bidding_rate,
            CASE WHEN b.user_accept_50pct_bidding_rate is null THEN -1.0 else b.user_accept_50pct_bidding_rate END as user_accept_50pct_bidding_rate,
            CASE WHEN b.user_accept_75pct_bidding_rate is null THEN -1.0 else b.user_accept_75pct_bidding_rate END as user_accept_75pct_bidding_rate
        FROM (
            -- 用户总体订单情况
            SELECT
                  user_id,
                  sum(user_order_num) as user_order_count,
                  avg(user_wait_time) as user_mean_wait_time,
                  sum(is_decision_ok) as user_order_decision_ok_count,
                  sum(is_bidding) as user_bidding_count,
                  sum(is_bidding_ok) as user_bidding_ok_count,
                  sum(is_bidding_ok) / sum(is_bidding) as user_bidding_accept_pct, -- 用户接受了的加价订单的占比
                  CASE
                      WHEN sum(is_bidding)=0 THEN -1 -- 没有加价记录
                      WHEN sum(is_bidding)>0 and sum(is_bidding_ok)=0 THEN 0 -- 拒绝了所有加价
                      WHEN sum(is_bidding)>0 and sum(is_bidding_ok)>0 and sum(is_bidding)>sum(is_bidding_ok) THEN 1 -- 接受了部分加价
                      WHEN sum(is_bidding)>0 and sum(is_bidding_ok)>0 and sum(is_bidding)=sum(is_bidding_ok) THEN 2 -- 接受了所有加价
                      else -2 -- 未知的case
                  END as user_bidding_accept_flag
            FROM (
                  -- 每个订单用0/1标示出：是否加价、是否加价成功？ 编译后续算sum
                  SELECT
                      user_id,
                      final_decision_time - min_dispatch_time as user_wait_time,
                      1 as user_order_num,
                      CASE WHEN final_decision_driver_id>0 THEN 1 else 0 END as is_decision_ok,
                      CASE WHEN final_driver_bidding_rate>0 or final_max_bidding_rate>0 THEN 1 else 0 END as is_bidding,
                      CASE WHEN final_decision_driver_id>0 and final_driver_bidding_rate>0 THEN 1 else 0 END as is_bidding_ok
                  FROM v_user_profile
            ) tmp_a
            GROUP BY user_id
        ) a
        LEFT JOIN (
              -- 用户加价成功的倍率分布
              SELECT
                  user_id,
                  avg(final_driver_bidding_rate) as user_accept_avg_bidding_rate,  -- 接受的平均加价倍率
                  percentile_approx(final_driver_bidding_rate, 0.25) as user_accept_25pct_bidding_rate, -- 25%接受加价倍率
                  percentile_approx(final_driver_bidding_rate, 0.50) as user_accept_50pct_bidding_rate, -- 50%（中位数）接受加价倍率
                  percentile_approx(final_driver_bidding_rate, 0.75) as user_accept_75pct_bidding_rate -- 75% 接受加价倍率
              FROM (
                  SELECT
                      user_id,
                      cast(final_driver_bidding_rate as float) as final_driver_bidding_rate
                      FROM v_user_profile
                      WHERE final_driver_bidding_rate>0 and final_decision_driver_id>0 -- 加价且撮合成功的订单
              ) tmp_b
              GROUP BY user_id
        ) b on a.user_id=b.user_id
        LEFT JOIN (
              -- 最近n轮指标（平均等待时长）
              SELECT
                    user_id,
                    avg(user_wait_time_per_order) as user_mean_wait_time_topk
              FROM (
                    SELECT
                        user_id,
                        final_decision_time - min_dispatch_time as user_wait_time_per_order,
                        ROW_NUMBER() over (DISTRIBUTE BY service_order_id SORT BY final_decision_time DESC) as rank
                    FROM v_user_profile
              ) tmp_c
              WHERE rank < 3
              GROUP BY user_id
        ) c on a.user_id=c.user_id
        """
        user_profile_df = spark_session.sql(sql)

        spark_session.catalog.uncacheTable("v_user_profile")

        return user_profile_df


def dump_feature(df):
    '''
    TODO：未实现，线上服务特征应该写入kv

    :param df:
    :return:
    '''
    pass


if __name__ == '__main__':
    log = Utils.get_logger(NAME)

    # 构造输入参数的默认值
    end_date = datetime.datetime.now(tz=pytz.timezone('Asia/Shanghai')) - datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=14)  # 默认取2周数据
    # 输入参数解析
    parser = OptionParser()
    parser.add_option('--city', dest="city", type=str, help='清洗的城市简码（必须填写）')
    parser.add_option('--start_date', dest="start_date", type=str, default=start_date.strftime("%Y%m%d"),
                      help='批量的开始日期')
    parser.add_option('--end_date', dest="end_date", type=str, default=end_date.strftime("%Y%m%d"),
                      help='批量的结束日期（不包括这一天）')
    (opt, args) = parser.parse_args()
    if opt.city is None or len(opt.city) == 0:
        raise ValueError("city code must be input with --city option")

    # 根据输入的开始和结束日期，生成日期list（字符串类型）
    start_date = datetime.datetime.strptime(opt.start_date, '%Y%m%d') + datetime.timedelta(hours=8)
    end_date = datetime.datetime.strptime(opt.end_date, '%Y%m%d') + datetime.timedelta(hours=8)
    days = (end_date - start_date).days
    dates = map(lambda x: x.strftime('%Y%m%d'), [start_date + datetime.timedelta(days=i) for i in range(days)])
    log.info("begin to generate offline features, city={city} dates={dates}".format(city=opt.city, dates=dates))

    # 生成画像数据
    start_time = time.time()
    spark = Utils.create_spark(job_name="user_feature_offline", port=4400)
    df = UserFeature.stat_profile(spark_session=spark, city=opt.city, dates=dates)
    # TODO 上线时应该将dataframe的数据写入kv
    df.write.mode("overwrite").saveAsTable("tmp.stat_user_profile_{city}".format(city=opt.city))
    spark.stop()

    log.info("all is done!elasped: " + str(time.time() - start_time))

    # 退出
    exit(0)
