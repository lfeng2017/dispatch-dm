# -*- coding: utf-8 -*-

'''

加价训练数据清洗，以订单为基准
数据源：
1. 订单，ods.service_order、ods.service_order_ext
2. 派单，ods.dispatch_info、ods.dispatch_detail
3. 加价，ods.pre_dispatch、ods.system_dispatch、ods.personal_dispatch
输出：
1. 临时文件：hdfs:/tmp/preprocess/biddingTraining/tmp_XXXX.parquet，聚合的中间结果
2. join后的数据：hdfs:/tmp/preprocess/biddingTraining/merged_order/XX_CITY/YYYYMMDD.parquet，订单+派单+加价，按城市分目录，按日期存储
3. 最终结果：hive表，tmp.bidding_training，为分区


批量执行：python OrderBasedBiddingMerger.py --start_date=20170407 --end_date=20170409

'''

from __future__ import absolute_import

import sys
import os.path as op
from optparse import OptionParser
import time
import datetime
import pytz
import json
import Geohash
from pyspark.sql import SparkSession
from pyspark.sql.functions import asc, desc, expr, udf
from pyspark.sql import Row
from pyspark.sql.types import StringType

sys.path.append(op.abspath(op.join(op.abspath(__file__), "../../../../")))

from dispatchETL.common.utils import get_logger, create_spark
from dispatchETL.core.feature.UserFeature import UserFeature

NAME = op.splitext(op.basename(__file__))[0]


class BiddingTrainingPreparator:
    def __init__(self):
        '''
        构造函数
        '''

        # 计算过程中间临时文件存放的hdfs目录位置（文件名按城市自动拼接）
        self.hdfs = "/tmp/preprocess/biddingTraining"
        # 订单聚合后保持的临时文件
        self.order_dump_path = "{hdfs}/tmp_order.parquet".format(hdfs=self.hdfs)
        # 派单info的临时文件
        self.dispatch_info_dump_path = "{hdfs}/tmp_dispatch_info.parquet".format(hdfs=self.hdfs)
        # 派单日志（info + dispatch）聚合后的临时文件
        self.dispatch_dump_path = "{hdfs}/tmp_dispatch.parquet".format(hdfs=self.hdfs)
        # 加价日志聚合后的临时文件
        self.bidding_dump_path = "{hdfs}/tmp_bidding.parquet".format(hdfs=self.hdfs)
        # 订单 left join 派单、加价之后的临时文件
        self.merged_dump_dir = "{hdfs}/merged_order".format(hdfs=self.hdfs)
        # 订单join后，并增加了离线计算特征的临时文件
        self.merged_plus_dump_dir = "{hdfs}/merged_order_plus".format(hdfs=self.hdfs)

        # 初始化spark session, 添加geohash的依赖, 默认hive切换至ods库
        self.spark = create_spark(job_name="orderBase_bidding_clean", port=4200, execute_instances=7)
        self.spark.sparkContext.addPyFile("hdfs:///libs/pyspark/Geohash.zip")
        self.spark.sql("use ods")

        self.log = get_logger(self.__class__.__name__)

    def destroy(self):
        '''
        结束spark session
        '''

        if self.spark is not None:
            self.spark.stop()

    def join_datasource(self, city, dt):
        '''
        以order为基准，left join 派单、加价的数据

        :param city: 城市简码
        :param dt: 当前处理的日期，对应hive表分区
        :return: None
        '''

        self.extract_order(city, dt)
        self.extract_dispath_info(city, dt)
        self.extract_merged_dispatch(city, dt)
        self.extract_bidding(city, dt)
        self.merge_order_dispatch_bidding(city, dt)

    def extract_order(self, city, dt):
        '''
         提取订单有关数据，保存为parquet文件
         ods.service_order left join ods.service_order_ext

        :param city: 城市简码
        :param dt: 当前处理的日期，对应hive表分区
        :return: None
        '''

        # 构造日常参数
        execute_date = datetime.datetime.strptime(dt, '%Y%m%d') + datetime.timedelta(hours=8)
        start_ts = int(time.mktime(execute_date.replace(hour=0, minute=0, second=0).timetuple()))
        end_ts = int(time.mktime(execute_date.replace(hour=23, minute=59, second=59).timetuple()))

        # step 1. 取service_order中的有用字段
        where = " where dt={dt} and city='{city}' and status in (7,8) and create_time between {start} and {end}" \
            .format(dt=dt, city=city, start=start_ts, end=end_ts)
        sql = """
            select
            dt,
            city,  -- 这是下单所在城市，和dispatch的司机所在城市可能匹配不上
            service_order_id,
            product_type_id,
            is_asap,
            flag as order_flag,
            account_id,
            user_id,
            driver_id,
            corporate_id,
            car_type_id,
            car_type,
            car_brand,
            status as order_status,
            rc_status,
            reason_id as order_reason_id,
            create_time as order_create_time,
            arrival_time,
            confirm_time,
            start_time,
            end_time,
            start_position,
            start_address,
            end_position,
            end_address,
            expect_start_latitude,
            expect_start_longitude,
            expect_end_latitude,
            expect_end_longitude,
            start_latitude,
            start_longitude,
            end_latitude,
            end_longitude,
            coupon_name,
            coupon_facevalue,
            dependable_distance, --调整后里程
            actual_time_length --调整后时长
            from service_order {where}
            """.format(where=where)

        # 订单按时间戳去重（partition的分割不同日期可能会有重复的），保留当天更新时间最晚的1条记录
        self.spark.sql(sql) \
            .orderBy("service_order_id", desc("update_time")) \
            .dropDuplicates(['service_order_id']) \
            .drop('update_time') \
            .createOrReplaceTempView("v_order")

        # step 2. 取service_order_ext中的有用字段，与service_order做join
        where = " where a.dt={dt} and a.service_order_id in (select service_order_id from v_order)".format(dt=dt)
        sql = """
            select
            a.service_order_id,
            a.update_time,
            a.user_type,
            a.predict_amount,
            a.deadhead_distance,
            b.distance as estimate_distance,
            b.estimate_price
            from service_order_ext a
            lateral view json_tuple(a.estimate_snap, 'time_length', 'distance', 'estimate_price') b as time_length, distance, estimate_price
            {where}
        """.format(where=where)

        # 订单按时间戳去重（partition的分割不同日期可能会有重复的），保留当天更新时间最晚的1条记录
        self.spark.sql(sql) \
            .orderBy("service_order_id", desc("update_time")) \
            .dropDuplicates(['service_order_id']) \
            .drop('update_time') \
            .createOrReplaceTempView("v_order_ext")

        # step 3. service_order left join service_order_ext
        sql = """
        select
            a.*,
            b.service_order_id as service_order_id_ext,
            b.user_type,
            b.predict_amount,
            b.deadhead_distance,
            b.estimate_distance,
            b.estimate_price
        from v_order a left join v_order_ext b
        on a.service_order_id = b.service_order_id
        """

        dumpDF = self.spark.sql(sql) \
            .drop('service_order_id_ext') \
            .dropDuplicates(['service_order_id'])

        # step 4. 导出为parquet文件
        dumpDF.filter("estimate_distance is not null").write.mode('overwrite').parquet(self.order_dump_path)
        self.log.info("dump merged order to {path}, size={size}"
                      .format(path=self.order_dump_path, size=dumpDF.count()))
        self.spark.catalog.uncacheTable("v_order")
        self.spark.catalog.uncacheTable("v_order_ext")
        self.spark.catalog.clearCache()

    def extract_dispath_info(self, city, dt):
        '''
         提取派单有关数据，保存为parquet文件
         ods.dispatch_info left join ods.dispatch_detail

        :param city: 城市简码
        :param dt: 当前处理的日期，对应hive表分区
        :return: None
        '''

        # step 1. 提取派单基本信息
        where = " where dt={dt} and c.city='{city}'".format(dt=dt, city=city)
        sql = """
        select
            c.city,
            a.service_order_id,
            a.round,
            a.batch,
            ROW_NUMBER() over (distribute by service_order_id sort by round, batch) as acc_batch,
            unix_timestamp(a.datetime,'yyyy-MM-dd HH:mm:ss') as datetime,
            a.dispatch_time,
            a.decision_time,
            a.create_time,
            a.update_time,
            a.status,
            a.bidding_id,
            a.dispatch_type,
            a.decision_type,
            a.dispatch_count,
            a.response_count,
            a.accept_count,
            a.flag,
            a.user_level,
            a.user_gender,
            a.can_dispatch_count,
            a.decision_driver_id,
            a.decision_car_type_id,
            b.driver_bidding_rate,
            b.user_bidding_rate,
            b.driver_estimate_price,
            c.code,
            c.isRushHour,
            c.remark,
            d.assign_max_range,
            d.batch_driver_count,
            d.batch_interval,
            d.contribution_pct,
            d.distanct_pct,
            d.evaluation_pct,
            d.max_accept_count,
            d.max_driver_count,
            d.max_range
        from dispatch_info a
        lateral view json_tuple(a.add_price_info, 'dsb', 'sb', 'dep') b as driver_bidding_rate, user_bidding_rate, driver_estimate_price
        lateral view json_tuple(a.template_snapshot, 'city', 'code', 'dispatchParams', 'isRushHour', 'remark') c as city, code, dispatchParams, isRushHour,remark
        lateral view json_tuple(c.dispatchParams, 'aSSIGN_MAX_RANGE', 'bATCH_DRIVER_COUNT', 'bATCH_INTERVAL', 'cONTRIBUTION','dISTANCE','eVALUATION','mAX_ACCEPT_COUNT','mAX_DRIVER_COUNT','mAX_RANGE') d as assign_max_range, batch_driver_count, batch_interval, contribution_pct, distanct_pct,evaluation_pct,max_accept_count,max_driver_count,max_range
        {where}
        """.format(where=where)
        # 执行sparkSQL，cache为临时表
        self.spark.sql(sql).createOrReplaceTempView("v_dispatch_info")
        self.spark.catalog.cacheTable("v_dispatch_info")

        # step 2. 计算派单info的基本统计信息
        # 2-1 派单基准信息，取最后一条决策时间戳的记录对应的信息
        sql = """
        select
            city,
            service_order_id,
            bidding_id,
            acc_batch as sum_batch,
            create_time as dispatch_create_time,
            status as dispatch_status,
            flag as dispatch_flag,
            dispatch_type,
            decision_type,
            user_level,
            user_gender,
            isRushHour,
            remark,
            batch_driver_count,
            batch_interval,
            contribution_pct,
            distanct_pct,
            evaluation_pct,
            max_range
        from v_dispatch_info
        """
        baseDF = self.spark.sql(sql) \
            .orderBy("service_order_id", desc("decision_time")) \
            .dropDuplicates(['service_order_id']) \
            .drop('decision_time')

        # 2-2 派发累计信息，每一个订单的累计派发统计信息
        sql = """
        select
            service_order_id,
            max(round) as max_round,
            round(avg(can_dispatch_count), 0) as avg_can_dispatch,   --平均每轮可派司机数
            sum(can_dispatch_count) as sum_can_dispatch,             --累计可派司机数
            round(avg(dispatch_count), 0) as avg_dispatch_count,
            sum(dispatch_count) as sum_dispatch,
            round(avg(response_count), 0) as avg_response_count,
            sum(response_count) as sum_response,
            sum(accept_count) as sum_accept
        from v_dispatch_info
        group by service_order_id
        """
        totalDF = self.spark.sql(sql)
        # 派单基准 left join 派单累计信息
        outDF = baseDF.join(totalDF, baseDF.service_order_id == totalDF.service_order_id, 'left') \
            .drop(totalDF.service_order_id)

        # 2-3 首轮司机响应（但用户不一定决策成功），取响应司机数>0的第一条记录
        sql = """
        select
            service_order_id,
            round as first_accept_round,
            acc_batch as first_accept_acc_batch,
            decision_time as first_accept_time,
            driver_bidding_rate as first_accept_driver_bidding_rate,
            user_bidding_rate as first_accept_user_bidding_rate,
            driver_estimate_price as first_accept_driver_estimate_price
        from v_dispatch_info
        where accept_count>0
        """
        firstAcceptDF = self.spark.sql(sql) \
            .orderBy("service_order_id", asc("first_accept_time")) \
            .dropDuplicates(['service_order_id'])
        # 派单基准 left join 首轮司机响应
        outDF = outDF.join(firstAcceptDF, outDF.service_order_id == firstAcceptDF.service_order_id, 'left') \
            .drop(firstAcceptDF.service_order_id)

        # 2-4 首轮决策成功，取决策成功时间最早的1条记录
        sql = """
        select
            service_order_id,
            round as first_decision_round,
            acc_batch as first_decision_acc_batch,
            decision_time as first_decision_time,
            decision_driver_id as first_decision_driver_id,
            decision_car_type_id as first_decision_car_type_id,
            driver_bidding_rate as first_decision_driver_bidding_rate,
            user_bidding_rate as first_decision_user_bidding_rate,
            driver_estimate_price as first_decision_driver_estimate_price
        from v_dispatch_info
        where decision_driver_id>0
        """
        firstDecisionDF = self.spark.sql(sql) \
            .orderBy("service_order_id", asc("first_decision_time")) \
            .dropDuplicates(['service_order_id'])
        outDF = outDF.join(firstDecisionDF, outDF.service_order_id == firstDecisionDF.service_order_id, 'left') \
            .drop(firstDecisionDF.service_order_id)

        # 2-5 最后1轮，取决策时间最晚的1条记录
        sql = """
        select
            service_order_id,
            round as final_round,
            case when decision_time>0 then decision_time else datetime end as final_decision_time,
            decision_driver_id as final_decision_driver_id,
            decision_car_type_id as final_decision_car_type_id,
            status as final_dispatch_status,
            driver_bidding_rate as final_driver_bidding_rate,
            user_bidding_rate as final_user_bidding_rate,
            driver_estimate_price as final_driver_estimate_price
        from v_dispatch_info
        """
        finalDF = self.spark.sql(sql) \
            .orderBy("service_order_id", desc("final_decision_time")) \
            .dropDuplicates(['service_order_id'])
        outDF = outDF.join(finalDF, outDF.service_order_id == finalDF.service_order_id, 'left') \
            .drop(finalDF.service_order_id)

        # step 3. 导出为parquet文件
        outDF.write.mode('overwrite').parquet(self.dispatch_info_dump_path)
        self.log.info("dump merged dispatch_info to {path}, dt={dt} size={size}"
                      .format(path=self.dispatch_info_dump_path, dt=dt, size=outDF.count()))
        self.spark.catalog.uncacheTable("v_dispatch_info")
        self.spark.catalog.clearCache()

    def extract_merged_dispatch(self, city, dt):
        '''
         提取派单有关数据，保存为parquet文件
         ods.dispatch_info left join ods.dispatch_detail

        :param city: 城市简码
        :param dt: 当前处理的日期，对应hive表分区
        :return: None
        '''

        # step 1. 提取dispatch_detail基础信息
        where = " where dt={dt} and city='{city}'".format(dt=dt, city=city)
        sql = """
        select
            unix_timestamp(a.datetime,'yyyy-MM-dd HH:mm:ss') as datetime,
            a.service_order_id,
            a.round,
            a.batch,
            a.driver_id,
            a.dispatch_time,
            a.response_time,
            a.accept_status,
            a.driver_bidding_rate,
            a.driver_estimate_price,
            a.decision_time,
            a.decision_result,
            a.is_assigned,
            a.route_distance,
            a.route_time_length,
            a.distance,
            a.distance_time_length
            -- b.car_type_id,
            -- b.brand,
            -- b.driver_level
        from dispatch_detail a
        -- lateral view json_tuple(a.dispatch_snapshot, 'car_type_id', 'brand', 'driver_level', 'distance_rate', 'contribution_rate', 'evaluation_rate', 'base_score', 'evaluation', 'contribution') b as car_type_id, brand, driver_level, distance_rate, contribution_rate, evaluation_rate, base_score, evaluation, contribution
        {where}
        """.format(where=where)
        self.spark.sql(sql).createOrReplaceTempView("v_dispatch_detail")

        # step 2. 计算dispatch_detail的统计量
        # 备注：按batch聚合每一batch的派单情况，形成和dispath_info类似的格式
        sql = """
        select
            service_order_id,
            round,
            batch,
            max(datetime) as datetime,
            ROW_NUMBER() over (distribute by service_order_id sort by round, batch) as acc_batch,
            count(driver_id) as dispatch_count,
            max(dispatch_time) as dispatch_time,
            -- 本来响应司机数，时间用最大时间近似
            sum(case when accept_status=1 then 1 else 0 end) as accept_count,
            max(response_time) as response_time,
            -- 本轮有决策成功的，时间用最大时间近似
            max(case when decision_result=2 then 1 else 0 end) as decision_complete,
            max(decision_time) as decision_time,
            -- 加价相关
            min(driver_bidding_rate) as min_bidding_rate,
            --mean(driver_bidding_rate) as avg_bidding_rate,
            max(driver_bidding_rate) as max_bidding_rate,
            min(driver_estimate_price) as min_price,
            --mean(driver_estimate_price) as avg_price,
            max(driver_estimate_price) as max_price,
            -- 距离及时间相关
            min(route_distance) as min_route_distance,
            mean(route_distance) as avg_route_distance,
            max(route_distance) as max_route_distance,
            min(route_time_length) as min_route_time_length,
            mean(route_time_length) as avg_route_time_length,
            max(route_time_length) as max_route_time_length,
            min(distance) as min_distance,
            mean(distance) as avg_distance,
            max(distance) as max_distance,
            min(distance_time_length) as min_distance_time_length,
            mean(distance_time_length) as avg_distance_time_length,
            max(distance_time_length) as max_distance_time_length
        from v_dispatch_detail
        group by service_order_id, round, batch
        """.format()
        self.spark.sql(sql).createOrReplaceTempView("v_dispatch_detail_grouped")
        self.spark.catalog.cacheTable("v_dispatch_detail_grouped")

        # step 3. 读取dispatch_info清洗后的paruqet，按订单id为基准left join派单明细的统计量
        dispatchInfoDF = self.spark.sql("select * from `parquet`.`{path}`"
                                        .format(path=self.dispatch_info_dump_path))
        # 3-1 首轮司机响应（但用户不一定决策成功）, based on grouped dispatch_detail
        sql = """
        select
            service_order_id,
            round as first_accept_round_ext,
            acc_batch as first_accept_acc_batch_ext,
            response_time as first_accept_time_ext,
            -- 加价相关
            min_bidding_rate as first_accept_min_bidding_rate,
            max_bidding_rate as first_accept_max_bidding_rate,
            min_price as first_accept_min_price,
            max_price as first_accept_max_price,
            -- 距离及时间相关
            min_route_distance as first_accept_min_route_distance,
            max_route_distance as first_accept_max_route_distance,
            min_route_time_length as first_accept_min_route_time_length,
            avg_route_time_length as first_accept_avg_route_time_length,
            max_route_time_length as first_accept_max_route_time_length,
            min_distance as first_accept_min_distance,
            avg_distance as first_accept_avg_distance,
            max_distance as first_accept_max_distance,
            min_distance_time_length as first_accept_min_distance_time_length,
            avg_distance_time_length as first_accept_avg_distance_time_length,
            max_distance_time_length as first_accept_max_distance_time_length
        from v_dispatch_detail_grouped
        where accept_count>0
        """
        firstAcceptDF = self.spark.sql(sql) \
            .orderBy("service_order_id", asc("first_accept_time_ext")) \
            .dropDuplicates(['service_order_id'])
        outDF = dispatchInfoDF.join(firstAcceptDF, dispatchInfoDF.service_order_id == firstAcceptDF.service_order_id,
                                    'left').drop(firstAcceptDF.service_order_id)

        # 3-2 首轮决策成功
        sql = """
        select
            service_order_id,
            round as first_decision_round_ext,
            acc_batch as first_decision_acc_batch_ext,
            decision_time as first_decision_time_ext,
            -- 加价相关
            min_bidding_rate as first_decision_min_bidding_rate,
            max_bidding_rate as first_decision_max_bidding_rate,
            min_price as first_decision_min_price,
            max_price as first_decision_max_price,
            -- 距离及时间相关
            min_route_distance as first_decision_min_route_distance,
            avg_route_distance as first_decision_avg_route_distance,
            max_route_distance as first_decision_max_route_distance,
            min_route_time_length as first_decision_min_route_time_length,
            avg_route_time_length as first_decision_avg_route_time_length,
            max_route_time_length as first_decision_max_route_time_length,
            min_distance as first_decision_min_distance,
            avg_distance as first_decision_avg_distance,
            max_distance as first_decision_max_distance,
            min_distance_time_length as first_decision_min_distance_time_length,
            avg_distance_time_length as first_decision_avg_distance_time_length,
            max_distance_time_length as first_decision_max_distance_time_length
        from v_dispatch_detail_grouped
        where decision_complete>0
        """
        firstDecisionDF = self.spark.sql(sql) \
            .orderBy("service_order_id", asc("first_decision_time_ext")) \
            .dropDuplicates(['service_order_id'])
        outDF = outDF.join(firstDecisionDF, outDF.service_order_id == firstDecisionDF.service_order_id, 'left') \
            .drop(firstDecisionDF.service_order_id)

        # 3-3 最后1轮
        sql = """
        select
            service_order_id,
            round as final_round_ext,
            acc_batch as final_acc_batch_ext,
            datetime as final_time_ext,
            -- 加价相关
            min_bidding_rate as final_min_bidding_rate,
            max_bidding_rate as final_max_bidding_rate,
            min_price as final_min_price,
            max_price as final_max_price,
            -- 距离及时间相关
            min_route_distance as final_min_route_distance,
            avg_route_distance as final_avg_route_distance,
            max_route_distance as final_max_route_distance,
            min_route_time_length as final_min_route_time_length,
            avg_route_time_length as final_avg_route_time_length,
            max_route_time_length as final_max_route_time_length,
            min_distance as final_min_distance,
            avg_distance as final_avg_distance,
            max_distance as final_max_distance,
            min_distance_time_length as final_min_distance_time_length,
            avg_distance_time_length as final_avg_distance_time_length,
            max_distance_time_length as final_max_distance_time_length
        from v_dispatch_detail_grouped
        """
        finalDF = self.spark.sql(sql).orderBy("service_order_id", desc("final_time_ext")) \
            .dropDuplicates(['service_order_id'])
        outDF = outDF.join(finalDF, outDF.service_order_id == finalDF.service_order_id, 'left') \
            .drop(finalDF.service_order_id)

        # step 4. 导出派单数据合并后的结果，至parquet文件
        outDF.write.mode('overwrite').parquet(self.dispatch_dump_path)
        self.log.info("dump merged dispatch (info+detail) to {path}, dt={dt} size={size}"
                      .format(path=self.dispatch_dump_path, dt=dt, size=outDF.count()))
        self.spark.catalog.uncacheTable("v_dispatch_detail")
        self.spark.catalog.uncacheTable("v_dispatch_detail_grouped")
        self.spark.catalog.clearCache()

    def extract_bidding(self, city, dt):
        '''
         提取加价有关数据，保存为parquet文件
         ods.pre_dispatch left join ods.system_dispatch、ods.personal_dispatch

        :param city: 城市简码
        :param dt: 当前处理的日期，对应hive表分区
        :return: None
        '''

        # 从dispatch_info提取加价id，存为临时表，用于过滤出响应的派单数据
        dep_where = "where dt={dt} and city='{city}'".format(dt=dt, city=city)

        # 根据bidding_id提取pre_dispatch记录 （加急日志不区分城市，用dispatch_info辅助过滤）
        where = "where dt={dt} and bidding_id in (select distinct bidding_id from ods.dispatch_info {dep_where})" \
            .format(dt=dt, dep_where=dep_where)

        # step 1. 提取pre_dispatch作为基准
        sql = """
        select
            unix_timestamp(a.datetime,'yyyy-MM-dd_HH:mm:ss') as datetime,
            a.bidding_id,
            b.productTypeId as pre_productTypeId,
            b.estimatePrice as pre_estimatePrice,
            b.availableDriverNum as pre_availableDriverNum,
            b.M as pre_M
        from (
            select
            datetime,
            bidding_id,
            regexp_replace(data, '\\\\\\\\\"', '"') as data
            from pre_dispatch
            {where}
        ) a
        lateral view json_tuple(a.data, 'productTypeId', 'estimatePrice', 'availableDriverNum', 'M', 'M1', 'M2', 'M3', 'a', 'b', 'c', 'T', 'N', 'L') b as productTypeId, estimatePrice, availableDriverNum, M, M1, M2, M3, a, b, c, T, N, L
        """.format(where=where)
        bidding_base_df = self.spark.sql(sql).dropDuplicates(['bidding_id'])

        # step 2. 过滤系统决策的订单，获取加价信息 （系统加价每一轮是相同倍率的）
        dep_where = "where dt={dt} and city='{city}' and decision_type=1".format(dt=dt, city=city)
        where = "where dt={dt} and bidding_id in (select distinct bidding_id from ods.dispatch_info {dep_where})" \
            .format(dt=dt, dep_where=dep_where)
        sql = """
        select
            a.bidding_id,
            a.round as end_bidding_round,
            b.totalMagnification as end_min_bidding_rate,
            b.totalMagnification as end_max_bidding_rate
            --(b.totalMagnification * (1 - b.commission_rate)) as end_min_bidding_rate,
            --(b.totalMagnification * (1 - b.commission_rate)) as end_max_bidding_rate
        from (
            select
            datetime,
            order_id,
            bidding_id,
            round,
            regexp_replace(data, '\\\\\\\\\"', '"') as json
            from system_dispatch
            {where} and order_id is not null
        ) a
        lateral view json_tuple(a.json, 'startBiddingRound', 'commission_rate', 'totalMagnification', 'M', 'N', 'L', 'O', 'D') b as startBiddingRound, commission_rate, totalMagnification, M, N, L, O, D
        """.format(where=where)
        sys_bidding_df = self.spark.sql(sql) \
            .orderBy("bidding_id", desc('end_bidding_round')) \
            .dropDuplicates(['bidding_id'])

        # step 3. 过滤人工决策的订单，获取加价倍率 （人工决策没一轮的倍率是有差异的）
        dep_where = "where dt={dt} and city='{city}' and decision_type=2".format(dt=dt, city=city)
        where = "where dt={dt} and bidding_id in (select distinct bidding_id from ods.dispatch_info {dep_where})" \
            .format(dt=dt, dep_where=dep_where)
        sql = """
        select
            a.bidding_id,
            a.round as end_bidding_round,
            b.commission_rate,
            b.driverData
        from (
            select
            datetime,
            order_id,
            bidding_id,
            round,
            regexp_replace(data, '\\\\\\\\\"', '"') as json
            from personal_dispatch
            {where} and order_id is not null
        ) a
        lateral view json_tuple(a.json, 'commission_rate', 'estimateDist', 'M', 'O', 'D', 'driverData') b as commission_rate, estimateDist, M, O, D, driverData
        """.format(where=where)

        # 提取人工计策司机倍率的udf方法
        def extract_driver_bidding_rate(row):
            '''
            spark map udf函数，从加价日志的json字符串中提取加价轮次及倍率信息
            :param row: 一行日志，其中driverData部分为json字符串
            :return: spark Row，包含：加价id，当前轮次，本轮最大倍率，本轮最小倍率
            '''
            try:
                dirvers = json.loads(row.driverData)
                bidding_rates = [val['totalMagnification'] for val in dirvers.values()]
            except:
                bidding_rates = [0.0]
            return Row(bidding_id=row.bidding_id,
                       end_bidding_round=row.end_bidding_round,
                       end_min_bidding_rate=min(bidding_rates),
                       end_max_bidding_rate=max(bidding_rates))

        personal_bidding_df = self.spark.sql(sql) \
            .orderBy("bidding_id", desc('end_bidding_round')) \
            .dropDuplicates(['bidding_id']) \
            .rdd.map(extract_driver_bidding_rate).toDF()

        # step 4. 加价倍率join （以pre_dispatch中的订单为基准，left join 系统决策加价 和 人工决策加价）
        bidding_df = sys_bidding_df.union(personal_bidding_df)
        outDF = bidding_base_df.join(bidding_df, bidding_base_df.bidding_id == bidding_df.bidding_id, 'left') \
            .drop(bidding_df.bidding_id)

        # step 5. 加价数据合并后，导出为parquet文件
        outDF.write.mode('overwrite').parquet(self.bidding_dump_path)
        self.log.info("dump merged bidding to {path}, dt={dt} size={size}"
                      .format(path=self.bidding_dump_path, dt=dt, size=outDF.count()))
        self.spark.catalog.clearCache()

    def merge_order_dispatch_bidding(self, city, dt):
        '''
         合并前3步extract的订单、派单。加价数据
         order left join dispatch、bidding

        :param city: 城市简码
        :param dt: 当前处理的日期，对应hive表分区
        :return: None
        '''

        # step 1. 读取parquet文件中聚合的中间结果数据
        dispatchDF = self.spark.sql("select * from `parquet`.`{path}`".format(path=self.dispatch_dump_path))
        orderDF = self.spark.sql("select * from `parquet`.`{path}`".format(path=self.order_dump_path))
        biddingDF = self.spark.sql("select * from `parquet`.`{path}`".format(path=self.bidding_dump_path))

        # step 2. order left join dispatch and bidding
        # 2-1 order <- dispatch
        dumpDF = orderDF.join(dispatchDF, orderDF.service_order_id == dispatchDF.service_order_id, 'left') \
            .drop(dispatchDF.service_order_id) \
            .drop(dispatchDF.city)
        # 2-2 order <- bidding
        dumpDF = dumpDF.join(biddingDF, dumpDF.bidding_id == biddingDF.bidding_id, 'left') \
            .drop(biddingDF.bidding_id)
        # allInOneDF.createOrReplaceTempView("v_allInOneDF")

        # step 3. 过滤加价日志和派单明细对不上（加价日志缺失）
        dumpDF = dumpDF.filter("not (final_max_bidding_rate>0 and end_bidding_round is null)")
        # df = self.spark.sql(
        #     "select * from v_allInOneDF where not (final_max_bidding_rate>0 and end_bidding_round is null)")
        # 决策类型改为从flag提取
        dumpDF = dumpDF.withColumn("decision_type_flag", expr("""
        case
            when (dispatch_flag & 1024) = 1024 then 'system_decision'
            when (dispatch_flag & 512) = 512 then 'manual_decision'
            else 'unkown'
        end
        """))
        # step 4. join后的数据导出为hive表的分区，为后续的高级特征计算做准备
        dumpDF.write.mode('overwrite').parquet(self.merged_dump_dir + "/{city}/{dt}.parquet".format(dt=dt, city=city))
        self.log.info("dump merged result to {path},dt={dt} size={size}"
                      .format(path=self.merged_dump_dir, dt=dt, size=dumpDF.count()))

    def add_advanced_features(self, city, dates):
        '''
         离线方式计算高阶特征，join到合并的数据中，主要增加下述几类特征：
         1、时空分割的供需数据 （时间对齐到5mins正点、地块火星坐标取geohash 5位）
         2、简单用户画像，用户历史加价成功的相关倍率信息
         3、地块特征的历史订单量、成单量（地块火星坐标取geohash 5位）

        :param city: 城市简码
        :param dt: 当前处理的日期，对应hive表分区
        :return: None
        '''

        # 第一天作为基准
        file_path = self.merged_dump_dir + "/{city}/{dt}.parquet".format(dt=dates[0], city=city)
        totalOrderDF = self.spark.sql("select * from `parquet`.`{path}`".format(path=file_path))
        for dt in dates[1:]:
            # 将第二天之后的union到一起
            file_path = self.merged_dump_dir + "/{city}/{dt}.parquet".format(dt=dt, city=city)
            dateDF = self.spark.sql("select * from `parquet`.`{path}`".format(path=file_path))
            totalOrderDF = totalOrderDF.union(dateDF)
        totalOrderDF.createOrReplaceTempView("v_bidding_order")
        self.spark.catalog.cacheTable("v_bidding_order")

        # step 1. 离线计算的供需数据
        # 需求：group by(期望上车点映射5位geohash + 下单时间按5分钟分箱) -> count(service_order_id)
        # 相对供给：group by(期望上车点映射5位geohash + 下单时间按5分钟分箱) -> sum(pre_availableDriverNum) / count(service_order_id)
        sql = """
        select
            service_order_id,
            order_create_time,
            expect_start_latitude,
            expect_start_longitude,
            pre_availableDriverNum
        from v_bidding_order
        """
        supply_demand_df = self.spark.sql(sql)

        # udf函数:
        # 1. latlng -> geohash， 5位, 正负2.5km
        # 2. timestamp 对其到 5分钟的整数倍
        def create_geo_time_key(timestamp, lat, lng):
            # geohash映射
            geohash = Geohash.encode(lat, lng, precision=5)
            # 时间round至5分钟整数（floor方式）
            tm = datetime.datetime.utcfromtimestamp(timestamp)
            tm = tm - datetime.timedelta(minutes=tm.minute % 5, seconds=tm.second, microseconds=tm.microsecond)
            ts_5mins = str(time.mktime(tm.timetuple()))
            # 组合key (时间+空间)
            key = geohash + "_" + ts_5mins[:-2]
            return key

        geo_time_key_udf = udf(create_geo_time_key, StringType())
        # 增加 时空的key，用作统计供需
        supply_demand_df.withColumn("geo_time_key", geo_time_key_udf(supply_demand_df['order_create_time'],
                                                                     supply_demand_df['expect_start_latitude'],
                                                                     supply_demand_df['expect_start_longitude'])) \
            .createOrReplaceTempView("v_geo_time")
        # 按时时空的key（即：geo_time_key）聚合供需数据
        sql = """
        select
            a.service_order_id as oid,
            b.abs_demand,
            b.relative_supply
        from v_geo_time a left join (
            select
                geo_time_key,
                count(1) as abs_demand, -- 区域内订单数作为绝度需求
                mean(pre_availableDriverNum) / (count(1) + 1) as relative_supply  -- 相对供给 = 可派司机 / 绝对需求
            from v_geo_time
            group by geo_time_key
        ) b
        on a.geo_time_key = b.geo_time_key
        """
        supply_demand_df = self.spark.sql(sql)

        # step 2. 离线计算用户画像（对加价倍率的敏感程度）
        # 1. 总体加价接受率：sum(加价且成单) / sum(加价订单)，float，建议分箱
        # 2. 平均成功加价倍率：avg(加价且成单的倍率)
        # 3. 25 % 成功加价倍率（近似下界）：percentile_approx(0.25)
        # 4. 50 % 成功加价倍率（近似均值）：percentile_approx(0.25)
        # 5. 75 % 成功加价倍率（近似上界）：percentile_approx(0.25)
        # 6. 拒绝所有加价flag：1 -> 拒绝所有加价, 0 -> 接受过任意加价，2 -> 无记录
        user_profile_df = UserFeature.stat_profile(self.spark, city, dates)

        ## step 3. 地块的订单特征（是否热门区域），按5位geohash映射，做区域内统计
        # 1. 区域内订单量（ ~ 是否热门区域）
        # 2. 区域内加价订单量
        # 3. 区域内加价成单量
        # 4. 区域内总体加价占比：count(加价订单) / count(order_id)
        # 5. 区域内加价成单占比：count(加价且成单) / count(加价)
        sql = """
        select
            service_order_id,
            expect_start_latitude,
            expect_start_longitude,
            -- 提取星期几
            case date_format(cast(start_time as timestamp), 'EEEE')
                WHEN 'Monday' THEN 1
                WHEN 'Tuesday' THEN 2
                WHEN 'Wednesday' THEN 3
                WHEN 'Thursday' THEN 4
                WHEN 'Friday' THEN 5
                WHEN 'Saturday' THEN 6
                WHEN 'Sunday' THEN 7
                ELSE null
            END as day_of_week,
            -- 提取小时
            cast(date_format(cast(start_time as timestamp), 'HH') as int) as hour_of_day,
            1 as order_num,
            case when final_driver_bidding_rate>0 or final_max_bidding_rate>0 then 1 else 0 end as is_bidding,
            case when final_decision_driver_id>0 and final_driver_bidding_rate>0 then 1 else 0 end as is_bidding_ok
        from v_bidding_order
        """
        geo_stat_df = self.spark.sql(sql)
        geohash5_udf = udf(lambda lat, lng: Geohash.encode(lat, lng, precision=5), StringType())
        geo_stat_df.withColumn("expect_geohash",
                               geohash5_udf(geo_stat_df['expect_start_latitude'],
                                            geo_stat_df['expect_start_longitude'])) \
            .createOrReplaceTempView("v_geo")
        # 按geohash 5位聚合区域内的订单量、加价订单、加价成单
        # TODO：数据稀疏，暂不区分星期和时间段
        sql = """
        select
            expect_geohash as geo,
            geo_order_count,
            geo_bidding_count,
            geo_bidding_ok_count,
            case when geo_bidding_pct is null then -1 else geo_bidding_pct end  as geo_bidding_pct,
            case when geo_bidding_ok_pct is null then -1 else geo_bidding_ok_pct end as geo_bidding_ok_pct
        from (
            select
                expect_geohash,
                sum(order_num) as geo_order_count,
                sum(is_bidding) as geo_bidding_count,
                sum(is_bidding_ok) as geo_bidding_ok_count,
                sum(is_bidding) / sum(order_num) as geo_bidding_pct,
                sum(is_bidding_ok) / sum(is_bidding) as geo_bidding_ok_pct
            from v_geo
            group by expect_geohash
        ) tmp
        """
        geo_stat_df = self.spark.sql(sql)

        # step 4. 合并计算出的特征
        dumpDF = self.spark.sql("select * from v_bidding_order")

        # join 供需数据
        dumpDF = dumpDF.join(supply_demand_df, dumpDF.service_order_id == supply_demand_df.oid, "left").drop(
            supply_demand_df.oid)
        # join 用户画像数据
        dumpDF = dumpDF.join(user_profile_df, dumpDF.user_id == user_profile_df.uid, "left").drop(user_profile_df.uid)
        # join 上车位置统计指标
        dumpDF = dumpDF.withColumn("expect_geohash",
                                   geohash5_udf(dumpDF['expect_start_latitude'], dumpDF['expect_start_longitude']))
        dumpDF = dumpDF.join(geo_stat_df, dumpDF.expect_geohash == geo_stat_df.geo, "left").drop(geo_stat_df.geo)
        # 过滤掉无供需数据的订单
        dumpDF = dumpDF.filter("avg_can_dispatch>=0 and abs_demand>=0 and relative_supply>=0")

        dumpDF.createOrReplaceTempView("v_training")
        self.spark.catalog.cacheTable("v_training")
        # 释放已不需要的临时表
        self.spark.catalog.uncacheTable("v_bidding_order")

        # step 5. 条件过滤，导出到hive
        # 设置判定如下：
        # 5-1 最终加价倍率（来自加价日志，用户不一定接受）
        bidding_rate_gt0 = "end_max_bidding_rate>0"
        bidding_rate_eq0 = "(end_max_bidding_rate=0 or end_max_bidding_rate is null)"
        # 5-2 最终决策情况
        decision_ok = "final_decision_driver_id>0"
        decision_failed = "(final_decision_driver_id=0 or final_decision_driver_id is null)"
        # 5-3 是否有司机接单
        accept_ok = "first_accept_time>0"
        accept_failed = "(first_accept_time>0 is null or first_accept_time=0)"
        # 5-4 最终决策成功的加价倍率
        bidding_decision_ok = "final_driver_bidding_rate>0"
        # 导出到hive
        self.spark.sql("drop table if exists tmp.bidding_training")
        sql = """
        create table tmp.bidding_training stored as orc as
        select
            *,
            case
                -- 决策成功，取决策倍率
                when final_decision_driver_id >0 then final_driver_bidding_rate
                -- 决策不成功，触发了加价, 取加价系统末轮倍率
                when {decision_failed} and final_max_bidding_rate>0 then end_max_bidding_rate
                -- 决策不成功，触发了加价，但该加价倍率没有派发（用户看到就关掉了）
                when {decision_failed} and end_max_bidding_rate>0 then end_max_bidding_rate
                else 0
            end as bidding_rate_adjust, -- 末轮加价倍率，根据决策情况区分
            case
                when {decision_ok} and {bidding_decision_ok} then 'bidding_decision_ok'
                when {bidding_rate_gt0} and {decision_failed} then 'bidding_decision_failed'
                when {bidding_rate_eq0} and {decision_ok} then 'no_bidding_decision_ok'
                when {bidding_rate_eq0} and {decision_failed} then 'no_bidding_decision_failed'
                else 'unkown'
            end as label
        from v_training
        """.format(decision_ok=decision_ok, decision_failed=decision_failed, bidding_decision_ok=bidding_decision_ok,
                   bidding_rate_gt0=bidding_rate_gt0, bidding_rate_eq0=bidding_rate_eq0)
        self.spark.sql(sql)
        self.spark.catalog.uncacheTable("v_training")
        self.spark.catalog.clearCache()
        self.log.info("dump training data to hive table tmp.bidding_training")


def main():
    ' 将ods_service_order_charge导入本地hive '
    log = get_logger(NAME)

    yesterday = datetime.datetime.now(tz=pytz.timezone('Asia/Shanghai')) - datetime.timedelta(days=1)
    sevenDaysAgo = yesterday - datetime.timedelta(days=8)

    parser = OptionParser()
    parser.add_option('--city', dest="city", type=str, help='清洗的城市简码（必须填写）')
    parser.add_option('--start_date', dest="start_date", type=str, default=yesterday.strftime("%Y%m%d"), help='批量的开始日期')
    parser.add_option('--end_date', dest="end_date", type=str, default=sevenDaysAgo.strftime("%Y%m%d"),
                      help='批量的结束日期（不包括这一天）')
    parser.add_option('--skip_merge', action="store_true", dest="is_skip_merge", default=False)

    (opt, args) = parser.parse_args()
    if opt.city is None or len(opt.city) == 0:
        raise ValueError("city code must be input with --city option")

    # 获取日期list
    start_date = datetime.datetime.strptime(opt.start_date, '%Y%m%d') + datetime.timedelta(hours=8)
    end_date = datetime.datetime.strptime(opt.end_date, '%Y%m%d') + datetime.timedelta(hours=8)
    days = (end_date - start_date).days
    dates = map(lambda x: x.strftime('%Y%m%d'), [start_date + datetime.timedelta(days=i) for i in range(days)])
    log.info("city={city} dates={dates}".format(city=opt.city, dates=dates))

    # 构造数据清洗的对象
    extractor = BiddingTrainingPreparator()

    start_time = time.time()
    log.info("city={city} start={start} end={end}".format(city=opt.city, start=opt.start_date, end=opt.end_date))

    # 按天完成order, dispatch, bidding的计算和merge，导出到临时目录（按city划分目录，按日期划分文件）
    # 备注：这个for训练merge完后是导出为parquet文件（每天1个），若已经导出过可以跳过此步骤
    # if not opt.is_skip_merge:
    #     for dt in dates:
    #         batch_start_time = time.time()
    #         extractor.join_datasource(opt.city, dt)
    #         log.info("dt={dt} is merged, eplased: {t}".format(dt=dt, t=str(time.time() - batch_start_time)))
    #     log.info("merge base data by day complete!")

    # 进行高级特征计算（目前使用所有数据，未来可以根据需要取最近1个月即可）
    extractor.add_advanced_features(opt.city, dates)

    # 退出
    extractor.destroy()

    log.info("all is done! total elasped: " + str(time.time() - start_time))

    exit(0)


if __name__ == '__main__':
    main()
