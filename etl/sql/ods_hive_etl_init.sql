-- kafka提取的hive临时表数据，存为parquet格式

CREATE DATABASE ods;

--###########################################################################
--  service_order
--###########################################################################

--订单表
CREATE TABLE `ods`.`service_order`(
  `service_order_id` bigint,
  `product_type_id` int,
  `fixed_product_id` int,
  `is_asap` tinyint,
  `source` int,
  `platform` tinyint,
  `status` tinyint,
  `rc_status` tinyint,
  `end_status` tinyint,
  `abnormal_mark` tinyint,
  `flag` bigint,
  `account_id` bigint,
  `user_id` bigint,
  `user_phone` string,
  `passenger_name` string,
  `passenger_phone` string,
  `corporate_id` bigint,
  `corporate_dept_id` int,
  `city` string,
  `reason_id` int,
  `flight_number` string,
  `create_time` int,
  `update_time` int,
  `init_time` int,
  `select_car_time` int,
  `arrival_time` int,
  `cancel_time` int,
  `car_id` int,
  `car_type_id` int,
  `car_type_ids` string COMMENT 'mixed car_type support',
  `car_type` string,
  `car_brand` string,
  `driver_id` int,
  `driver_phone` string,
  `driver_name` string,
  `vehicle_number` string,
  `expect_start_time` int,
  `expect_end_time` int,
  `start_time` int,
  `end_time` int,
  `confirm_time` int,
  `start_position` string,
  `start_address` string,
  `end_position` string,
  `end_address` string,
  `expect_start_latitude` double,
  `expect_start_longitude` double,
  `expect_end_latitude` double,
  `expect_end_longitude` double,
  `start_latitude` double,
  `start_longitude` double,
  `end_latitude` double,
  `end_longitude` double,
  `payment` string,
  `refund_status` tinyint,
  `pay_method` tinyint,
  `pay_status` tinyint,
  `first_recharge_transaction_id` bigint,
  `first_recharge_amount` decimal(10,2),
  `coupon_member_id` bigint,
  `coupon_name` string,
  `coupon_type` tinyint,
  `coupon_facevalue` decimal(8,2),
  `discount` decimal(8,2),
  `version` int,
  `fee_version` int,
  `balance_status` tinyint,
  `payable` tinyint,
  `total_amount` decimal(8,2),
  `deposit` decimal(8,2),
  `loan_in_credit` decimal(10,2),
  `pay_amount` decimal(10,2),
  `min_amount` decimal(10,2),
  `origin_amount` decimal(8,2),
  `origin_sharing_amount` decimal(10,2),
  `sharing_amount` decimal(10,2),
  `actual_time_length` int,
  `dependable_distance` int,
  `mileage` int,
  `system_distance` int,
  `alitongxin_secret_no_x` string,
  `alitongxin_subs_id` bigint,
  `alitongxin_status` tinyint,
  `passenger_session_id` string,
  `last_operator` string,
  `time_length` int,
  `operation` string
)
PARTITIONED BY ( `dt` int)
STORED AS PARQUET;

--修复分区
MSCK REPAIR TABLE ods.service_order;

--###########################################################################
--  service_order_ext
--###########################################################################


--订单扩展表
CREATE TABLE `ods`.`service_order_ext`(
  `service_order_id` bigint,
  `operator_id` int,
  `user_type` tinyint,
  `sms` string,
  `create_order_longitude` double,
  `create_order_latitude` double,
  `confirm_latitude` double,
  `confirm_longitude` double,
  `arrive_latitude` double,
  `arrive_longitude` double,
  `src_city_name` string,
  `dst_city_name` string,
  `dest_city` string,
  `dispatch_driver_ids` string,
  `change_driver_reason_id` int,
  `before_cancel_status` tinyint,
  `app_version` string,
  `driver_version` string,
  `balance_time` int,
  `balance_result` tinyint,
  `preauth_status` tinyint,
  `extra_amount` decimal(8,2),
  `predict_amount` decimal(10,2),
  `night_amount` decimal(10,2),
  `driver_amount` decimal(8,2),
  `predict_origin_amount` decimal(10,2),
  `predict_pay_amount` decimal(10,2),
  `additional_time_amount` decimal(8,2),
  `highway_amount` decimal(8,2),
  `parking_amount` decimal(8,2),
  `addons_amount` decimal(8,2),
  `addons_amount_src` string,
  `other_amount` decimal(8,2),
  `runtime` int,
  `total_distance` int,
  `deadhead_distance` int,
  `is_night` tinyint,
  `regulatepan_amount` decimal(8,2),
  `regulatedri_amount` decimal(8,2),
  `regulatepan_reason` string,
  `regulatedri_reason` string,
  `regulate_amount` decimal(8,2),
  `estimate_snap` string,
  `app_msg` string,
  `comment` string,
  `ip` string,
  `order_port` int,
  `confirm_ip` string,
  `confirm_port` string,
  `create_time` int,
  `update_time` int,
  `operation` string
)
PARTITIONED BY ( `dt` int)
STORED AS PARQUET;

--修复分区
MSCK REPAIR TABLE ods.service_order_ext;


--###########################################################################
--  dispatch_info
--###########################################################################

--派单概要
CREATE TABLE `ods`.`dispatch_info`(
  `datetime` string,
  `service_order_id` bigint,
  `dispatch_count` smallint,
  `response_count` smallint,
  `accept_count` smallint,
  `flag` int,
  `dispatch_time` int,
  `decision_time` int,
  `contribution` int,
  `expect_decision_time` int,
  `dispatch_template_id` int,
  `template_snapshot` string,
  `status` tinyint,
  `dispatch_type` tinyint,
  `decision_type` tinyint,
  `round` tinyint,
  `batch` tinyint,
  `create_time` int,
  `update_time` int,
  `estimate_time` int,
  `can_dispatch_count` smallint,
  `user_id` bigint,
  `user_level` tinyint,
  `user_name` string,
  `user_gender` string,
  `add_price_redispatch` smallint,
  `add_price_info` string,
  `decision_driver_id` bigint,
  `decision_car_type_id` tinyint,
  `bidding_id` bigint,
  `bidding_rate` double,
  `driver_bidding_rate` double,
  `estimate_price` int,
  `driver_estimate_price` int,
  `city` string
)
PARTITIONED BY ( `dt` int)
STORED AS PARQUET;

--修复分区
MSCK REPAIR TABLE ods.dispatch_info;


--###########################################################################
--  dispatch_detail
--###########################################################################


--派单明细
CREATE TABLE `ods`.`dispatch_detail`(
  `datetime` string,
  `service_order_id` bigint,
  `round` tinyint,
  `batch` tinyint,
  `flag` int,
  `driver_id` bigint,
  `distance` int,
  `dispatch_time` int,
  `dispatch_lat` double,
  `dispatch_lng` double,
  `dispatch_total_rate` double,
  `dispatch_snapshot` string,
  `response_time` int,
  `accept_status` tinyint,
  `response_lat` double,
  `response_lng` double,
  `response_distance` double,
  `response_time_length` int,
  `decision_time` int,
  `decision_total_rate` double,
  `decision_result` tinyint,
  `decision_failure_reason` tinyint,
  `decision_msg_snapshot` string,
  `subtract_amount` int,
  `add_price_set` string,
  `response_snapshot` string,
  `is_assigned` tinyint,
  `route_distance` int,
  `route_time_length` int,
  `distance_time_length` int,
  `driver_bidding_rate` double,
  `driver_estimate_price` int,
  `city` string
)
PARTITIONED BY (
  `dt` int,
  `hour` int)
STORED AS PARQUET;

--修复分区
MSCK REPAIR TABLE ods.dispatch_detail;


--###########################################################################
--  bidding
--###########################################################################


--加价表
CREATE TABLE `ods`.`bidding_access`(
  `timestamp` string,
  `tag` string,
  `operation` string,
  `request` string,
  `result` string
) PARTITIONED BY ( `dt` int)
ROW FORMAT DELIMITED
STORED AS PARQUET;


--###########################################################################
--  pre_dispatch  派单前获取加价信息
--###########################################################################

CREATE TABLE `ods`.`pre_dispatch`(
    `datetime` string,
    `bidding_id` bigint,
    `data` string
) PARTITIONED BY ( `dt` int)
STORED AS PARQUET;


--###########################################################################
--  system_dispatch  系统决策
--###########################################################################

CREATE TABLE `ods`.`system_dispatch`(
    `datetime` string,
    `order_id` bigint,
    `bidding_id` bigint,
    `round` int,
    `data` string
) PARTITIONED BY ( `dt` int)
STORED AS PARQUET;


--###########################################################################
--  personal_dispatch  人工决策
--###########################################################################

CREATE TABLE `ods`.`personal_dispatch`(
    `datetime` string,
    `order_id` bigint,
    `bidding_id` bigint,
    `round` int,
    `data` string
) PARTITIONED BY ( `dt` int)
STORED AS PARQUET;


--###########################################################################
--  order_track  订单变更轨迹
--###########################################################################

CREATE TABLE `ods`.`order_track`(
    `order_track_id` bigint,
    `order_id` bigint,
    `action_name` string,
    `username` string,
    `dateline` int,
    `operator` string,
    `ip` string,
    `extra` string,
    `add_price_amount` int,
    `create_time` int,
    `update_time` int,
    `operation` string
) PARTITIONED BY ( `dt` int)
STORED AS PARQUET;


--###########################################################################
--  driver_api_access_gray  司机端api新接口
--###########################################################################

CREATE TABLE `ods`.`driver_api_access_gray`(
    `datetime` string,
    `ip_1` string,
    `ip_2` string,
    `driver_id` int
) PARTITIONED BY ( `dt` int)
STORED AS PARQUET;