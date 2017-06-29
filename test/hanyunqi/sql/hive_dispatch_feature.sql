CREATE TABLE `tmp`.`dispatch_feature`(
    `service_order_id` bigint COMMENT '订单号',
    `redispatch_id` int COMMENT '改派ID，第一次改派记1',
    `bidding_id` bigint COMMENT '加价ID',

-- 自变量
    `dispatch_start_time` int COMMENT '开始派单的时间',
    `dispatch_end_time` int COMMENT '派单结束时间',
    `product_type_id` int COMMENT '产品类型ID',
    `car_type_id` int COMMENT '车型ID',
    `decision_type` int COMMENT '决策类型',
    `city` string COMMENT '城市',

    `start_position` string COMMENT '上车地点',
    `end_position` string COMMENT '下车地点',
    `start_address` string COMMENT '上车地址',
    `end_address` string COMMENT '下车地址',
    `start_latitude` double COMMENT '上车经度',
    `start_longitude` double COMMENT '上车纬度',
    `end_latitude` double COMMENT '下车经度',
    `end_longitude` double COMMENT '下车纬度',

    `week` int COMMENT '星期，1~7',
    `hour` int COMMENT '小时，0~23',
    `date_time` string COMMENT '日期',
    `weather` string COMMENT '天气',

    `working_drivers` int COMMENT '周围可服务的司机数量',
    `estimate_price` int COMMENT '预估价格，分',
    `estimate_distance` int COMMENT '预估距离，米',

-- 因变量
    `decision_round` int COMMENT '决策时的轮次',
    `decision_batch` int COMMENT '决策时的批次',
    `bidding_rate` double COMMENT '加价倍率',
    `driver_bidding_rate` double COMMENT '司机加价倍率',
    `dispatch_duration` int COMMENT '派单时长，秒',
    `decision_status` int COMMENT '决策状态，成功、失败……',

-- 其他
    `user_id` int COMMENT '乘客ID',
    `decision_driver_id` int COMMENT '司机ID'
 )
 PARTITIONED BY ( `dt` int)
 STORED AS PARQUET;

--修复分区
MSCK REPAIR TABLE tmp.dispatch_feature;
