# -*- coding: utf-8 -*-

'''
hive表的desc输出，用来解析表结构的meta信息，供BdpImportManager使用
'''

# size = 87
OrderDesc = """
service_order_id        bigint
product_type_id         int
fixed_product_id        int
is_asap                 tinyint
source                  int
platform                tinyint
status                  tinyint
rc_status               tinyint
end_status              tinyint
abnormal_mark           tinyint
flag                    bigint
account_id              bigint
user_id                 bigint
user_phone              string
passenger_name          string
passenger_phone         string
corporate_id            bigint
corporate_dept_id       int
city                    string
reason_id               int
flight_number           string
create_time             int
update_time             int
init_time               int
select_car_time         int
arrival_time            int
cancel_time             int
car_id                  int
car_type_id             int
car_type_ids            string                  mixed car_type support
car_type                string
car_brand               string
driver_id               int
driver_phone            string
driver_name             string
vehicle_number          string
expect_start_time       int
expect_end_time         int
start_time              int
end_time                int
confirm_time            int
start_position          string
start_address           string
end_position            string
end_address             string
expect_start_latitude   double
expect_start_longitude  double
expect_end_latitude     double
expect_end_longitude    double
start_latitude          double
start_longitude         double
end_latitude            double
end_longitude           double
payment                 string
refund_status           tinyint
pay_method              tinyint
pay_status              tinyint
first_recharge_transaction_id   bigint
first_recharge_amount   decimal(10,2)
coupon_member_id        bigint
coupon_name             string
coupon_type             tinyint
coupon_facevalue        decimal(8,2)
discount                decimal(8,2)
version                 int
fee_version             int
balance_status          tinyint
payable                 tinyint
total_amount            decimal(8,2)
deposit                 decimal(8,2)
loan_in_credit          decimal(10,2)
pay_amount              decimal(10,2)
min_amount              decimal(10,2)
origin_amount           decimal(8,2)
origin_sharing_amount   decimal(10,2)
sharing_amount          decimal(10,2)
actual_time_length      int
dependable_distance     int
mileage                 int
system_distance         int
alitongxin_secret_no_x  string
alitongxin_subs_id      bigint
alitongxin_status       tinyint
passenger_session_id    string
last_operator           string
time_length             int
operation               string
dt                      int
"""

# size = 53
OrderExtDesc = """
service_order_id        bigint
operator_id             int
user_type               tinyint
sms                     string
create_order_longitude  double
create_order_latitude   double
confirm_latitude        double
confirm_longitude       double
arrive_latitude         double
arrive_longitude        double
src_city_name           string
dst_city_name           string
dest_city               string
dispatch_driver_ids     string
change_driver_reason_id int
before_cancel_status    tinyint
app_version             string
driver_version          string
balance_time            int
balance_result          tinyint
preauth_status          tinyint
extra_amount            decimal(8,2)
predict_amount          decimal(10,2)
night_amount            decimal(10,2)
driver_amount           decimal(8,2)
predict_origin_amount   decimal(10,2)
predict_pay_amount      decimal(10,2)
additional_time_amount  decimal(8,2)
highway_amount          decimal(8,2)
parking_amount          decimal(8,2)
addons_amount           decimal(8,2)
addons_amount_src       string
other_amount            decimal(8,2)
runtime                 int
total_distance          int
deadhead_distance       int
is_night                tinyint
regulatepan_amount      decimal(8,2)
regulatedri_amount      decimal(8,2)
regulatepan_reason      string
regulatedri_reason      string
regulate_amount         decimal(8,2)
estimate_snap           string
app_msg                 string
comment                 string
ip                      string
order_port              int
confirm_ip              string
confirm_port            string
create_time             int
update_time             int
operation               string
dt                      int
"""

# size = 36
DispatchInfoDesc = """
datetime                string                  from deserializer
service_order_id        bigint                  from deserializer
dispatch_count          smallint                from deserializer
response_count          smallint                from deserializer
accept_count            smallint                from deserializer
flag                    int                     from deserializer
dispatch_time           int                     from deserializer
decision_time           int                     from deserializer
contribution            int                     from deserializer
expect_decision_time    int                     from deserializer
dispatch_template_id    int                     from deserializer
template_snapshot       string                  from deserializer
status                  tinyint                 from deserializer
dispatch_type           tinyint                 from deserializer
decision_type           tinyint                 from deserializer
round                   tinyint                 from deserializer
batch                   tinyint                 from deserializer
create_time             int                     from deserializer
update_time             int                     from deserializer
estimate_time           int                     from deserializer
can_dispatch_count      smallint                from deserializer
user_id                 bigint                  from deserializer
user_level              tinyint                 from deserializer
user_name               string                  from deserializer
user_gender             string                  from deserializer
add_price_redispatch    smallint                from deserializer
add_price_info          string                  from deserializer
decision_driver_id      bigint                  from deserializer
decision_car_type_id    tinyint                 from deserializer
bidding_id              bigint                  from deserializer
bidding_rate            float                   from deserializer
driver_bidding_rate     float                   from deserializer
estimate_price          int                     from deserializer
driver_estimate_price   int                     from deserializer
city                    string                  from deserializer
dt                      int
"""

# size = 35
DispatchDetailDesc = """
datetime                string                  from deserializer
service_order_id        bigint                  from deserializer
round                   tinyint                 from deserializer
batch                   tinyint                 from deserializer
flag                    int                     from deserializer
driver_id               bigint                  from deserializer
distance                int                     from deserializer
dispatch_time           int                     from deserializer
dispatch_lat            float                   from deserializer
dispatch_lng            float                   from deserializer
dispatch_total_rate     float                   from deserializer
dispatch_snapshot       string                  from deserializer
response_time           int                     from deserializer
accept_status           tinyint                 from deserializer
response_lat            float                   from deserializer
response_lng            float                   from deserializer
response_distance       float                   from deserializer
response_time_length    int                     from deserializer
decision_time           int                     from deserializer
decision_total_rate     float                   from deserializer
decision_result         tinyint                 from deserializer
decision_failure_reason tinyint                 from deserializer
decision_msg_snapshot   string                  from deserializer
subtract_amount         int                     from deserializer
add_price_set           string                  from deserializer
response_snapshot       string                  from deserializer
is_assigned             tinyint                 from deserializer
route_distance          int                     from deserializer
route_time_length       int                     from deserializer
distance_time_length    int                     from deserializer
driver_bidding_rate     float                   from deserializer
driver_estimate_price   int                     from deserializer
city                    string                  from deserializer
dt                      int
hour                    int
"""

# size = 5
BiddingAccessDesc = """
datetime                string
tag                     string
operation               string
request                 string
result                  string
dt                      int
"""

# 派单前获取加价
# size = 3
PreDispatch = """
datetime                string
bidding_id              bigint
data                    string
dt                      int
"""

# 系统决策
SystemDispatch = """
datetime                string
order_id                bigint
bidding_id              bigint
round                   int
data                    string
dt                      int
"""

# 人工决策
PersonalDispatch = """
datetime                string
order_id                bigint
bidding_id              bigint
round                   int
data                    string
dt                      int
"""

# 订单轨迹历史记录
OrderTrack = """
order_track_id          bigint
order_id                bigint
action_name             string
username                string
dateline                int
operator                string
ip                      string
extra                   string
add_price_amount        int
create_time             int
update_time             int
operation               string
dt                      int
"""

# 司机端调用频次 （目前供异常订单分析使用）
DriverApiAccessGray = """
datetime                string
ip_1                    string
ip_2                    string
driver_id               int
dt                      int
"""