#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：南迦
# 邮箱：nanjia.lj@taobao.com
# 日期：20130123
# ------------------------------------------------------------------------
# 功能：
# 上游：
# ------------------------------------------------------------------------

$Hive <<EOF
drop table lz_fact_ep_client_log;

create external table lz_fact_ep_client_log
(
   time_stamp           string comment '时间戳',
   url                  string comment 'page',
   refer_url            string comment 'arg1',
   seller_id            string comment '卖家ID',
   auction_id           string comment '解析自args',
   user_id              string comment '用户nickname',
   cookie               string comment 'imei + imsi',
   session              string comment '空',
   visit_id              string comment '空',
   useful_extra         string comment 'args 展开',
   extra                string comment '空'
)
partitioned by (dt string, logtype string )
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_client_log';

EOF
