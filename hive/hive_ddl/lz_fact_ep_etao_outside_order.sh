#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

$Hive <<EOF
drop table lz_fact_ep_etao_outside_order;
create external table lz_fact_ep_etao_outside_order 
(
    gmv_create_ori_ts   bigint,
    trade_no            string,
    trade_track_info    string,
    seller_id           string,
    auction_id          string,
    user_id             string,
    gmv_trade_num       bigint,
    gmv_trade_amt       double,
    pay_trade_num       bigint,
    pay_trade_amt       double
)
partitioned by (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_etao_outside_order'
;

EOF
