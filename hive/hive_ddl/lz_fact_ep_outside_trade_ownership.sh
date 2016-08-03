#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：feiqiong.dpf
# 邮箱：feiqiong.dpf@taobao.com
# 日期：20120706
# ------------------------------------------------------------------------
# 功能：
# 上游：
# ------------------------------------------------------------------------

$Hive << EOF
drop table lz_fact_ep_outside_trade_ownership;
create external table lz_fact_ep_outside_trade_ownership
(
    analyzer_id           bigint,
    plan_id              bigint,
    ts                   string,
    url                  string,
    refer                string,
    cookie               string,
    session              string,
    src                  string,
    visit_id               string,
    shop_id              string,
    auction_id           string,
    ali_corp             bigint,
    index_root_path      string,
    is_leaf              bigint,
    trade_track_info     string,
    outside_buyerid    string,
    outside_gmv_amt   double,
    outside_gmv_trade_num double,
    outside_alipay_amt   double,
    outside_alipay_trade_num double
)
partitioned by (dt string, planid string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_outside_trade_ownership';
EOF
