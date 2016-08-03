#!/bin/bash

ydate=`date -d -1days +%Y%m%d`
if [ $# == 1 ];then
    ydate=$1
fi
Dir=`dirname $0`
source $HOME/linezing/effect_platform/config/set_env.conf


$Hive <<EOF
CREATE TEMPORARY FUNCTION trunc AS 'com.taobao.hive.udf.UDFTrunc';
insert overwrite table lz_fact_ep_etao_outside_order partition (dt=${ydate})
select
    coalesce(unix_timestamp(gmt_create_ori), cast(-99 as bigint)) as gmv_create_ori_ts,
    trade_no as trade_no,
    trade_track_info as trade_track_info,
    seller_id as seller_id,
    trade_track_info as auction_id, 
    buyer_id as user_id,
    cast((case when is_order = 'y' then 1 else 0 end) as bigint)  as  gmv_trade_num,
    (case when is_order = 'y' then cast(total_fee as double) else cast(0 as double) end)  as  gmv_trade_amt,
    cast((case when is_pay = 'y' then 1 else 0 end) as bigint)  as  pay_trade_num,
    (case when is_pay = 'y' then cast(total_fee as double) else cast(0 as double) end)  as  pay_trade_amt
from s_dw_sh_etao_pay_detail
where  ds='${ydate}' 
    and (is_order = 'y' or is_pay = 'y')
    and trade_track_info is not null
; 
EOF

if [ $? -ne 0 ];then
    exit 2
fi
