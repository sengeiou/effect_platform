#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
## 成交数据周期：11（1天gmv，1天alipay），13（1天gmv，3天alipay），17（1天gmv，7天alipay），33（3天gmv，3天alipay）
period=11
src='taobao'
if [ $# == 1 ];then
    ydate=$1
fi
if [ $# == 2 ];then
    ydate=$1
    period=$2
fi
if [ $# == 3 ];then
    ydate=$1
    period=$2
    src=$3
fi
Dir=`dirname $0`
source $HOME/linezing/effect_platform/config/set_env.conf
gmv_period=`expr $period / 10 - 1`
alipay_period=`expr $period % 10 - 1`
sdate=`date -d "$ydate" +%Y%m%d`
gmv_edate=`date -d "$ydate +$gmv_period days" +%Y%m%d`
alipay_edate=`date -d "$ydate +$alipay_period days" +%Y%m%d`
if [ $gmv_period -ge $alipay_period ];then
    edate=$gmv_edate
else
    edate=$alipay_edate
fi
##########################################################################
# 作者: feiqiong.dpf
# 邮箱：feiqiong.dpf@taobao.com
# 日期：2012-08-29
# ------------------------------------------------------------------------
# 功能：生成月光宝盒-成交数据，useful_extra为空,extra内容：order_id, parent_id, is_wireless
# 上游：r_gmv_alipay, lz_dim_sellers
# ------------------------------------------------------------------------
#
##########################################################################

$Hive <<EOF
CREATE TEMPORARY FUNCTION date_format AS 'com.taobao.hive.udf.UDFDateFormat';
insert overwrite table lz_fact_ep_trade_info partition (dt='${sdate}', round='${period}', logsrc='${src}')
select
    coalesce(unix_timestamp(a.closingdate), cast(-99 as bigint)) as gmv_trade_timestamp,
    coalesce(cast(b.shop_id as bigint), cast(-99 as bigint)) as shop_id,
    a.auction_id as auction_id,
    a.user_id as user_id,
    case b.shop_type 
        when cast(1 as bigint) then cast(2 as bigint) 
        when cast(0 as bigint) then cast(1 as bigint) 
        else cast(0 as bigint) end as ali_corp,
    a.gmv_trade_num as gmv_trade_num,
    a.gmv_trade_amt as gmv_trade_amt,
    a.gmv_auction_num as gmv_auction_num,
    a.alipay_trade_num as alipay_trade_num,
    a.alipay_trade_amt as alipay_trade_amt,
    a.alipay_auction_num as alipay_auction_num,
    '' as useful_extra,
    concat_ws('\002', order_id, parent_id, cast(is_wireless as string), closingdate, gmt_receive_pay) as extra
from 
(    
    select
        ${ydate} as thedate,
        id as order_id,
        parent_id as parent_id,
        buyer_id as user_id,
        seller_id as seller_id,
        auction_id as auction_id,
        sum(case when gmv='gmv' then 1 else 0 end) as gmv_trade_num,
        sum(case when gmv='gmv' then gmv_fee else 0.0 end) as gmv_trade_amt,
        sum(case when gmv='gmv' then cast (buy_amount as bigint) else cast(0 as bigint) end) as gmv_auction_num,
        sum(case when alipay='alipay' then 1 else 0 end) as alipay_trade_num,
        sum(case when alipay='alipay' then ali_fee else 0.0 end) as alipay_trade_amt,
        sum(case when alipay='alipay' then cast (buy_amount as bigint) else cast(0 as bigint) end) as alipay_auction_num,
        max(case when gmv='gmv' then gmv_date else '' end) as closingdate,
        max(case when alipay='alipay' then ali_date else '' end) as gmt_receive_pay,
        case 
            when attributes like '%wap:1%' or attributes like '%cosys:wap%'
            then cast(1 as bigint)
            else cast(0 as bigint)
        end as is_wireless
    from r_gmv_alipay
    where pt>='${sdate}' and pt<='${edate}'
    and not biz_type in (100, 800)
    and ((gmv='gmv' and date_format(gmv_date, 'yyyy-MM-dd HH:mm:ss', 'yyyyMMdd') >= $sdate and date_format(gmv_date, 'yyyy-MM-dd HH:mm:ss', 'yyyyMMdd') <= $gmv_edate)
    or (alipay='alipay' and date_format(gmv_date, 'yyyy-MM-dd HH:mm:ss', 'yyyyMMdd') >= $sdate and date_format(gmv_date, 'yyyy-MM-dd HH:mm:ss', 'yyyyMMdd') <= $gmv_edate and date_format(ali_date, 'yyyy-MM-dd HH:mm:ss', 'yyyyMMdd') >= $sdate and date_format(ali_date, 'yyyy-MM-dd HH:mm:ss', 'yyyyMMdd') <= $alipay_edate))
    group by 
        id,
        parent_id,
        buyer_id,
        seller_id,
        auction_id,
        attributes
)a
left outer join lz_dim_sellers b
on  b.dt = '${edate}'
    and a.seller_id = b.seller_id
;
EOF
