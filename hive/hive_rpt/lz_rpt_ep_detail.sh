#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
if [ $# != 0 ];then
    ydate=$1
fi
Dir=`dirname $0`
source $HOME/linezing/effect_platform/config/set_env.conf

yydate=`date -d "$ydate -1days" +%Y%m%d`

$Hive <<EOF
set hive.exec.compress.output=false;
add jars $HOME/linezing/tools/hive_udf/p4plz_udf.jar;
create temporary function double_roundhalfup as 'com.taobao.lzdata.hive.udf.DoubleRoundHalfUp';
insert overwrite table lz_rpt_ep_detail partition(dt=$ydate)
select
    plan_id,
    analyzer_id,
    day,
    dim,
    sum(effect_pv) as effect_pv,
    cast(sum(case when effect_pv > 0 then 1 else 0 end) as double) as effect_uv,
    sum(effect_click_pv) as effect_click_pv,
    cast(sum(case when effect_click_pv > 0 then 1 else 0 end) as double) as effect_click_uv,
    coalesce(double_roundhalfup(
        sum(leaf_effect_page_pv), 
        sum(effect_pv), 4),
        cast(0.0 as double)) as effect_bounce_rate,
    sum(direct_item_pv) as direct_item_pv,
    sum(case when direct_item_pv > 0 then 1 else 0 end) as direct_item_uv,
    sum(case when direct_item_gmv_trade_num > 0 then 1 else 0 end) as direct_item_gmv_uv,
    sum(direct_item_gmv_amt) as direct_item_gmv_amt,
    sum(direct_item_gmv_auction_num) as direct_item_gmv_auction_num,
    sum(direct_item_gmv_trade_num) as direct_item_gmv_trade_num,
    sum(case when direct_item_alipay_trade_num > 0 then 1 else 0 end) as direct_item_alipay_uv,
    sum(direct_item_alipay_amt) as direct_item_alipay_amt,
    sum(direct_item_alipay_auction_num) as direct_item_alipay_auction_num,
    sum(direct_item_alipay_trade_num) as direct_item_alipay_trade_num,

    sum(direct_itemshop_pv) as direct_itemshop_pv,
    sum(case when direct_itemshop_pv > 0 then 1 else 0 end) as direct_itemshop_uv,
    sum(direct_itemshop_ipv) as direct_itemshop_ipv,
    sum(case when direct_itemshop_ipv > 0 then 1 else 0 end) as direct_itemshop_iuv,
    sum(case when direct_itemshop_gmv_trade_num > 0 then 1 else 0 end) as direct_itemshop_gmv_uv,
    sum(direct_itemshop_gmv_amt) as direct_itemshop_gmv_amt,
    sum(direct_itemshop_gmv_auction_num) as direct_itemshop_gmv_auction_num,
    sum(direct_itemshop_gmv_trade_num) as direct_itemshop_gmv_trade_num,
    sum(case when direct_itemshop_alipay_trade_num > 0 then 1 else 0 end) as direct_itemshop_alipay_uv,
    sum(direct_itemshop_alipay_amt) as direct_itemshop_alipay_amt,
    sum(direct_itemshop_alipay_auction_num) as direct_itemshop_alipay_auction_num,
    sum(direct_itemshop_alipay_trade_num) as direct_itemshop_alipay_trade_num,

    sum(direct_shop_pv) as direct_shop_pv,
    sum(case when direct_shop_pv > 0 then 1 else 0 end) as direct_shop_uv,
    sum(direct_shop_ipv) as direct_shop_ipv,
    sum(case when direct_shop_ipv > 0 then 1 else 0 end) as direct_shop_iuv,
    sum(case when direct_shop_gmv_trade_num > 0 then 1 else 0 end) as direct_shop_gmv_uv,
    sum(direct_shop_gmv_amt) as direct_shop_gmv_amt,
    sum(direct_shop_gmv_auction_num) as direct_shop_gmv_auction_num,
    sum(direct_shop_gmv_trade_num) as direct_shop_gmv_trade_num,
    sum(case when direct_shop_alipay_trade_num > 0 then 1 else 0 end) as direct_shop_alipay_uv,
    sum(direct_shop_alipay_amt) as direct_shop_alipay_amt,
    sum(direct_shop_alipay_auction_num) as direct_shop_alipay_auction_num,
    sum(direct_shop_alipay_trade_num) as direct_shop_alipay_trade_num,

    sum(other_item_pv) as other_item_pv,
    sum(case when other_item_pv > 0 then 1 else 0 end) as other_item_uv,
    sum(case when other_item_gmv_trade_num > 0 then 1 else 0 end) as other_item_gmv_uv,
    sum(other_item_gmv_amt) as other_item_gmv_amt,
    sum(other_item_gmv_auction_num) as other_item_gmv_auction_num,
    sum(other_item_gmv_trade_num) as other_item_gmv_trade_num,
    sum(case when other_item_alipay_trade_num > 0 then 1 else 0 end) as other_item_alipay_uv,
    sum(other_item_alipay_amt) as other_item_alipay_amt,
    sum(other_item_alipay_auction_num) as other_item_alipay_auction_num,
    sum(other_item_alipay_trade_num) as other_item_alipay_trade_num,

    sum(other_itemshop_pv) as other_itemshop_pv,
    sum(case when other_itemshop_pv > 0 then 1 else 0 end) as other_itemshop_uv,
    sum(other_itemshop_ipv) as other_itemshop_ipv,
    sum(case when other_itemshop_ipv > 0 then 1 else 0 end) as other_itemshop_iuv,
    sum(case when other_itemshop_gmv_trade_num > 0 then 1 else 0 end) as other_itemshop_gmv_uv,
    sum(other_itemshop_gmv_amt) as other_itemshop_gmv_amt,
    sum(other_itemshop_gmv_auction_num) as other_itemshop_gmv_auction_num,
    sum(other_itemshop_gmv_trade_num) as other_itemshop_gmv_trade_num,
    sum(case when other_itemshop_alipay_trade_num > 0 then 1 else 0 end) as other_itemshop_alipay_uv,
    sum(other_itemshop_alipay_amt) as other_itemshop_alipay_amt,
    sum(other_itemshop_alipay_auction_num) as other_itemshop_alipay_auction_num,
    sum(other_itemshop_alipay_trade_num) as other_itemshop_alipay_trade_num,

    sum(other_shop_pv) as other_shop_pv,
    sum(case when other_shop_pv > 0 then 1 else 0 end) as other_shop_uv,
    sum(other_shop_ipv) as other_shop_ipv,
    sum(case when other_shop_ipv > 0 then 1 else 0 end) as other_shop_iuv,
    sum(case when other_shop_gmv_trade_num > 0 then 1 else 0 end) as other_shop_gmv_uv,
    sum(other_shop_gmv_amt) as other_shop_gmv_amt,
    sum(other_shop_gmv_auction_num) as other_shop_gmv_auction_num,
    sum(other_shop_gmv_trade_num) as other_shop_gmv_trade_num,
    sum(case when other_shop_alipay_trade_num > 0 then 1 else 0 end) as other_shop_alipay_uv,
    sum(other_shop_alipay_amt) as other_shop_alipay_amt,
    sum(other_shop_alipay_auction_num) as other_shop_alipay_auction_num,
    sum(other_shop_alipay_trade_num) as other_shop_alipay_trade_num,
    
    sum(case when outside_gmv_trade_num > 0 then 1 else 0 end) as outside_gmv_uv,
    sum(outside_gmv_amt) as outside_gmv_amt,
    sum(outside_gmv_trade_num) as outside_gmv_trade_num,
    sum(case when outside_alipay_trade_num > 0 then 1 else 0 end) as outside_alipay_uv,
    sum(outside_alipay_amt) as outside_alipay_amt,
    sum(outside_alipay_trade_num) as outside_alipay_trade_num,
    sum(pv) as pv,
    sum(case when pv > 0 then 1 else 0 end) as uv,
    coalesce(double_roundhalfup(sum(pv), sum(case when pv > 0 then 1 else 0 end), 2),
               cast(0.0 as double)) as avg_pv,
    src
from 
(
    select
        plan_id,
        analyzer_id,
        day,
        dim,
        visit_id,
        src,
        sum(case when is_leaf =1 and is_effect_page = 1 then pv end) as leaf_effect_page_pv,
        sum(case when is_effect_page = 1 then pv end) as effect_pv,
        sum(case when ref_is_effect_page = 1 then pv end) as effect_click_pv,
        cast(sum(pv) as double) as pv,
        sum(case when jump_num < 2 and index_type = 1 then pv end) as direct_item_pv,
        sum(case when jump_num < 2 and index_type = 1 then gmv_amt end) as direct_item_gmv_amt,
        sum(case when jump_num < 2 and index_type = 1 then gmv_auction_num end) as direct_item_gmv_auction_num,
        sum(case when jump_num < 2 and index_type = 1 then gmv_trade_num end) as direct_item_gmv_trade_num,
        sum(case when jump_num < 2 and index_type = 1 then alipay_amt end) as direct_item_alipay_amt,
        sum(case when jump_num < 2 and index_type = 1 then alipay_auction_num end) as direct_item_alipay_auction_num,
        sum(case when jump_num < 2 and index_type = 1 then alipay_trade_num end) as direct_item_alipay_trade_num,

        sum(case when jump_num < 2 and index_type = 2 then pv end) as direct_itemshop_pv,
        sum(case when jump_num < 2 and index_type = 2 and length(auction_id) > 0 then pv end) as direct_itemshop_ipv,
        sum(case when jump_num < 2 and index_type = 2 then gmv_amt end) as direct_itemshop_gmv_amt,
        sum(case when jump_num < 2 and index_type = 2 then gmv_auction_num end) as direct_itemshop_gmv_auction_num,
        sum(case when jump_num < 2 and index_type = 2 then gmv_trade_num end) as direct_itemshop_gmv_trade_num,
        sum(case when jump_num < 2 and index_type = 2 then alipay_amt end) as direct_itemshop_alipay_amt,
        sum(case when jump_num < 2 and index_type = 2 then alipay_auction_num end) as direct_itemshop_alipay_auction_num,
        sum(case when jump_num < 2 and index_type = 2 then alipay_trade_num end) as direct_itemshop_alipay_trade_num,

        sum(case when jump_num < 2 and index_type = 3 then pv end) as direct_shop_pv,
        sum(case when jump_num < 2 and index_type = 3 and length(auction_id) > 0 then pv end) as direct_shop_ipv,
        sum(case when jump_num < 2 and index_type = 3 then gmv_amt end) as direct_shop_gmv_amt,
        sum(case when jump_num < 2 and index_type = 3 then gmv_auction_num end) as direct_shop_gmv_auction_num,
        sum(case when jump_num < 2 and index_type = 3 then gmv_trade_num end) as direct_shop_gmv_trade_num,
        sum(case when jump_num < 2 and index_type = 3 then alipay_amt end) as direct_shop_alipay_amt,
        sum(case when jump_num < 2 and index_type = 3 then alipay_auction_num end) as direct_shop_alipay_auction_num,
        sum(case when jump_num < 2 and index_type = 3 then alipay_trade_num end) as direct_shop_alipay_trade_num,

        sum(case when jump_num >= 2 and index_type = 1 then pv end) as other_item_pv,
        sum(case when jump_num >= 2 and index_type = 1 then gmv_amt end) as other_item_gmv_amt,
        sum(case when jump_num >= 2 and index_type = 1 then gmv_auction_num end) as other_item_gmv_auction_num,
        sum(case when jump_num >= 2 and index_type = 1 then gmv_trade_num end) as other_item_gmv_trade_num,
        sum(case when jump_num >= 2 and index_type = 1 then alipay_amt end) as other_item_alipay_amt,
        sum(case when jump_num >= 2 and index_type = 1 then alipay_auction_num end) as other_item_alipay_auction_num,
        sum(case when jump_num >= 2 and index_type = 1 then alipay_trade_num end) as other_item_alipay_trade_num,

        sum(case when jump_num >= 2 and index_type = 2 then pv end) as other_itemshop_pv,
        sum(case when jump_num >= 2 and index_type = 2 and length(auction_id) > 0 then pv end) as other_itemshop_ipv,
        sum(case when jump_num >= 2 and index_type = 2 then gmv_amt end) as other_itemshop_gmv_amt,
        sum(case when jump_num >= 2 and index_type = 2 then gmv_auction_num end) as other_itemshop_gmv_auction_num,
        sum(case when jump_num >= 2 and index_type = 2 then gmv_trade_num end) as other_itemshop_gmv_trade_num,
        sum(case when jump_num >= 2 and index_type = 2 then alipay_amt end) as other_itemshop_alipay_amt,
        sum(case when jump_num >= 2 and index_type = 2 then alipay_auction_num end) as other_itemshop_alipay_auction_num,
        sum(case when jump_num >= 2 and index_type = 2 then alipay_trade_num end) as other_itemshop_alipay_trade_num,

        sum(case when jump_num >= 2 and index_type = 3 then pv end) as other_shop_pv,
        sum(case when jump_num >= 2 and index_type = 3 and length(auction_id) > 0 then pv end) as other_shop_ipv,
        sum(case when jump_num >= 2 and index_type = 3 then gmv_amt end) as other_shop_gmv_amt,
        sum(case when jump_num >= 2 and index_type = 3 then gmv_auction_num end) as other_shop_gmv_auction_num,
        sum(case when jump_num >= 2 and index_type = 3 then gmv_trade_num end) as other_shop_gmv_trade_num,
        sum(case when jump_num >= 2 and index_type = 3 then alipay_amt end) as other_shop_alipay_amt,
        sum(case when jump_num >= 2 and index_type = 3 then alipay_auction_num end) as other_shop_alipay_auction_num,
        sum(case when jump_num >= 2 and index_type = 3 then alipay_trade_num end) as other_shop_alipay_trade_num,

        sum(case when index_type = 4 then gmv_amt end) as outside_gmv_amt,
        sum(case when index_type = 4 then gmv_trade_num end) as outside_gmv_trade_num,
        sum(case when index_type = 4 then alipay_amt end) as outside_alipay_amt,
        sum(case when index_type = 4 then alipay_trade_num end) as outside_alipay_trade_num
    from lz_fact_ep_ownership_ext 
    where dt=${ydate}
    group by
        plan_id,
        analyzer_id,
        day,
        dim,
        visit_id,
        src
) a
group by
    plan_id,
    analyzer_id,
    day,
    dim,
    src
;
EOF
if [ $? -ne 0 ];then
exit 2
fi


