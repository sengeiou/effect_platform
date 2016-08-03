#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
if [ $# != 0 ];then
    ydate=$1
fi
Dir=`dirname $0`
source $HOME/linezing/effect_platform/config/set_env.conf

yydate=`date -d "$ydate -1days" +%Y%m%d`

$Hive <<EOF
insert overwrite table lz_fact_ep_ownership_ext partition (dt=${ydate})
select
    analyzer_id,
    plan_id,
    src,
    dim,
    ${ydate} as day,
    case when split(src, '\002')[0] < 10000 then cast(split(src, '\002')[0] as bigint)
         when split(src, '\002')[0] >=10000 and split(src, '\002')[0] < 20000 then cast(split(src, '\002')[0] - 10000 as bigint)
         when split(src, '\002')[0] >=20000 and split(src, '\002')[0] < 30000 then cast(split(src, '\002')[0] - 20000 as bigint)
         end as path_id,
    case when split(src, '\002')[0] < 10000 then cast(coalesce(split(split(src, '\002')[1], '\003')[0], '0') as bigint)
         when split(src, '\002')[0] >=10000 and split(src, '\002')[0] < 20000 then cast(100 as bigint) 
         when split(src, '\002')[0] >=20000 and split(src, '\002')[0] < 30000 then cast(103 as bigint)
         end as src_id,
    auction_id,
    user_id,
    visit_id,
    is_effect_page,
    ref_is_effect_page,
    is_leaf,
    jump_num,
    index_type,
    sum(pv),
    sum(gmv_amt),
    sum(gmv_auction_num),
    sum(gmv_trade_num),
    sum(alipay_amt),
    sum(alipay_auction_num),
    sum(alipay_trade_num),
    sum(item_collect_num),
    sum(shop_collect_num),
    sum(cart_auction_num)
from (
    select
        plan_id,
        analyzer_id,
        src,
        auction_id,
        user_id,
        ali_corp as dim,
        visit_id,
        is_effect_page,
        ref_is_effect_page,
        is_leaf,
        jump_num,
        index_type,
        pv,
        gmv_amt,
        gmv_auction_num,
        gmv_trade_num,
        alipay_amt,
        alipay_auction_num,
        alipay_trade_num,
        item_collect_num,
        shop_collect_num,
        cart_auction_num
    from lz_fact_ep_ownership
    where dt='${ydate}' and ali_corp<>0
    
    union all

    select
        plan_id,
        analyzer_id,
        src,
        auction_id,
        user_id,
        cast(0 as bigint) as dim,
        visit_id,
        is_effect_page,
        ref_is_effect_page,
        is_leaf,
        jump_num,
        index_type,
        pv,
        gmv_amt,
        gmv_auction_num,
        gmv_trade_num,
        alipay_amt,
        alipay_auction_num,
        alipay_trade_num,
        item_collect_num,
        shop_collect_num,
        cart_auction_num
    from lz_fact_ep_ownership
    where dt='${ydate}'
) temp
group by
    analyzer_id,
    plan_id,
    src,
    dim,
    case when split(src, '\002')[0] < 10000 then cast(split(src, '\002')[0] as bigint)
         when split(src, '\002')[0] >=10000 and split(src, '\002')[0] < 20000 then cast(split(src, '\002')[0] - 10000 as bigint)
         when split(src, '\002')[0] >=20000 and split(src, '\002')[0] < 30000 then cast(split(src, '\002')[0] - 20000 as bigint)
         end,
    case when split(src, '\002')[0] < 10000 then cast(coalesce(split(split(src, '\002')[1], '\003')[0], '0') as bigint)
         when split(src, '\002')[0] >=10000 and split(src, '\002')[0] < 20000 then cast(100 as bigint) 
         when split(src, '\002')[0] >=20000 and split(src, '\002')[0] < 30000 then cast(103 as bigint)
         end,
    auction_id,
    user_id,
    visit_id,
    is_effect_page,
    ref_is_effect_page,
    is_leaf,
    jump_num,
    index_type
;
EOF
if [ $? -ne 0 ];then
exit 2
fi
