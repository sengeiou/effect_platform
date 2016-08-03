insert overwrite table lz_fact_ep_cart_info partition(dt='${date}', logsrc='taobao')
select
    time_stamp,
    shop_id,
    url_id,
    '',
    acookie,
    case when url rlike '^(http|https)://([^/]*\\.)?taobao\\.com.*' then 1
        when url rlike '^(http|https)://([^/]*\\.)?tmall\\.com.*' then 2
        when url rlike '^(http|https)://([^/]*\\.)?etao\\.com.*' then 3
        when url rlike '^(http|https)://ju\\.taobao\\.com.*' then 4
        else 0 end as ali_corp,
    cast(1 as bigint) as cart_num,
    '',
    ''
from
    lz_fact_jslog_info
where
    dt='${date}'
    and all_clicktabs rlike '[\.#](J_LinkAdd|J_LinkBasket)\\b'
;
