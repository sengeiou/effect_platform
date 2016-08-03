insert overwrite table lz_dim_ep_hjlj_info partition (dt='${date}', logsrc='tmall_brandsite')
select
    '${date}',
    code,
    '' as biz_name,
    '' as page_name,
    concat_ws("\002", "action", "type") as gokeys
from
    dim_tmall_brandsite_hjlj 
where
    dt='${date}'
    and status=1
group by
    code,
    concat_ws("\002", "action", "type")
;

