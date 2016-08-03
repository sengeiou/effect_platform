#!/usr/bin/env python
# -*- encoding:utf-8 -*-

# 全局变量
ssh_cmd = 'scp'
hadoop_cmd = '/home/yunti/hadoop-current/bin/hadoop'

admin_host = 'dev039'
remote_path = '/home/lz/effect_platform/conf/report/%(YYYY)s/%(MM)s/%(DD)s'
remote_del_path = '/home/lz/effect_platform/conf/overdue/%(YYYY)s/%(MM)s/%(DD)s'
local_temp_path = '/home/linezing/effect_platform/temp_conf'
local_path = '/home/linezing/effect_platform/conf/%(PERIOD)s/%(TREE_SPLIT)s'
hadoop_path = '/group/tbads/sds/linezing/effect_platform/conf/%(PERIOD)s/%(TREE_SPLIT)s'

hadoop_root_path='/group/tbads/sds/linezing/effect_platform/conf/'
local_root_path='/home/linezing/effect_platform/conf'
