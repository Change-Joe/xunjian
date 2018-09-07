#!/bin/python
# -*- coding:utf-8 -*-
import datetime
import json
import ConfigParser
import sys
import os
import linecache
import pymysql
import requests
from utils import string_datetime, datetime_string, timestamp_datetime, \
    father_path, system_version, execute_command

SALT_ERROR = 'Minion did not return. [Not connected]'
TRUE = 'True'
FALSE = 'False'
REBOOT = 'Reboot'

deepflow_conf_path = '/usr/local/deepflow/conf/deepflow.conf'
date_file_path = father_path + '/datetime_record.txt'
data_file_path = father_path + '/data_disk_used.txt'
interval_list = ['__1m_*', '__1h_*', '__1d_*']

cmd = "mt saltstack.list | grep controller | wc -l"
controller_number = int(execute_command(cmd))
cmd = "mt saltstack.list | grep analyzer | wc -l"
analyzer_number = int(execute_command(cmd))
cmd = "mt saltstack.list | grep elasticsearch | awk {'print $1'}"
ret = execute_command(cmd)
es_name_list = ret.split('\n')
cmd = 'cat /usr/local/deepflow/saltstack/nodegroup | grep elasticsearch'
# example: elasticsearch-hot:  example: elasticsearch-warm: []
ret_elasticsearch = execute_command(cmd)
es_role_list = ret_elasticsearch.split('\n')

controller_process_list = ['alarm', 'cerebro', 'exchange', 'hades', 'kibana',
                           'lcwebapi', 'nginx', 'painter', 'postman',
                           'sdncontroller', 'talker', 'webmsgcenter',
                           'zookeeper']
if system_version == 'DeepFlow-5.3.0':
    controller_process_list.append('openstack-agent')
    controller_process_list.append('vsphere-agent')
if system_version == 'DeepFlow-5.3.3':
    controller_process_list.append('cloud-agent')
    controller_process_list.append('siren')
analyzer_process_list = [
            'ovs-trident', 'openvswitch', 'ndpi', 'telegraf', 'influxdb-relay',
            'pyagexec']
jstorm_service_list = ['nimbus', 'supervisor']

process_log_path = ['/var/log/exchange.log', '/var/log/painter.log',
                    '/var/log/talker.log']
if system_version == 'DeepFlow-5.3.0':
    process_log_path.append('/var/log/openstack-agent.log')
    process_log_path.append('/var/log/vsphere-agent.log')
if system_version == 'DeepFlow-5.3.3':
    process_log_path.append('/var/log/cloud-agent.log')


def execute_salt_command(cmd):
    """执行salt命令,返回字典类型的结果"""
    ret_dict = {}
    ret = execute_command(cmd)
    ret_list = ret.split('\n')
    for index in range(len(ret_list) / 2):
        ret_dict[ret_list[2 * index]] = ret_list[2 * index + 1].strip()
    return ret_dict


def convert_data_used_unit(ret_list):
    """计算数据盘使用量,并统一使用G为单位"""
    data_disk_used = 0
    for index, value in enumerate(ret_list):
        if value.strip(':') not in es_name_list:
            if value.split()[-1][-1] == 'T':
                data_disk_used += float(value.split()[-1][:-1]) * 1024
            elif value.split()[-1][-1] == 'M':
                data_disk_used += float(value.split()[-1][:-1]) / 1024
            else:  # value.split()[-1][-1] == 'G'
                data_disk_used += float(value.split()[-1][:-1])
    return data_disk_used


def convert_data_size_unit(ret_list):
    """计算数据盘总量,并统一使用G为单位"""
    data_disk_size = 0
    for index, value in enumerate(ret_list):
        if value.strip(':') not in es_name_list:
            if value.split()[0][-1] == 'T':
                data_disk_size += float(value.split()[-1][:-1]) * 1024
            else:  # value.split()[0][-1] == 'G'
                data_disk_size += float(value.split()[-1][:-1])
    return data_disk_size


def print_content(check_point, check_content):
    """检查点及检查内容输出"""
    print '-------------------------------------------------------------------'
    print '\033[1;34m检 查 点：' + check_point + ' \033[0m'
    print '\033[1;34m检查内容：' + check_content + ' \033[0m'


def print_pass():
    """检查pass输出"""
    print '\033[1;34m检查结果：\033[0m'
    print '\033[1;32m\t  Pass\t\033[0m'


def print_fail():
    """检查fail输出"""
    print '\033[1;34m检查结果：\033[0m'
    print '\033[1;31m\t  Fail\t\033[0m'
    print '\033[1;34m异常内容：\033[0m'


def print_advice(advice):
    """异常处理建议输出"""
    print '\033[1;33m处理建议：\033[0m'
    print '\t\033[1;31m ' + advice + '\033[0m'


class Logger(object):
    def __init__(self, fileN="Default.log"):
        self.terminal = sys.stdout
        self.log = open(fileN, "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        pass


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_1 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def get_enable_app(file_path):
    """获取deepflow.conf中enable的app"""
    cmd = 'cat ' + file_path + ' | grep -i app'
    ret = execute_command(cmd)
    ret_list = ret.split('\n')
    app_list = []
    for app in ret_list:
        if app.split('=')[0].strip() != 'wechat.app_name':
            if app.split('=')[1].strip() == 'enable':
                app_list.append(app.split('=')[0].strip())
    return app_list


def get_app_endprocess(app):
    """查询app对应索引的时间"""
    cmd = 'curl -XGET -s localhost:20042/dfi_meta/_search?pretty ' \
          '-d \'{"size":50}\''
    ret = execute_command(cmd)
    endprocess_dict = json.loads(ret)
    for hit in endprocess_dict['hits']['hits']:
        if hit['_id'] == app:
            return hit['_source']['end_process']


def check_point_1():
    print_content('界面数据展示是否滞后',
                  '检查deepflow.conf配置文件中enable的app对应索引数据是否滞后')
    app_list = get_enable_app(deepflow_conf_path)
    result_dict = {}
    for app in app_list:
        for interval in interval_list:
            index = 'dfi_' + app[:-4] + interval
            endprocess = get_app_endprocess(index)
            if endprocess is None:
                continue
            end_dt = timestamp_datetime(endprocess)
            if interval == '__1m_*':
                # 计算滞后的秒数
                diff = int((this_xunjian - end_dt).total_seconds())
                if diff > 0:
                    if diff > int(index_1m_limit) * 60:
                        result_dict[index + ':' + str(diff / 60) + 'm'] = FALSE
                    else:
                        result_dict[index] = TRUE
                else:
                    result_dict[index] = TRUE
            elif interval == '__1h_*':
                diff = int((this_xunjian - end_dt).total_seconds()) / 60 / 60
                if diff > int(index_1h_limit):
                    result_dict[index + ':' + str(diff) + 'h'] = FALSE
                else:
                    result_dict[index] = TRUE
            elif interval == '__1d_*':
                diff = (this_xunjian - end_dt).days
                if diff > int(index_1d_limit):
                    result_dict[index + ':' + str(diff) + 'd'] = FALSE
                else:
                    result_dict[index] = TRUE

    if FALSE in result_dict.values():
        print_fail()
        for key, value in result_dict.items():
            if value == FALSE:
                print '\033[1;31m\t  ' + key.split(':')[0] + '数据滞后' + \
                      key.split(':')[1] + '\t\033[0m'
        print_advice(' 检查平台服务性能')
    else:
        print_pass()
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_1 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_2 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def check_point_2():
    print_content('系统运行状态', '检查各节点系统在巡检周期内是否发生过重启')
    cmd = 'salt "*" cmd.run "last | grep boot | head -1 | ' \
          'awk \'{print \$6,\$7,\$8,\$11}\'"'
    # example: Sep 3 10:47 (3+00:52)
    ret_dict = execute_salt_command(cmd)
    result_dict = {}
    for key, value in ret_dict.items():
        interval_list = value.split('(')[1].strip(')').split('+')
        last_reboot_seconds = 0
        if len(interval_list) == 2:
            last_reboot_seconds = 24 * 60 * 60 * int(interval_list[0]) + \
                                  60 * 60 * int(interval_list[1][:2]) + \
                                  60 * int(interval_list[1][-2:])
        elif len(interval_list) == 1:
            last_reboot_seconds = 60 * 60 * int(interval_list[0][:2]) + \
                                  60 * int(interval_list[0][-2:])
        if last_reboot_seconds > total_xunjian_seconds:
            result_dict[key] = TRUE
        else:
            result_dict[key + ' ' + value.split('(')[0].strip()] = FALSE

    if FALSE in result_dict.values():
        print_fail()
        for key, value in result_dict.items():
            if value == FALSE:
                print '\033[1;31m\t  ' + key.split(': ')[0] + '在巡检周期内' \
                    '发生过重启,重启时间为' + key.split(': ')[1] + '\t\033[0m'
        print_advice(' 检查并恢复平台状态,排查重启原因')
    else:
        print_pass()
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_2 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_3 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def check_point_3():
    print_content('系统盘使用率', '检查各节点系统盘使用率是否超过阈值')
    cmd = 'salt "*" cmd.run "df -h --output=target,pcent | ' \
          'grep -vE [s,v]d[b-z] | grep -w \'/\'"'
    # example:     /                4%
    ret_dict = execute_salt_command(cmd)
    result_dict = {}
    for key, value in ret_dict.items():
        sys_disk_threshold_value = int(sys_disk_threshold.strip('%'))
        if int(value.split()[-1].strip('%')) >= sys_disk_threshold_value:
            result_dict[key] = FALSE
        else:
            result_dict[key] = TRUE

    if FALSE in result_dict.values():
        print_fail()
        for key, value in result_dict.items():
            if value == FALSE:
                print '\033[1;31m\t  ' + key + '系统盘使用率超过阈值\t\033[0m'
        print_advice(' 进一步分析原因,并及时清理不必要的数据')
    else:
        print_pass()
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_3 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_4 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def cal_xunjian_date():
    """计算巡检周期中包含的date,返回格式:Sep 11"""
    xunjian_date_list = []
    for i in range(xunjian_days + 1):
        delta = datetime.timedelta(i)
        ago_date = this_xunjian - delta  # 计算当前日期前j天的日期
        ago_date_datetime = ago_date.strftime('%b %d')
        # 将Sep 03更改为Sep  3格式
        if ago_date_datetime.split(' ')[1][0] == '0':
            ago_date_datetime = ago_date_datetime.split(' ')[0] + '  ' + \
                                ago_date_datetime.split(' ')[1][1]
            xunjian_date_list.append(ago_date_datetime)
    return xunjian_date_list


def check_point_4():
    print_content('磁盘故障检查', '检查各节点磁盘是否存在故障')
    result_dict = {}
    xunjian_date_list = cal_xunjian_date()
    for date in xunjian_date_list:
        cmd = 'salt \'*\' cmd.run "cat /var/log/messages* | ' \
              'grep -i \'error\' | grep -i \'XFS\' | grep -i \'' + date + \
              '\' | grep -iv \'salt\' | wc -l"'
        ret_dict = execute_salt_command(cmd)
        for key, value in ret_dict.items():
            if value == '0':
                result_dict[key + date] = TRUE
            else:
                result_dict[key + date] = FALSE

    if FALSE in result_dict.values():
        print_fail()
        for key, value in result_dict.items():
            if value == FALSE:
                print '\033[1;31m\t  ' + key.split(':')[0] + \
                      '节点messages日志中存在XFS error,时间为' + \
                      key.split(':')[1] + '\t\033[0m'
        print_advice(' 磁盘可能存在硬件问题,检查messages日志定位具体原因')
    else:
        print_pass()
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_4 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_5 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def check_point_5():
    print_content('时间同步', '检查各节点时钟是否同步')
    cmd = "salt '*' cmd.run 'date'"
    ret_dict = execute_salt_command(cmd)
    date_list = [value for value in ret_dict.values()]
    if len(list(set(date_list))) == 1:  # 取时间列表中不同的值
        print_pass()
    else:
        print_fail()
        print '\033[1;31m\t  各节点时间未同步\t\033[0m'
        print_advice(' 检查是否控制器和分析器之间有安全策略限制,'
                     '禁止了chronyd服务端口\n\t  重启各节点的chronyd服务')
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_5 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_6 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def check_point_6(host_list):
    """检查控制器、分析器上的进程状态以及在巡检周期内是否发生重启"""
    print_content('控制器与分析器组件',
                  '检查控制器与分析器组件状态,并判断巡检周期内是否发生过重启')
    result_dict = {}
    for host in host_list:
        process_list = []
        if host == 'controller':
            process_list = controller_process_list
        if host == 'analyzer':
            process_list = analyzer_process_list
        for process in process_list:
            cmd = 'salt -N ' + host + ' cmd.run "systemctl status ' + \
                  process + '| grep \'Active\' | cut -d \' \' -f 5,9-10"'
            # example: active 2018-09-03 13:38:04
            ret_dict = execute_salt_command(cmd)
            for key, value in ret_dict.items():
                if value.split()[0] == 'inactive' \
                        or value.split()[0] == 'failed':
                    result_dict[key + process] = FALSE
                else:
                    run_time = value.split()[1] + ' ' + value.split()[2]
                    run_dtime = string_datetime(run_time)
                    interval = (run_dtime - last_xunjian).total_seconds()
                    if interval >= 0:
                        result_dict[key + process + '  ' + run_time] = REBOOT
                    else:
                        result_dict[key + process] = TRUE

    if FALSE in result_dict.values() or REBOOT in result_dict.values():
        print_fail()
        for key, value in result_dict.items():
            if value == FALSE:
                print '\033[1;31m\t  ' + key + '进程状态异常\t\033[0m'
            if value == REBOOT:
                print '\033[1;31m\t  ' + key.split('  ')[0] + \
                      '进程在巡检周期内发生过重启,重启时间为' + \
                      key.split('  ')[1] + '\t\033[0m'
        print_advice(' 检查日志排查具体原因')
    else:
        print_pass()
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_6 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_7 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def check_point_7():
    print_content('分析器组件配置', '检查dfi.check是否有error输出')
    cmd = 'mt dfi.check | wc -l'
    ret = execute_command(cmd)
    if ret == str(analyzer_number):
        print_pass()
    else:
        print_fail()
        print '\033[1;31m\t  mt dfi.check检查有error输出\t\033[0m'
        print_advice(' 请研发协助排查 error 原因后再执行恢复操作\n\t  恢复操作:'
                     'salt $analyzer_hostname state.apply openvswitch,'
                     'ovs-trident,ndpi(注意apply的组件顺序必须保持一致)')
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_7 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_8 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def check_point_8():
    print_content('mysql连接数状态', '检查mysql服务连接数是否超过阈值')
    cmd = "salt -N controller cmd.run 'netstat -anlp | grep -i mysql | wc -l'"
    ret_dict = execute_salt_command(cmd)
    for key, value in ret_dict.items():
        if int(value) > int(mysql_connect_threshold):
            print_fail()
            print '\033[1;31m\t  mysql服务连接数超过阈值\t\033[0m'
            print_advice(' 可尝试修改DeepFlow控制器MySQL服务连接相关设置:'
                         '连接超时wait_timeout=60,最大连接数max_connections=1000')
        else:
            print_pass()
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_8 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_9 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def connect_mysql(sql):
    """连接mysql数据库,并执行查询语句,返回查询结果"""
    conn = None
    cursor = None
    try:
        conn = pymysql.connect(
                host='127.0.0.1', port=20130, user='guest', passwd='guest',
                db='deepflow')
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows
    except Exception as e:
        print '\033[1;31m\t  数据库操作发生异常\t\033[0m'
        print e
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def check_point_9():
    print_content('trident状态', '检查trident运行状态')
    sql = 'select name,state from vtap'
    results = connect_mysql(sql)
    if results:
        result_dict = {}
        for result in results:
            if result[1] == 1:
                result_dict[result[0]] = TRUE
            else:
                result_dict[result[0]] = FALSE

        if FALSE in result_dict.values():
            print_fail()
            for key, value in result_dict.items():
                if value == FALSE:
                    print '\033[1;31m\t  ' + key + ':trident状态异常\t\033[0m'
            print_advice(' 检查对应节点trident日志,排查具体原因后恢复')
        else:
            print_pass()
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_9 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_10 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def check_point_10():
    print_content('计算服务集群控制组件与计算组件数目',
                  '检查JStrom-Nimbus、JStrom-Supervisor是否与控制器、分析器数目一致')
    result_dict = {}
    for service in jstorm_service_list:
        cmd = 'salt \'*\' cmd.run "systemctl status jstorm-' + service + \
              ' | grep \'Active\' | cut -d \' \' -f 5"'
        # example: active
        ret_dict = execute_salt_command(cmd)
        count = 0
        for key, value in ret_dict.items():
            if value == 'active':
                count += 1
        if service == 'nimbus':
            if count == controller_number:
                result_dict['JStorm-nimbus-number'] = TRUE
            else:
                result_dict['JStorm-nimbus-number'] = FALSE
        if service == 'supervisor':
            if count == analyzer_number:
                result_dict['JStorm-supervisor-number'] = TRUE
            else:
                result_dict['JStorm-supervisor-number'] = FALSE

    if FALSE in result_dict.values():
        print_fail()
        for key, value in result_dict.items():
            if value == FALSE:
                if key == 'JStorm-nimbus-number':
                    print '\033[1;31m\t  ' + key + '与控制器数目不一致\t\033[0m'
                if key == 'JStorm-supervisor-number':
                    print '\033[1;31m\t  ' + key + '与分析器数目不一致\t\033[0m'
        print_advice(' 检查nimbus/supervisor服务运行状态')
    else:
        print_pass()
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_10 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_11 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def check_point_11():
    print_content('计算服务集群控制组件与计算组件状态',
                  '检查JStrom-Nimbus、JStrom-Supervisor在巡检周期内是否发生重启')
    result_dict = {}
    for service in jstorm_service_list:
        cmd = ''
        if service == 'nimbus':
            cmd = 'salt -N controller cmd.run "systemctl status ' \
                  'jstorm-nimbus | grep \'Active\' | cut -d \' \' -f 9-10"'
            # example: 2018-09-03 14:42:57
        if service == 'supervisor':
            cmd = 'salt -N analyzer cmd.run "systemctl status ' \
                  'jstorm-supervisor | grep \'Active\' | cut -d \' \' -f 9-10"'
        ret_dict = execute_salt_command(cmd)
        for key, value in ret_dict.items():
            run_time = value.split()[0] + ' ' + value.split()[-1]
            run_dtime = string_datetime(run_time)
            interval = (run_dtime - last_xunjian).total_seconds()
            if interval >= 0:
                result_dict[key + service + '  ' + value] = FALSE
            else:
                result_dict[key + service] = TRUE

    if FALSE in result_dict.values():
        print_fail()
        for key, value in result_dict.items():
            if value == FALSE:
                print '\033[1;31m\t  ' + key.split('  ')[0] + \
                      '在巡检周期内发生重启,重启时间为' + key.split('  ')[1] + \
                      '\t\033[0m'
        print_advice(' 检查日志排查重启原因')
    else:
        print_pass()
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_11 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_12 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def check_point_12():
    print_content('计算服务集群数据处理组件', '检查poseidon状态是否有error')
    cmd = '/var/lib/jstorm/bin/jstorm list | tail -n +7'
    ret = execute_command(cmd)
    ret_dict = json.loads(ret)

    if ret_dict['topologies']:
        poseidon_status = ret_dict['topologies'][0]['status']
        poseidon_errorinfo = ret_dict['topologies'][0]['errorInfo']
        if poseidon_status == 'ACTIVE' and poseidon_errorinfo == '':
            print_pass()
        else:
            print_fail()
            if poseidon_status != 'ACTIVE':
                print '\033[1;31m\t  poseidon状态异常\t\033[0m'
            if poseidon_errorinfo != '':
                print '\033[1;31m\t  poseidon存在error信息\t\033[0m'
            print_advice(' 检查poseidon日志,需要重启恢复(注意:'
                         '重启poseidon会丢失数据，需要跟售前和客户确认)')
    else:
        print_fail()
        print '\033[1;31m\t  poseidon状态异常\t\033[0m'
        print_advice(' 检查poseidon日志,需要重启恢复(注意:'
                     '重启poseidon会丢失数据，需要跟售前和客户确认)')
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_12 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_13 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def check_zk_mode():
    """检查zk集群的mode信息"""
    result_dict = {}
    cmd = 'salt -N master cmd.run "zkServer status 2>&1 | grep Mode"'
    # example: Mode: leader
    ret_dict = execute_salt_command(cmd)
    for key, value in ret_dict.items():
        if value == 'Mode: leader' or value == 'Mode: follower':
            result_dict[key] = TRUE
        else:
            result_dict[key] = FALSE
    return result_dict


def check_point_13():
    print_content('计算服务集群配置同步组件',
                  '检查各节点zookeeper状态及对应的mode信息')
    cmd = 'salt -N master service.status zookeeper'
    ret_dict = execute_salt_command(cmd)

    if FALSE in ret_dict.values():
        print_fail()
        for key, value in ret_dict.items():
            if value == 'False':
                print '\033[1;31m\t  ' + key + '服务异常\t\033[0m'
        print_advice(' 检查各master节点的zookeeper服务')
    else:
        result_dict = check_zk_mode()
        if FALSE in result_dict.values():
            print_fail()
            for key, value in result_dict.items():
                if value == FALSE:
                    print '\033[1;31m\t  ' + key + 'mode信息有误\t\033[0m'
            print_advice(' 重启各master节点的zookeeper服务')
        else:
            print_pass()
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_13 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_14 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def check_point_14():
    print_content('存储集群整体状态',
                  '检查存储集群服务及状态是否正常,巡检周期内是否发生重启')
    cmd = 'salt \'*\' cmd.run "systemctl status elasticsearch | ' \
          'grep \'Active\' | cut -d \' \' -f 5,9-10"'
    # example: active 2018-09-03 10:47:52
    ret_dict = execute_salt_command(cmd)
    result_dict = {}
    for key, value in ret_dict.items():
        run_time = value.split()[1] + ' ' + value.split()[2]
        run_dtime = string_datetime(run_time)
        interval = (run_dtime - last_xunjian).total_seconds()
        if value.split()[0] == 'inactive':
            result_dict[key] = FALSE
        elif interval >= 0:
            result_dict[key + '  ' + run_time] = REBOOT
        else:
            result_dict[key] = TRUE

    if FALSE not in result_dict.values():
        request = requests.get("http://localhost:20042/_cluster/health?pretty")
        request_dict = request.json()
        result_dict['cluster_status'] = str(request_dict['status'].decode())
    if FALSE in result_dict.values() or REBOOT in result_dict.values() \
            or 'yellow' in result_dict.values() \
            or 'red' in result_dict.values():
        print_fail()
        for key, value in result_dict.items():
            if value == FALSE:
                print '\033[1;31m\t  ' + key.strip(':') + '存储服务异常\t\033[0m'
            if value == REBOOT:
                print '\033[1;31m\t  ' + key.split(':  ')[0] + \
                      '节点存储服务在巡检周期内发生重启,重启时间为' + \
                      key.split(':  ')[1] + '\t\033[0m'
            if value == 'yellow' or value == 'red':
                print '\033[1;31m\t  集群状态：' + value + '\t\033[0m'
        print_advice(' 存储服务异常检查')
    else:
        print_pass()
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_14 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_15 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def get_host_name_in_list(index, ret_list):
    """检查点15中用于获取节点名称"""
    host_name = ''
    for i in range(0, index):
        if ret_list[i].strip(':') in es_name_list:
            host_name = ret_list[i]
    return host_name


def get_data_used(host):
    """获取elasticsearch-hot/warm节点的数据盘使用量"""
    cmd = 'salt -N ' + host.strip().strip(':') + ' cmd.run "df -h ' \
        '--output=target,used | grep -E [s,v]d[b-z]"'
    ret = execute_command(cmd)
    ret_list = ret.split('\n')
    data_disk_used = convert_data_used_unit(ret_list)
    if os.path.exists(data_file_path):
        with open(data_file_path, 'r') as file:
            for line, value in enumerate(file):
                if value.strip('\n') == host.strip():
                    data_disk_used_last = linecache.getline(data_file_path,
                                                            line + 2)
                    cmd = "sed -i '" + str(line + 2) + "s/" + \
                          data_disk_used_last.strip('\n') + "/" + \
                          str(data_disk_used) + "/' " + data_file_path
                    execute_command(cmd)
        cmd = 'cat ' + data_file_path + ' | grep ' + host.strip()
        ret = execute_command(cmd)
        if not ret:
            cmd = "echo '" + host.strip() + "\n" + str(data_disk_used) + \
                  "' >> " + data_file_path
            execute_command(cmd)
    else:  # 环境第一次执行脚本时,将数据盘使用量写入
        cmd = "echo '" + host.strip() + "\n" + str(data_disk_used) + \
              "' >> " + data_file_path
        execute_command(cmd)


def get_data_used_trend(host):
    """获取elasticsearch-hot/warm节点的数据盘使用率趋势"""
    cmd = 'salt -N ' + host.strip().strip(':') + ' cmd.run "df -h ' \
          '--output=target,size,used | grep -E [s,v]d[b-z]"'
    ret = execute_command(cmd)
    ret_list = ret.split('\n')
    data_disk_used = convert_data_used_unit(ret_list)
    data_disk_size = convert_data_size_unit(ret_list)

    if os.path.exists(data_file_path):
        with open(data_file_path, 'r') as file:
            for line, value in enumerate(file):
                if value.strip('\n') == host.strip():
                    data_disk_used_last = linecache.getline(data_file_path,
                                                            line + 2)
                    if xunjian_days > 0:   # 每天增幅
                        trend = (data_disk_used - float(data_disk_used_last)) \
                                / xunjian_days
                        if trend <= 0:
                            print '\033[1;31m\t  ' + host.strip().strip(':') \
                                + '节点磁盘使用率未呈上升趋势\t\033[0m'
                        else:
                            keep_day = (float(data_disk_size) *
                                        float(data_disk_threshold.strip('%'))
                                        / 100 - data_disk_used) / trend
                            print '\033[1;31m\t  预计' + str(keep_day) + \
                                  '天,' + host.strip().strip(':') + \
                                  '节点的数据盘使用率接近阈值\t\033[0m'


def check_point_15():
    print_content('数据盘使用率', '检查各节点数据盘使用率是否超过阈值')
    result_dict = {}
    for role in es_role_list:
        if len(role.split(': ')) == 1:  # 判断是否存在elasticsearch-warm节点
            cmd = 'salt -N ' + role.strip().strip(':') + ' cmd.run "df -h ' \
                    '--output=target,pcent | grep -E [s,v]d[b-z]"'
            # example: /mnt/sde1        5%
            ret = execute_command(cmd)
            ret_list = ret.split('\n')
            for index, value in enumerate(ret_list):
                if value.strip(':') not in es_name_list:
                    if int(value.strip().split()[1].strip('%')) > int(
                            data_disk_threshold.strip('%')):
                        host_name = get_host_name_in_list(index, ret_list)
                        result_dict[
                            host_name + value.strip().split()[0]] = FALSE
                    elif int(value.strip().split()[1].strip('%')) > \
                            int(data_disk_close_to_threshold.strip('%')):
                        host_name = get_host_name_in_list(index, ret_list)
                        result_dict[
                            host_name + value.strip().split()[0]] = 'warn'
                    else:
                        host_name = get_host_name_in_list(index, ret_list)
                        result_dict[host_name] = TRUE

    if FALSE in result_dict.values():
        print_fail()
        for key, value in result_dict.items():
            if value == FALSE:
                print '\033[1;31m\t  ' + key + '磁盘使用率超过阈值\t\033[0m'
        print_advice(' 删除历史索引,降低数据盘使用率\n\t  平台资源扩容')
    elif 'warn' in result_dict.values():
        print '\033[1;34m检查结果：\033[0m'
        print '\033[1;31m\t  Warn。\t\033[0m'
        for key, value in result_dict.items():
            if value == 'warn':
                print '\033[1;31m\t  ' + key + '磁盘使用率接近阈值\t\033[0m'
        print_advice(' 分析在下一次巡检时,磁盘使用率是否会超过75%,'
                     '如果会超过,需要做删除数据处理')
        # 对数据盘数据进行估算
        for role in es_role_list:
            if len(role.split(': ')) == 1:
                get_data_used_trend(role)
    else:
        print_pass()
    # 将data_used写入文件，为了下一次执行脚本时计算磁盘使用率趋势
    for role in es_role_list:
        if len(role.split(': ')) == 1:
            get_data_used(role)
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_15 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_16 start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def check_point_16():
    print_content('进程日志检查',
                  '检查巡检周期内各进程日志是否有ERROR级别的日志输出')
    result_dict = {}
    for process_log in process_log_path:
        for j in range(xunjian_days + 1):
            delta = datetime.timedelta(j)
            ago_date = this_xunjian - delta  # 计算当前日期前j天的日期
            ago_date_datetime = ago_date.strftime('%Y-%m-%d %H:%M:%S')
            cmd = "cat '" + process_log + "'* | grep -i 'error' | grep -i '"\
                  + ago_date_datetime[:10] + "'"
            ret = execute_command(cmd)
            if ret == "":
                result_dict[process_log + ' ' + ago_date_datetime[:10]] = TRUE
            else:
                result_dict[process_log + ' ' + ago_date_datetime[:10]] = FALSE

    if FALSE in result_dict.values():
        print_fail()
        for key, value in result_dict.items():
            if value == FALSE:
                print '\033[1;31m\t  ' + key.split(' ')[0] + \
                      '存在ERROR日志,时间为' + key.split(' ')[1] + '\t\033[0m'
        print_advice(' 检查日志ERROR的具体原因')
    else:
        print_pass()
# >>>>>>>>>>>>>>>>>>>>>>>>>>>check_point_16 end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# >>>>>>>>>>>>>>>>>>>>>>>>>disk optimization start<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def disk_optimization():
    """磁盘空间优化"""
    if delete_process_log_confirm.lower() == 'true':
        print '\n-------------------------------------------------------------'
        print '\033[1;34m磁盘空间优化：' \
              '删除自当前日期起的上上个月的进程日志 \033[0m'
        result_dict = {}
        for process_log in process_log_path:
            month = int(str(this_xunjian)[5:7])
            days = int(str(this_xunjian)[8:10])
            for day in range(1, days + 1):
                delete_log_date_old = str(this_xunjian)[:5] + str(
                    month - 2) + '-' + str(day)  # 2018-6-1转换成2018-06-01
                delete_log_date_new = str(datetime.datetime.strptime(
                    delete_log_date_old, "%Y-%m-%d"))[:10]
                cmd = 'rm -rf ' + process_log + '.' + delete_log_date_new
                ret = execute_command(cmd)
                if ret != "":
                    result_dict[process_log] = FALSE

        if FALSE in result_dict.values():
            for key, value in result_dict.items():
                if value == FALSE:
                    print '\033[1;31m\t  ' + key + '删除失败\t\033[0m'
        else:
            print '\033[1;32m\tPass\t\033[0m'
# >>>>>>>>>>>>>>>>>>>>>>>>>disk optimization end<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


sys.stdout = Logger(father_path + '/target_file.txt')
config = ConfigParser.ConfigParser()
with open(father_path + '/xunjian.cfg', 'r') as cfgfile:
    config.readfp(cfgfile)
    index_1m_limit = config.get('Info', 'index_1m_limit')
    index_1h_limit = config.get('Info', 'index_1h_limit')
    index_1d_limit = config.get('Info', 'index_1d_limit')
    sys_disk_threshold = config.get('Info', 'system_disk_threshold')
    data_disk_threshold = config.get('Info', 'data_disk_threshold')
    data_disk_close_to_threshold = config.get('Info',
                                              'data_disk_close_to_threshold')
    mysql_connect_threshold = config.get('Info', 'mysql_connect_threshold')
    delete_process_log_confirm = config.get('Info',
                                            'delete_process_log_confirm')

cmd = "salt '*' test.ping"
ret_dict = execute_salt_command(cmd)
if SALT_ERROR in ret_dict.values():
    for key, value in ret_dict.items():
        if value == SALT_ERROR:
            print '\033[1;31m\t  ' + key + '节点salt通信失败,需恢复\t\033[0m'
else:
    if os.path.exists(date_file_path):
        with open(date_file_path) as record_file:
            last_xunjian_str = record_file.readlines()[-1].strip('\n')
            last_xunjian = string_datetime(last_xunjian_str)
    else:
        last_xunjian_date = raw_input('请输入上次巡检日期(YYYY-MM-DD):')
        last_xunjian_time = raw_input('请输入上次巡检时间(HH:MM:SS):')
        last_xunjian = string_datetime(
            last_xunjian_date + ' ' + last_xunjian_time)
    this_xunjian = datetime.datetime.now()
    cmd = 'echo ' + datetime_string(this_xunjian) + ' >> ' + date_file_path
    execute_command(cmd)
    total_xunjian_seconds = int((this_xunjian - last_xunjian).total_seconds())
    xunjian_days = (this_xunjian - last_xunjian).days
    print '上次巡检时间：' + '\033[1;32m ' + datetime_string(last_xunjian) \
          + ' \033[0m'
    print '本次巡检时间：' + '\033[1;32m ' + datetime_string(this_xunjian) \
          + ' \033[0m'

    # check_point_1
    check_point_1()

    # check_point_2
    check_point_2()

    # check_point_3
    check_point_3()

    # check_point_4
    check_point_4()

    # check_point_5
    check_point_5()

    # check_point_6
    check_point_6(['controller', 'analyzer'])

    # check_point_7
    check_point_7()

    # check_point_8
    check_point_8()

    # check_point_9
    check_point_9()

    # check_point_10
    check_point_10()

    # check_point_11
    check_point_11()

    # check_point_12
    check_point_12()

    # check_point_13
    check_point_13()

    # check_point_14
    check_point_14()

    # check_point_15
    check_point_15()

    # check_point_16
    check_point_16()

    # 磁盘空间优化
    disk_optimization()



