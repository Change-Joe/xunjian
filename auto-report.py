#!/bin/python
# -*- coding:utf-8 -*-
import xlsxwriter
import linecache
from influxdb import InfluxDBClient
import datetime
import json
from utils import string_datetime, timestamp_datetime, datetime_unix, \
    father_path, system_version, execute_command

# 获取控制器、分析器name
cmd_host_name = "mt saltstack.list | grep -e controller -e analyzer " \
                "| cut -d ' ' -f 1"
ret_host_name = execute_command(cmd_host_name)
host_list = ret_host_name.split('\n')

interface_file_path = '/usr/local/deepflow/saltstack/pillar/network.sls'
target_file_path = father_path + '/target_file.txt'


def get_line_content(file_path, line_number):
    """获取文件指定行的内容"""
    return linecache.getline(file_path, line_number).strip()


def query_influxdb(sql):
    """执行查询语句"""
    try:
        client = InfluxDBClient('localhost', 20044, 'root', '', 'telegraf')
        result = client.query(sql)
        return result
    except Exception as e:
        print '\033[1;31m\t  数据库操作发生异常\t\033[0m'
        print e
        return None


def get_interface(file_path, host_list):
    """获取接口信息
    file_path:获取接口信息的file
    host_list:host名称列表
    """
    interface_dict = {}
    for host in host_list:
        cmd = 'cat ' + file_path + ' | grep -n ' + host
        ret = execute_command(cmd)
        host_row = ret.split(':')[0]
        if get_line_content(file_path, int(host_row) + 1).__contains__('control'):
            control_if = get_line_content(file_path, int(host_row) + 2)
            interface_dict[host + ':(ctrl-if)'] = control_if.split(': ')[1]
        if get_line_content(file_path, int(host_row) + 5).__contains__('data'):
            data_if = get_line_content(file_path, int(host_row) + 8)
            interface_dict[host + ':(data-if)'] = data_if.split(' ')[1]
        if get_line_content(file_path, int(host_row) + 9).__contains__('tap'):
            tap_if = get_line_content(file_path, int(host_row) + 10)
            interface_dict[host + ':(tap-if)'] = tap_if.split(' ')[1]
    return interface_dict


def repair_time_display(raw_time):
    """修复用influxdb的time画图，时区不一致导致相差8小时的问题,
    返回datetime格式"""
    time = raw_time.split('T')[0] + ' ' + raw_time.split('T')[1][:-1]
    dt = (string_datetime(time) + datetime.timedelta(hours=8)).strftime(
        "%Y-%m-%d %H:%M:%S")
    return dt


def setstyle(name, height, color, bold=False):
    """设置报表中单元格字体格式"""
    format = table.add_format()
    format.set_font_name(name)
    format.set_font_size(height)
    format.set_font_color(color)
    format.set_bold(bold)  # 字体加粗
    format.set_align('vcenter')  # 垂直居中
    format.set_border(1)  # 单元格边框
    format.set_text_wrap()  # 自动换行
    return format


def set_chart_style(chart_name, chart_ylabel):
    """设置图表的格式
    chart_name:趋势图name
    chart_ylabel:趋势图纵坐标标签名
    """
    chart = table.add_chart({'type': 'line'})
    chart.height = 720
    chart.width = 960
    chart.set_style(2)
    chart.set_title({'name': chart_name})
    chart.set_x_axis({'name': 'Time'})
    chart.set_y_axis({'name': chart_ylabel})
    return chart


def add_chart(sheet_name, chart_name, chart_ylabel_name, db_field, db_table,
              host_list):
    """向报表中添加趋势图chart
    sheet_name:新建的sheet name
    chart_name:趋势图name
    chart_ylabel_name:趋势图纵坐标标签名
    db_field:需要查询的字段
    db_table:需要查询的表名
    host_list:控制器、分析器节点名称
    """
    host_num = len(host_list)
    sheet = table.add_worksheet(sheet_name)
    sheet.set_column('A:AD', 22)
    sum_rows = 0
    for index, value in enumerate(db_field):
        chart = set_chart_style(chart_name[index], chart_ylabel_name[index])
        for i in range(host_num):
            col = 2 * host_num * index + 2 * i
            sheet.write(0, col + 11, 'Host')
            sheet.write(0, col + 12, host_list[i],
                        format_color)
            sheet.write(1, col + 11, 'Time')
            sheet.write(1, col + 12, chart_ylabel_name[index])
            sql = 'select ' + value + ' from ' + db_table + \
                        ' where host = \'' + host_list[i] + '\' and time >= ' \
                        + last_xunjian + ' and time <= ' + this_xunjian + \
                        ' GROUP BY time(5m) fill(null)'
            result = query_influxdb(sql)
            if result:
                result_list = [ret for ret in result][0]
                sum_rows = len(result_list)
                percent_list = []
                for j, result in enumerate(result_list):
                    dt = repair_time_display(result['time'])
                    sheet.write(j + 2, col + 11, dt)
                    if value.__contains__('mean'):
                        percent_list.append(result['mean'])
                    if value.__contains__('sum'):
                        percent_list.append(result['sum'])
                    if value.__contains__('max'):
                        percent_list.append(result['max'])
                for j, pcent in enumerate(percent_list):
                    if value.__contains__('sum'):
                        if value > 0:  # 只写大于0的值
                            sheet.write(j + 2, col + 12, pcent, format_left)
                    else:
                        sheet.write(j + 2, col + 12, pcent, format_left)

            chart.add_series({
                'name': [sheet_name, 0, col + 12],
                'categories': [
                    sheet_name, 3, col + 11, sum_rows + 2, col + 11],
                'values': [
                    sheet_name, 3, col + 12, sum_rows + 2, col + 12],})
        sheet.insert_chart(40 * index + 1, 0, chart, {
            'x_offset': 40,
            'y_offset': 30
        })


def add_chart_drop_packet(direction, sheet_name, chart_name, chart_ylabel_name,
                          host_list):
    """向报表中添加网卡丢包图
    direction:方向in/out
    sheet_name:新建的sheet name
    chart_name:趋势图name
    chart_ylabel_name:趋势图纵坐标标签名
    host_list:控制器、分析器节点名称
    """
    sheet = table.add_worksheet(sheet_name)
    sheet.set_column('A:AD', 22)
    interface_dict = get_interface(interface_file_path, host_list)

    for index, host in enumerate(host_list):
        chart = set_chart_style(chart_name, chart_ylabel_name)
        col = 8 * index
        sum_rows = 0
        interface_count = 0
        for hostname, interface in interface_dict.items():
            if hostname.split(':')[0] == host:
                interface_count += 1
                if hostname.split(':')[1] == '(ctrl-if)':
                    sheet.write(0, col + 11, 'Interface')
                    sheet.write(0, col + 12, host + ':' +
                                interface + '(ctrl-if)', format_color)
                    sheet.write(1, col + 11, 'Time')
                    sheet.write(1, col + 12, chart_ylabel_name)
                    sql = 'SELECT non_negative_derivative(mean(drop_' + \
                                direction + '),1s) FROM "net" WHERE host = \'' + \
                                host + '\' and interface = \'' + \
                                interface + '\' and time >= ' + \
                                last_xunjian + ' and time <= ' + this_xunjian + \
                                ' GROUP BY time(1m) fill(null)'
                    result = query_influxdb(sql)
                    if result:
                        result_list = [ret for ret in result][0]
                        sum_rows = len(result_list)
                        for j, result in enumerate(result_list):
                            dt = repair_time_display(result['time'])
                            sheet.write(j + 2, col + 11, dt)
                            value = result['non_negative_derivative']
                            if value > 0:
                                sheet.write(j + 2, col + 12, value, format_left)
                elif hostname.split(':')[1] == '(data-if)':
                    sheet.write(0, col + 13, 'Interface')
                    sheet.write(0, col + 14, host + ':' +
                                interface + '(data-if)', format_color)
                    sheet.write(1, col + 13, 'Time')
                    sheet.write(1, col + 14, chart_ylabel_name)
                    packet_direction = ''
                    if direction == 'in':
                        packet_direction = 'rx'
                    if direction == 'out':
                        packet_direction = 'tx'
                    sql = 'SELECT non_negative_derivative(mean(' + \
                          packet_direction + \
                          '_dropped),1s) FROM "telegraf_dfi" WHERE host = \'' + \
                          host + '\' and interface = \'' + interface + \
                          '\' and time >= ' + last_xunjian + ' and time <= ' + \
                          this_xunjian + ' GROUP BY time(1m) fill(null)'
                    result = query_influxdb(sql)
                    if result:
                        result_list = [ret for ret in result][0]
                        for j, result in enumerate(result_list):
                            dt = repair_time_display(result['time'])
                            sheet.write(j + 2, col + 13, dt)
                            value = result['non_negative_derivative']
                            if value > 0:
                                sheet.write(j + 2, col + 14, value, format_left)
                elif hostname.split(':')[1] == '(tap-if)':
                    sheet.write(0, col + 15, 'Interface')
                    sheet.write(0, col + 16, host + ':' +
                                interface + '(tap-if)', format_color)
                    sheet.write(1, col + 15, 'Time')
                    sheet.write(1, col + 16, chart_ylabel_name)
                    sql = 'SELECT non_negative_derivative(mean(drop_' + \
                            direction + '),1s) FROM "net" WHERE host = \'' + \
                            host + '\' and interface = \'' + \
                            interface + '\' and time >= ' + \
                            last_xunjian + ' and time <= ' + this_xunjian + \
                            ' GROUP BY time(1m) fill(null)'
                    result = query_influxdb(sql)
                    if result:
                        result_list = [ret for ret in result][0]
                        for j, result in enumerate(result_list):
                            dt = repair_time_display(result['time'])
                            sheet.write(j + 2, col + 15, dt)
                            value = result['non_negative_derivative']
                            if value > 0:
                                sheet.write(j + 2, col + 16, value, format_left)

        for i in range(interface_count):
            cols = col + i * 2
            chart.add_series({
                'name': [sheet_name, 0, cols + 12],
                'categories': [
                    sheet_name, 2, cols + 11, sum_rows + 2, cols + 11],
                'values': [
                    sheet_name, 2, cols + 12, sum_rows + 2, cols + 12],})
        sheet.insert_chart(40 * index + 1, 0, chart, {
            'x_offset': 40,
            'y_offset': 30
        })


def add_chart_es(sheet_name, chart_name, chart_ylabel_name, bit_rx_datetime,
                 bit_rx, bit_tx_datetime, bit_tx):
    """向报表中展示客户流量趋势图
    sheet_name:新建的sheet name
    chart_name:趋势图name
    chart_ylabel_name:趋势图纵坐标标签名
    bit_rx_datetime:入流量时间列表
    bit_rx:入流量列表
    bit_tx_datetime:出流量时间列表
    bit_tx:出流量列表
    """
    sheet = table.add_worksheet(sheet_name)
    sheet.set_column('A:AD', 22)
    chart = set_chart_style(chart_name, chart_ylabel_name)

    sheet.write(0, 11, 'Time')
    sheet.write(0, 12, 'bit_rx')
    sheet.write(0, 13, 'Time')
    sheet.write(0, 14, 'bit_tx')
    num = len(bit_rx_datetime)
    for index, value in enumerate(bit_rx_datetime):
        sheet.write(index + 1, 11, value)
        sheet.write(index + 1, 12, bit_rx[index])
        sheet.write(index + 1, 13, bit_tx_datetime[index])
        sheet.write(index + 1, 14, bit_tx[index])

    chart.add_series({
        'name': [sheet_name, 0, 12],
        'categories': [sheet_name, 1, 11, num + 1, 11],
        'values': [sheet_name, 1, 12, num + 1, 12],
    })
    chart.add_series({
        'name': [sheet_name, 0, 14],
        'categories': [sheet_name, 1, 13, num + 1, 13],
        'values': [sheet_name, 1, 14, num + 1, 14],
    })
    sheet.insert_chart('A1', chart, {'x_offset': 40, 'y_offset': 30})


table = xlsxwriter.Workbook(father_path + '/report.xlsx')
sheet1 = table.add_worksheet(u'巡检报告')

style_default = setstyle(u'微软雅黑', 10, 'black')
style_headline = setstyle(u'微软雅黑', 16, 'black', True)
style_headline.set_rotation(90)
style_headline.set_align('center')
style_result = setstyle(u'微软雅黑', 16, 'black', True)
style_result.set_align('center')
style_note = setstyle(u'微软雅黑', 12, 'black', True)
style_note_bg = setstyle(u'微软雅黑', 12, 'black', True)
style_note_bg.set_pattern(1)
style_note_bg.set_bg_color('4F94CD')
style_check_content = setstyle(u'微软雅黑', 10, 'black')
style_status_pass = setstyle(u'微软雅黑', 10, 'green', True)
style_status_fail = setstyle(u'微软雅黑', 10, 'red', True)
style_check_error_content = setstyle(u'微软雅黑', 10, 'red')

format_left = table.add_format()
format_left.set_align('left')
format_color = table.add_format()
format_color.set_font_color('blue')

# 设置巡检报告的默认格式
for i in range(22):
    for j in range(7):
        sheet1.write(i, j, '', style_default)
sheet1.write(0, 0, u'客户名称', style_note)
sheet1.write(0, 3, u'巡检项目', style_note)
sheet1.write(1, 0, u'巡检人', style_note)
sheet1.write(1, 3, u'巡检日期', style_note)
sheet1.merge_range('B1:C1', '', style_check_content)
sheet1.merge_range('E1:G1', '', style_check_content)
sheet1.merge_range('B2:C2', '', style_check_content)
sheet1.merge_range('E2:G2', '', style_check_content)
sheet1.merge_range('A3:A19', u'自动化巡检条目', style_headline)
sheet1.merge_range('A20:A22', u'巡检总结', style_result)
sheet1.merge_range('C20:G20', '', style_check_content)
sheet1.merge_range('C21:G21', '', style_check_content)
sheet1.merge_range('C22:G22', '', style_check_content)

row_list = [u'组件', u'巡检项', u'检查内容', u'检查结果', u'异常内容', u'备注']
for cel in range(len(row_list)):
    sheet1.write(2, cel + 1, row_list[cel], style_note_bg)
sheet1.set_column('A:B', 12)
sheet1.set_column('C:C', 14)
sheet1.set_column('D:D', 32)
sheet1.set_column('E:E', 10)
sheet1.set_column('F:F', 45)
sheet1.set_column('G:G', 32)

sheet1.write(3, 1, u'界面数据展示', style_check_content)
sheet1.merge_range('B5:B7', u'系统基础状态', style_check_content)
sheet1.merge_range('B8:B12', u'产品基础状态', style_check_content)
sheet1.merge_range('B13:B16', u'计算服务集群', style_check_content)
sheet1.merge_range('B17:B18', u'存储服务集群', style_check_content)
sheet1.write(18, 1, u'进程日志', style_check_content)
sheet1.write(19, 1, u'处理操作', style_check_content)
sheet1.write(20, 1, u'处理意见', style_check_content)
sheet1.write(21, 1, u'巡检结论', style_check_content)

last_xunjian_time = ''
this_xunjian_time = ''
check_point_list = []
check_content_list = []
check_result_list = []
abnormal_content_row_list = []
abnormal_advice_row_list = []
check_result_fail_row_list = []
with open(target_file_path, 'r') as file:
    for line, value in enumerate(file):
        if value.__contains__('上次巡检时间'):
            last_xunjian_time = value.split('：')[-1].split(' ')[1] + ' ' + \
                                value.split('：')[-1].split(' ')[2]
        if value.__contains__('本次巡检时间'):
            this_xunjian_time = value.split('：')[-1].split(' ')[1] + ' ' + \
                                value.split('：')[-1].split(' ')[2]
        if value.__contains__('检 查 点'):
            check_point_list.append(value.split('：')[-1].split()[0])
        if value.__contains__('检查内容'):
            check_content_list.append(value.split('：')[-1].split()[0])
        if value.__contains__('检查结果'):
            check_result = get_line_content(target_file_path,
                                            line + 2).split('\t')[1].strip()
            check_result_list.append(check_result)
        if value.__contains__('异常内容'):
            abnormal_content_row_list.append(line + 1)
        if value.__contains__('处理建议'):
            abnormal_advice_row_list.append(line + 1)

for index, value in enumerate(check_point_list):
    sheet1.write(index + 3, 2, value.decode("utf-8"), style_check_content)

for index, value in enumerate(check_content_list):
    sheet1.write(index + 3, 3, value.decode("utf-8"), style_check_content)

for index, value in enumerate(check_result_list):
    if value == 'Pass':
        sheet1.write(index + 3, 4, value.decode("utf-8"), style_status_pass)
    else:
        sheet1.write(index + 3, 4, value.decode("utf-8"), style_status_fail)
        check_result_fail_row_list.append(index)

# 取异常内容与异常处理建议中间的一段
for index, value in enumerate(abnormal_content_row_list):
    abnormal_content = ""
    for i in range(1, abnormal_advice_row_list[index] - value):
        abnormal_content += get_line_content(target_file_path,
                     abnormal_advice_row_list[index] - i).split('\t')[1].strip()\
                            + '\n'
        sheet1.write(check_result_fail_row_list[index] + 3, 5,
                     abnormal_content.strip('\n').decode("utf-8"),
                     style_check_error_content)

last_xunjian_datetime = string_datetime(last_xunjian_time)
this_xunjian_datetime = string_datetime(this_xunjian_time)
last_xunjian_unix = datetime_unix(last_xunjian_datetime)
this_xunjian_unix = datetime_unix(this_xunjian_datetime)
last_xunjian = str(last_xunjian_unix).split('.')[0] + '000000000'
this_xunjian = str(this_xunjian_unix).split('.')[0] + '000000000'

cpu_usage_chart_name = [
    'Average System usage', 'Average User usage', 'Average iowait usage'
]
cpu_usage_chart_ylabel = [
    'System usage percent', 'User usage percent', 'iowait usage percent'
]
cpu_usage_db_field = [
    'mean(usage_system)', 'mean(usage_user)', 'mean(usage_iowait)'
]
load1_chart_name = ['Average load1', 'Max load1']
load1_chart_ylabel = ['Average load1', 'Max load1']
load1_db_field = ['mean(load1)', 'max(load1)']
load5_chart_name = ['Average load5', 'Max load5']
load5_chart_ylabel = ['Average load5', 'Max load5']
load5_db_field = ['mean(load5)', 'max(load5)']
load15_chart_name = ['Average load15', 'Max load15']
load15_chart_ylabel = ['Average load15', 'Max load15']
load15_db_field = ['mean(load15)', 'max(load15)']

# >>>>>>>>>>>>>>>>>>>>>>绘制 Mem 趋势图<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
add_chart('Mem Trend', ['Average Mem Trend'], ['Mem used percent'],
          ['mean(used_percent)'], 'mem', host_list)

# >>>>>>>>>>>>>>>>>>>>>>绘制 Heap 趋势图<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
add_chart('Heap Trend', ['Average Heap Trend'], ['Heap percent usage'],
          ['mean(mem_heap_used_percent)'], 'elasticsearch_jvm', host_list)

# >>>>>>>>>>>>>>>>>>>>>>绘制 CPU 趋势图<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
add_chart('CPU usage', cpu_usage_chart_name, cpu_usage_chart_ylabel,
          cpu_usage_db_field, 'cpu', host_list)

# >>>>>>>>>>>>>>>>>>>>>>绘制 Load1 趋势图<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
add_chart('load1', load1_chart_name, load1_chart_ylabel, load1_db_field,
          'system', host_list)

# >>>>>>>>>>>>>>>>>>>>>>绘制 Load5 趋势图<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
add_chart('load5', load5_chart_name, load5_chart_ylabel, load5_db_field,
          'system', host_list)

# >>>>>>>>>>>>>>>>>>>>>>绘制 Load15 趋势图<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
add_chart('load15', load15_chart_name, load15_chart_ylabel, load15_db_field,
          'system', host_list)
# 判断客户计算节点是否有telegraf进程
sql = 'SHOW TAG VALUES FROM trident_dispatcher_rx WITH KEY=host'
result = query_influxdb(sql)
if result:
    result_list = [ret for ret in result][0]
    trident_host_list = []
    for hit in result_list:
        trident_host_list.append(hit['value'])
    # >>>>>>>>>>>>>>>>>>>>绘制 trident 内核丢包图<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    add_chart('trident_kernel_drop_packets',
              ['Sum trident kernel drop packets'], ['value'], ['sum(value)'],
              'trident_dispatcher_kernel_drops', trident_host_list)

# >>>>>>>>>>>>>>>>>>>>>>绘制网卡丢包图<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
add_chart_drop_packet('in', 'network_card_drop_packets_in',
                      'Average diff of drop packets', 'packets drop',
                      host_list)
add_chart_drop_packet('out', 'network_card_drop_packets_out',
                      'Average diff of drop packets', 'packets drop',
                      host_list)

# 绘制客户业务流量趋势图
bit_rx = []
bit_tx = []
bit_rx_datetime = []
bit_tx_datetime = []
if system_version == 'DeepFlow-5.3.3':
    cmd = 'curl -s -XPOST \'http://127.0.0.1:20042/dfi_bw_usage_isp_usage__1h_*/' \
          '_search?pretty\' -d \'{"size":0,"query":{"bool":{"filter":[{"range":' \
          '{"timestamp":{"gte":' + str(last_xunjian_unix).split('.')[0] + \
          ',"lt":' + str(this_xunjian_unix).split('.')[0] + '}}},' \
        '{"term":{"tag._code":0}}]}},"aggs":{"bitAvg":{"terms":{"field":"' \
        'timestamp","size":1534496742},"aggs":{"bit_tx":{"sum":{' \
        '"field":"sum.bit_tx"}},"bit_rx":{"sum":{"field":"sum.bit_rx"}}}}}}\''
    ret = execute_command(cmd)
    ret_dict = json.loads(ret)
    buckets = ret_dict['aggregations']['bitAvg']['buckets']
    for bucket in buckets:
        bit_rx.append(int(bucket['bit_rx']['value']) / 60.0 / 60.0 / 1024.0)
        bit_tx.append(int(bucket['bit_tx']['value']) / 60.0 / 60.0 / 1024.0)
        bit_rx_datetime.append(timestamp_datetime(int(bucket['key_as_string'])))
        bit_tx_datetime.append(timestamp_datetime(int(bucket['key_as_string'])))

    # >>>>>>>>>>>>>>>>>>>>绘制客户业务流量趋势图<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    add_chart_es('customer traffic trend', 'customer traffic trend', 'Kbit/s',
                 bit_rx_datetime, bit_rx, bit_tx_datetime, bit_tx)

table.close()
print '\033[1;34m输出报表位置目录:' + father_path + '/report.xlsx \033[0m'
# cmd = 'rm -rf target_file.txt'
# execute_command(cmd)
