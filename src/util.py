#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from os import path, getcwd
from datetime import datetime
import configparser


def generate_vehicle_json(dataset: tuple, keys):
    """
    根据配置文件封装数据库车辆数据
    :param dataset: 数据库返回的数据组
    :param keys: 配置文件的数据
    :return: 封装好的字典(json)
    """

    # 生成json模板
    total_json = {}
    data_json = {}
    for key in keys:
        data_json.update({key: ''})
    for data in dataset:
        total_json.update({f'{data[3]}': data_json.copy()})

    # 导入数据
    dataset_index = 0                              # 嵌套元组下标

    for data in total_json:
        dataset_element_index = 1                  # 嵌套元组中每个子元组的下标
        for key in total_json[data]:
            total_json[data][key] = dataset[dataset_index][dataset_element_index]
            dataset_element_index += 1
        # 归零操作，并转到下一组数据
        dataset_index += 1

    return total_json


def generate_latency_json(dataset: tuple, keys):
    total_json = {}
    dataset = [list(_) for _ in dataset]
    # 导入标签数据
    keys[0] = ''
    total_json.update({'labels': keys})

    # 生成模板
    for index,datas in enumerate(dataset):
        datas[0] = ''
        protocol = datas.pop(1)
        total_json.update({f'{protocol}': datas})
    return total_json


def generate_repr_statement(handle, not_normal=True):
    """
        生成数据库替换语句
        :param not_normal:
        :param handle: 根据配置文件生成的列表
        :return: 返回数据库需要的语句
    """
    key_str = ''
    for index in range(len(handle)):
        if not_normal and handle[index] == 'type':
            continue
        elif index == len(handle) - 1:
            key_str += handle[index] + ' = %s'
            continue
        key_str += handle[index] + ' = %s,'

    return key_str


def find_config():
    """查找配置文件
    :return: 配置文件类容，配置文件路径
    """
    base_dir = getcwd().split('\\src')[0]
    config   = path.join(base_dir, 'src\\config.ini')
    cf = configparser.ConfigParser()

    return cf, config


def send_latency(string):
    """
    计算发送时延
    :param string: 接收到的数据包
    :return: 延时(ms)
    """
    if type(string) == bytes:
        bit = len(string) * 8
    elif type(string) == str:
        bit = len(string.encode('utf-8')) * 8
    else:
        return None
    latency = bit / 10 ** 9
    return latency


def cur_time2ms(now: datetime.now):
    """
    当前时间转毫秒
    :param now: 此参数需要 datetime.now 的时间
    :return: 总毫秒
    """
    cur_time = now.strftime("%H:%M:%S:%f")[:-3]
    time_list = [int(_) for _ in cur_time.split(":")]
    total_ms = (time_list[0] * 3600 + time_list[1] * 60 + time_list[2]) * 1000 + time_list[3]
    return total_ms


def filter_packet(data):
    """
    过滤因为硬件过多发送重复数据包
    :param data:
    :return:
    """
    handle_data_list = data.split(',')[:8]              # [:8]：默认发出的数据包为8项以内
    processing_lat = handle_data_list[-1]               # 这里指的是数据包的最后一项：硬件处理时延

    # 如果在最后一项中发现了多余内容（通常是type），则删除此索引之后的内容
    if 'type' in processing_lat:
        index = processing_lat.index('type')
        handle_data_list[-1] = processing_lat[:index]

    data = ",".join(c for c in handle_data_list)        # 重新拼装正确的数据包格式
    return data


if __name__ == '__main__':
    a = datetime.now()
    print(cur_time2ms(a))