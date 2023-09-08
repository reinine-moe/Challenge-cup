#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from os import path, getcwd
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
    for datas in dataset:
        datas[0] = ''

    # 导入标签数据
    keys.insert(0, "")
    total_json.update({'labels': tuple(keys)})

    # 生成模板
    for index,datas in enumerate(dataset):
        total_json.update({f'values{index+1}': tuple(datas)})
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

if __name__ == '__main__':
    a = ((1, 1.5567, 0.0076, 0.00008),)
    keys = ['send_latency', 'hardware_latency', 'handle_latency']
    result = generate_latency_json(a,keys)
    print(result)