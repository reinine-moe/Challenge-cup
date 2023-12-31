#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask import Flask
from flask_cors import CORS
from flask_compress import Compress
from src.socket_connect import *
from src.util import find_config, generate_vehicle_json, generate_latency_json
import time
import signal
import threading

"""全局变量"""
cf, config = find_config()
cf.read(config, encoding='utf-8')

app = Flask(__name__)
app.config['COMPRESS_REGISTER'] = False
CORS(app, resources=r'/*')  # 设置跨域
compress = Compress(app)    # 压缩数据包

sql = Mysql()
sql.init_table()


@app.route('/')
def index():
    return 'hello client'


@app.route('/<type>/api')
def response(type):
    if type == 'vehicle':
        keys = cf.get('general setting', 'vehicle_key').split(',')
        result = sql.fetch_data(sql.vehicle_table)
        json_result = generate_vehicle_json(result,keys)

        json_result.update({'info': {'recent upload': sql.fetch_latest_time(sql.vehicle_table), 'sum': len(result)}})
        return json_result

    elif type == 'latency':
        keys = cf.get('general setting', 'latency_key').split(',')
        result = sql.fetch_data(sql.latency_table)
        json_result = generate_latency_json(result, keys)

        return json_result
    else:
        return {'msg': 'bad request'}


def runserver():
    # 信号处理
    signal.signal(signal.SIGINT, signal.SIG_DFL)         # 注册对 SIGINT 信号的处理。
                                                         # SIGINT 信号通常是在用户按下Ctrl+C键时发送给程序的，
                                                         # 用于请求程序终止。

    signal.signal(signal.SIGTERM, signal.SIG_DFL)        # SIGTERM信号是由操作系统发送给进程，用于请求进程终止。

    #  创建线程
    main_thr = threading.Thread(target=app.run, args=['0.0.0.0'])  # '192.168.0.100' '0.0.0.0'
    main_thr.daemon = True
    main_thr.start()
    time.sleep(0.2)

    comm_thr = threading.Thread(target=server_start)
    comm_thr.daemon = True
    comm_thr.start()

    while True:
        pass


if __name__ == '__main__':
    runserver()
