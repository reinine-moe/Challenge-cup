#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymysql
from src.util import find_config, generate_repr_statement

"""全局变量"""
cf, config = find_config()
cf.read(config, encoding='utf-8')


class Mysql:
    db_name = 'vehicledb'
    vehicle_table = 'vehicle_table'
    latency_table = 'latency_table'

    def __init__(self):

        self.host = cf.get('sql setting', 'host')
        self.user = cf.get('sql setting', 'user')
        self.password = cf.get('sql setting', 'passwd')
        self.port = int(cf.get('sql setting', 'port'))

        create_db = f"CREATE DATABASE IF NOT EXISTS {self.db_name} character set utf8;"

        con = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port,
        )

        db_cur = con.cursor()
        db_cur.execute(create_db)

        db_cur.close()
        con.close()

    def connect(self):
        con = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port,
            database=self.db_name
        )
        return con

    def init_table(self):
        vehicle_keys = cf.get('general setting', 'vehicle_key').split(',')
        key_v_str = ''

            # 生成语句
        for index in range(len(vehicle_keys)):
            if index == 0:
                key_v_str += f'\n{" " * 16}{vehicle_keys[index]} CHAR(10),\n'
                continue
            elif index == len(vehicle_keys) - 1:
                key_v_str += f'{" " * 16}{vehicle_keys[index]} VARCHAR(20)\n'
                break
            key_v_str += f'{" " * 16}{vehicle_keys[index]} VARCHAR(20),\n'

        create_v_table = f'''
        CREATE TABLE IF NOT EXISTS {self.vehicle_table}
        (
            id INT auto_increment primary key ,{key_v_str}
        );
    '''

        latency_keys = cf.get('general setting', 'latency_key').split(',')
        keys_l_str = ''

        for l_index in range(len(latency_keys)):
            if l_index == 0:
                keys_l_str += f'\n{" " * 16}{latency_keys[l_index]} CHAR(4),\n'
                continue
            elif l_index == len(latency_keys) - 1:
                keys_l_str += f'{" " * 16}{latency_keys[l_index]} DOUBLE\n'
                break
            keys_l_str += f'{" " * 16}{latency_keys[l_index]} DOUBLE,\n'
        create_l_table = f'''
        CREATE TABLE IF NOT EXISTS {self.latency_table}
        (
            id INT auto_increment primary key ,{keys_l_str}
        );
    '''

        con = self.connect()
        cursor = con.cursor()
        cursor.execute(create_v_table)
        cursor.execute(create_l_table)

        cursor.close()
        con.close()

    def insert_one(self, key, dataset, table_name):
        """
        插入一条数据
        :param key: 配置文件中的数据
        :param dataset: 接收到的数据集
        :param table_name: 表的名称
        """
        con = self.connect()
        table_cur = con.cursor()
        key_list = key.split(',')

        symbol_str = '%s,' * len(key_list)
        handle = f"INSERT INTO {table_name}({key}) VALUES({symbol_str[:-1]});"
        table_cur.execute(handle, tuple(i for i in dataset))

        con.commit()
        table_cur.close()
        con.close()
        print(f'{table_name.split("_")[0]} record inserted\n')

    def save_vehicle_data(self, dataset: tuple or list):
        def commit():
            con.commit()
            table_cur.close()
            con.close()
            print('vehicle record inserted\n')

        con = self.connect()
        table_cur = con.cursor()

        keys = cf.get('general setting', 'vehicle_key')
        keys_list = keys.split(',')
        dbResult = self.fetch_data(self.vehicle_table)

        # 若数据库里没有数据则直接添加一条新数据
        if len(dbResult) == 0:
            self.insert_one(keys,dataset,self.vehicle_table)
            return

        for data in dbResult:
            if data[1] == 'accident' and data[3] == dataset[2]:
                repr_str = generate_repr_statement(keys_list)     # 根据配置文件生成替换语句
                replace = f"UPDATE {self.vehicle_table} SET {repr_str} WHERE type = 'accident' AND vid = {dataset[2]}"
                table_cur.execute(
                    replace, tuple(i for i in dataset if i not in ('normal', 'accident'))
                )                                            # 不管当前传入的数据是正常车还是事故车，只要之前存入数据库中的vid被判定为事故车的话，
                                                             # 就不会再改变此vid的类型，只更新其他参数
                commit()
                return

            elif data[1] == 'normal' and data[3] == dataset[2]:
                repr_str = generate_repr_statement(keys_list, not_normal=False)
                replace = f"UPDATE {self.vehicle_table} SET {repr_str} WHERE type = 'normal' AND vid = {dataset[2]}"
                table_cur.execute(replace,tuple(i for i in dataset))
                commit()
                return

        symbol_str = '%s,' * len(keys_list)  # 根据配置文件生成与之对应值的插入语句
        handle = f"INSERT INTO {self.vehicle_table}({keys}) VALUES({symbol_str[:-1]});"
        result = tuple(i for i in dataset)
        table_cur.execute(handle, result)
        commit()

    def save_latency_data(self, dataset: tuple or list):
        def commit():
            con.commit()
            table_cur.close()
            con.close()
            print('latency record inserted\n')

        con = self.connect()
        table_cur = con.cursor()

        keys = cf.get('general setting', 'latency_key')
        keys_list = keys.split(',')
        dbResult = self.fetch_data(self.latency_table)

        # 若数据库里没有数据则直接添加一条新数据
        if len(dbResult) == 0:
            self.insert_one(keys,dataset,self.latency_table)
            return

        protocol = dataset[0]
        for data in dbResult:
            if data[1] == protocol:
                repr_str = generate_repr_statement(keys_list)     # 根据配置文件生成替换语句
                print(repr_str)
                replace = f"UPDATE {self.latency_table} SET {repr_str} WHERE 类型 = '{protocol}'"
                table_cur.execute(
                    replace, tuple(i for i in dataset)
                )
                commit()
                return

        symbol_str = '%s,' * len(keys_list)  # 根据配置文件生成与之对应值的插入语句
        handle = f"INSERT INTO {self.latency_table}({keys}) VALUES({symbol_str[:-1]});"
        result = tuple(i for i in dataset)
        table_cur.execute(handle, result)
        commit()

    def fetch_data(self, table_name):
        """
        抓取表数据
        :param table_name: 表名
        :return: 表结果
        """
        con = self.connect()
        cursor = con.cursor()

        query_data = f'select * from {table_name}; '
        cursor.execute(query_data)

        result = cursor.fetchall()

        cursor.close()
        con.close()

        return result

    def fetch_latest_time(self, table_name):
        """
        抓取最近录入数据的时间
        :param table_name: 表名
        :return: 表时间
        """
        con = self.connect()
        cursor = con.cursor()

        fetch_time = "SELECT `TABLE_NAME`, `UPDATE_TIME` FROM `information_schema`.`TABLES` " \
                     f"WHERE `information_schema`.`TABLES`.`TABLE_SCHEMA` = '{self.db_name}' " \
                     f"AND`information_schema`.`TABLES`.`TABLE_NAME` = '{table_name}';"

        cursor.execute(fetch_time)
        result = cursor.fetchone()

        cursor.close()
        con.close()

        return result[1]

if __name__ == '__main__':
    keys = cf.get('general setting', 'latency_key').split(',')
    sql = Mysql()
    sql.init_table()
    data = ['udp',0.12,0.021,0.238,0.38,0.0683]
    sql.save_latency_data(data)

