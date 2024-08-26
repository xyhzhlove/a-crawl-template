#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@File    :   my_mariadb.py
@Time    :   2024/08/16 11:35:02
@Author  :   许永辉
@Version :   0.1.0.dev
@Desc    :   mariadb数据库的封装
'''

import mariadb
import traceback
from automatic_crawler.configs.main_config import logger, MARIADB_PORT, MARIADB_HOST, MARIADB_POSSWORD, MARIADB_USERNAME, MARIADB_DATABASE, MARIADA_CHARSET


class MyMariadb:

    def __init__(self,
                 user=MARIADB_USERNAME,
                 password=MARIADB_POSSWORD,
                 host=MARIADB_HOST,
                 port=MARIADB_PORT,
                 database=MARIADB_DATABASE,
                 charset=MARIADA_CHARSET):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.charset = charset
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = mariadb.connect(user=self.user,
                                        password=self.password,
                                        host=self.host,
                                        port=self.port,
                                        database=self.database)
            self.cursor = self.conn.cursor()
        except mariadb.Error as e:
            logger.error(traceback.format_exc())

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def create_database(self, db_name):
        try:
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        except mariadb.Error as e:
            logger.error(traceback.format_exc())

    def delete_database(self, db_name):
        try:
            self.cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        except mariadb.Error as e:
            logger.error(traceback.format_exc())

    def create_index_if_not_exists(self, index_name, table_name, columns):
        index_columns = ', '.join(columns)
        check_index_query = f"SHOW INDEXES FROM {table_name} WHERE Key_name = '{index_name}'"

        # 检查索引是否已经存在
        self.cursor.execute(check_index_query)
        if not self.cursor.fetchone():
            # 如果索引不存在，则创建索引
            create_index_query = f"CREATE UNIQUE INDEX {index_name} ON {table_name} ({index_columns})"
            self.cursor.execute(create_index_query)
            self.conn.commit()

    def create_table(self, table_name, columns):
        try:
            col_defs = ','.join(columns)
            self.cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs}) DEFAULT CHARSET=utf8"
            )
            # 创建联合索引
            self.create_index_if_not_exists(
                'idx_title_lg_sm', table_name,
                ['title', 'lg_category', 'sm_category'])
        except mariadb.Error as e:
            logger.error(traceback.format_exc())

    def drop_table(self, table_name):
        try:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        except mariadb.Error as e:
            logger.error(traceback.format_exc())

    def insert_into_table(self, table_name, columns, values):
        '''
        实现逻辑:假如，根据创建的联合索引 idx_title_lg_sm 与即将插入新数据的title lg_category sm_category 在原始表中查询不到记录那么将新数据直接插入
        如果在原始表中找到了记录，如果新插入数据的timestamp大于查找到纪律的timestamp，那么将数据表中的记录信息全部更新为新插入数据的信息
        如果如果新插入数据的timestamp小于等于查找到纪律的timestamp那么不进行更新
        '''
        # 定义列名字符串和值的占位符
        columns_str = ','.join(columns)
        placeholders = ','.join(['%s'] * len(columns))
        query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE
                content = IF(VALUES(timestamp) > timestamp, VALUES(content), content),
                timestamp = IF(VALUES(timestamp) > timestamp, VALUES(timestamp), timestamp)    
        """
        # 这里要注意: 一定要凭着时间戳先更新内容，最后再更新时间戳，因为更新内容时仍然要借助表中的时间戳进行比较后才能决定是否更新。如果先更新了时间戳，那么再去更新内容时由于表中的时间戳已经更新了，这时再进行时间戳比较时已经无法正确更新内容了

        # 尝试执行插入/更新操作
        try:
            self.cursor.executemany(query, values)
            self.conn.commit()
        except mariadb.Error as e:
            self.conn.rollback()
            # 这里可以添加日志记录或其他错误处理逻辑
            # logger.error(traceback.format_exc())
            # 处理插入失败的情况
            for value in values:
                try:
                    self.cursor.executemany(query, [value])
                    self.conn.commit()
                except mariadb.Error as e:
                    self.conn.rollback()
                    # 可以在这里记录具体的错误信息
                    # logger.error(f"Error inserting record: {value}")


if __name__ == "__main__":
    db_manager = MyMariadb()
    db_manager.connect()

    # 创建表
    columns = [
        'id INT AUTO_INCREMENT PRIMARY KEY', 'title VARCHAR(100)',
        'lg_category VARCHAR(100)', 'sm_category VARCHAR(100)', 'content TEXT',
        'timestamp DATE'
    ]
    db_manager.create_table('policy', columns)

    values = [('文档1', '大类别1', "小类别1", '旧内容1', '2024-01-05'),
              ('文档1', '大类别1', "小类别1", '新内容1', '2024-01-06'),
              ('文档2', '大类别2', "小类别2", '旧内容1', '2024-01-05'),
              ('文档2', '大类别2', "小类别3", '内容22', '2024-01-05')]
    
    # 结果应该是('文档1', '大类别1', "小类别1", '新内容1', '2024-01-06'),
    # ('文档2', '大类别2', "小类别2", '旧内容1', '2024-01-05'),
    # ('文档2', '大类别2', "小类别3", '内容22', '2024-01-05')
    # 批量插入数据
    db_manager.insert_into_table(
        'policy',
        ['title', 'lg_category', 'sm_category', 'content', 'timestamp'],
        values=values)

    db_manager.disconnect()
