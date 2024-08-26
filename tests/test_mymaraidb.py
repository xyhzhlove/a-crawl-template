from src.automatic_crawler.core.my_mariadb import MyMariadb

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
