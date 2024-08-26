import logging
from datetime import datetime

# 数据库相关配置
MARIADB_HOST = 'localhost'
MARIADB_PORT = 3306
MARIADB_USERNAME = ''
MARIADB_POSSWORD = ''
MARIADB_DATABASE = ''
MARIADA_CHARSET = 'utf8mb4'

now = datetime.now()
# 获取当前年份
current_year = now.year

# 距离今年的时间范围
year_range = 2
# url配置

# 需要爬虫的IP
main_urls = [{
    "name":
    "url1",
    "address":
    ""
}, {
    "name":
    "url2",
    "address":
    ""
}]

# url1的请求基础地址
url1_base_url = ""
# url2的请求基础地址
url2_base_url = ""
# url2下载文件的请求基础地址
url2_file_base_url = ""
# 每个url爬取内容的存储字段
#  url1 文章大类别  文章小类别  文章时间戳  文章标题 文章正文内容
#  url2 文章标题    文章时间戳   文章标题  文章有效正文内容

# IP代理池
requests_proxies = {}

aiohttp_proxy = ''

# 爬虫请求头UA伪装
headers = {
    'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

# 响应设置编码
response_encoding = 'utf8'

# logger日志配置
logger = logging.getLogger(__name__)
# 设置logger的级别
logger.setLevel(logging.INFO)
# 创建一个控制台处理器
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
# 创建一个文件处理器
file_handler = logging.FileHandler('app.log', encoding='utf-8')
file_handler.setLevel(logging.INFO)
# 创建一个日志格式器并设置给处理器
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
# 将处理器添加到logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)
