#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@File    :   main.py
@Time    :   2024/08/13 09:32:20
@Author  :   许永辉
@Version :   0.1.0.dev
@Desc    :   一个线程 + 线程池 + 爬虫 + mariadb存储的简单实例
'''
import concurrent.futures
import threading
import io
import re
import os
import sys
import requests
from lxml import etree
from automatic_crawler.core.my_mariadb import MyMariadb
from automatic_crawler.configs.main_config import logger, main_urls, url1_base_url, url2_base_url, requests_proxies, headers, url2_file_base_url, response_encoding, current_year, year_range


class CrawlUrl1:

    def __init__(self, mymariadb_instance: MyMariadb):
        self.mymariadb_instance = mymariadb_instance

    def crawlUrl1Document(self, item):
        # print(int(item['timestamp'].split("-")[0]))
        if int(item['timestamp'].split("-")[0]) + year_range >= current_year:
            response = requests.get(url=item["url"],
                                    headers=headers,
                                    proxies=requests_proxies)
            if response.status_code != 200:
                if response.status_code == 404:
                    logger.info(f'{item["url"]}页面不存在')
                else:
                    logger.info(f'{item["url"]}页面异常')
            html_str = response.text
            logger.info(f'{item["title"]}爬取完成，网址为{item["url"]}')
            html_result = etree.HTML(html_str)
            doc_content = html_result.xpath("")
            if len(doc_content) == 0:
                doc_content = html_result.xpath("")
            doc_content = [
                re.sub(r'\n\r|\n|\r|\xa0|\r\t|\t|&nbsp;', '', item)
                for item in doc_content
            ]
            doc_content = ''.join(doc_content)
            document_item = {
                "lg_category": item['lg_category'],
                "sm_category": item['sm_category'],
                "title": item["title"],
                "timestamp": item["timestamp"],
                "content": doc_content
            }

            return document_item
        else:
            logger.info('信息过于陈旧，不进行获取')

    def crawlUrl1PageUrls(self, main_urls_items):
        # global ws
        if len(main_urls) < 1:
            return
        middle_results = []
        results = []
        for item in main_urls_items:
            response = requests.get(item['url'],
                                    headers=headers,
                                    proxies=requests_proxies)
            if response.status_code != 200:
                if response.status_code == 404:
                    logger.info(f'{item["url"]}页面不存在')
                else:
                    logger.info(f'{item["url"]}页面异常')
            html_str = response.text
            logger.info(f'{item["url"]}信息获取完成')
            middle_results.append({
                "lg_category": item['lg_category'],
                "sm_category": item['sm_category'],
                "html_str": html_str
            })
    # print(results)
        for item in middle_results:
            html_result = etree.HTML(item['html_str'])
            middle_title_url_times = html_result.xpath(
                ''
            )
            # print(title_url_times)

            for i in range(0, len(middle_title_url_times), 3):
                results.append({
                    "lg_category":
                    item['lg_category'],
                    "sm_category":
                    item['sm_category'],
                    "url": (url1_base_url + middle_title_url_times[i]),
                    "title":
                    middle_title_url_times[i + 1],
                    "timestamp":
                    middle_title_url_times[i + 2]
                })
        d_result = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 提交任务到线程池，并获取 Future 对象
            futures = [
                executor.submit(self.crawlUrl1Document, item)
                for item in results
            ]
            for future in concurrent.futures.as_completed(futures):
                try:

                    item = future.result()
                    # print('==================================================')
                    # print(item)
                    if item is not None:
                        try:
                            content = item["content"]
                            pattern = r'\\x[0-9a-fA-F]{2}'  # 匹配形如 \xXX 的转义序列
                            content = re.sub(pattern, '', content)
                            content = content.encode(
                                'utf-8', errors='replace').decode('utf-8')
                            d_result.append(
                                (item["title"], item['lg_category'],
                                 item['sm_category'], content,
                                 item["timestamp"]))
                            # 存储为excel表格
                            # ws.append([
                            #     item['lg_category'], item['sm_category'],
                            #     item['title'], content, item['timestamp']
                            # ])
                        except Exception as e:
                            logger.error('编码格式错误')

                except Exception as e:
                    print(f"Task  generated an exception: {e}")

        # 执行数据存储操作
        self.mymariadb_instance.connect()
        columns = [
            'id INT AUTO_INCREMENT PRIMARY KEY',  # 自增的整数主键'
            'title VARCHAR(100)',
            'lg_category VARCHAR(100)',
            'sm_category VARCHAR(100)',
            'content TEXT',
            'timestamp DATE'
        ]
        self.mymariadb_instance.create_table('policy', columns)
        if len(d_result) > 0:
            self.mymariadb_instance.insert_into_table('policy', [
                'title', 'lg_category', 'sm_category', 'content', 'timestamp'
            ],
                                                      values=d_result)
        self.mymariadb_instance.disconnect()
        logger.info('该次数据存储完成')

    

    def crawlMainUrl1(self):
        response = requests.get(url=main_urls[0]['address'],
                                headers=headers,
                                proxies=requests_proxies)
        if response.status_code != 200:
            if response.status_code == 404:
                logger.info(f'{item["url"]}页面不存在')
            else:
                logger.info(f'{item["url"]}页面异常')
        result = response.text
        logger.info(f'{main_urls[0]["address"]}请求完成')
        # 先注释掉，写完所有代码后再取消注释
        html_result = etree.HTML(result)
        pannel_texts = html_result.xpath(
            ''
        )
        pannel_results = [item.strip().split(" ")[1] for item in pannel_texts]
        # 获取大类的爬虫代码
        ul_elements = html_result.xpath(
            '')
        # 获取大类的ul元素的代码
        middle_results = []
        for index, item in enumerate(pannel_results):
            middle_results.append({
                "lg_category": pannel_results[index],
                "ul_element": ul_elements[index]
            })
        results = []
        for item in middle_results:
            ul_elements = item['ul_element'].xpath('ul')
            ul_effective_elements = []
            for ul_element in ul_elements:
                li_head_category = ul_element.xpath(
                    "li[@class='ui-list-head']/a//text()")
                if len(li_head_category) > 0 and (li_head_category[0] == ""
                                                  or li_head_category[0]
                                                  == ''):
                    ul_effective_elements.append({
                        "lg_category":
                        item['lg_category'],
                        "ul_element":
                        ul_element
                    })
            #    获取大类下的有效子类内容的爬虫代码
            for ul_effective_element in ul_effective_elements:
                middle_url_titles = ul_effective_element['ul_element'].xpath(
                    ''
                )
                url_titles = []
                # print(len(middle_url_titles))
                for i in range(0, len(middle_url_titles), 2):
                    url_titles.append({
                        "lg_category":
                        item['lg_category'],
                        "sm_category":
                        middle_url_titles[i],
                        "url": (url1_base_url + middle_url_titles[i + 1])
                    })
                results.append(url_titles)
        logger.info('main_urls数据构造结束')

        # print(results)
        # crawlUrl1PageUrls(results)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 提交任务到线程池，并获取 Future 对象
            futures = [
                executor.submit(self.crawlUrl1PageUrls, main_urls_items)
                for main_urls_items in results
            ]


class CrawlUrl2:
    def __init__(self, mymariadb_instance: MyMariadb):
        self.mymariadb_instance = mymariadb_instance

    def url2_download_file(self, item):
        response = requests.get(url=item["file_url"],
                                headers=headers,
                                proxies=requests_proxies)
        if response.status_code != 200:
            logger.info(f'{item["file_url"]}请求无效')
            return
        if not os.path.exists('files'):
            # 如果不存在，创建文件夹
            os.makedirs('files')
            logger.info('创建files文件夹')
        if not os.path.exists(f'files\\{item["title"]}'):
            os.makedirs(f'files\\{item["title"]}')
            logger.info(f'创建files\\{item["title"]}文件夹')
        pdf_content = io.BytesIO(response.content)
        # 将PDF文件内容写入到本地文件中
        with open(f'files\\{item["title"]}\\{item["file_title"]}',
                  'wb') as file:
            file.write(pdf_content.read())
            logger.info(f'files\\{item["title"]}\\{item["file_title"]}写入完成')

    def crawlUrl2Document(self, item):
        # 处理信息不仅是下文件
        response = requests.get(url=item['url'],
                                proxies=requests_proxies,
                                headers=headers)
        if response.status_code != 200:
            if response.status_code == 404:
                logger.info(f'{item["url"]}页面不存在')
            else:
                logger.info(f'{item["url"]}页面异常')

            return
        # 改变编码格式
        response.encoding = response_encoding
        # print(response.text)
        html_str = response.text
        logger.info(f'{item["url"]}文件信息获取成功')
        html_result = etree.HTML(html_str)
        file_urls = html_result.xpath(
            '')
        titles = html_result.xpath(
            '')
        file_urls = [(url2_file_base_url + item) for item in file_urls]
        for index1, item1 in enumerate(file_urls):
            url2_data_item = {
                "title": item["title"],
                "timestamp": item["timestamp"],
                "file_url": item1,
                "file_title": titles[index1]
            }
            if "指南" in url2_data_item["title"]:
                # pass
                self.url2_download_file(url2_data_item)
            else:
                logger.info('非指南数据，不需要')

    # 爬取url2的页面
    def crawlUrl2PageUrls(self, url):
        # 收集双重循环的url地址
        response = requests.get(url=url,
                                headers=headers,
                                proxies=requests_proxies)
        if response.status_code != 200:
            if response.status_code == 404:
                logger.info(f'{item["url"]}页面不存在')
            else:
                logger.info(f'{item["url"]}页面异常')
        # response.encoding = chardet.detect(response.content)['encoding']
        html_str = response.text
        logger.info(f'{url}提取完毕')
        html_result = etree.HTML(html_str)
        middle_url_titles = html_result.xpath(
            ""
        )
        # print(middle_url_titles)
        times = html_result.xpath(
            '')
        times = [re.sub(r'\xa0', '', item) for item in times]
        url_title_times = []
        for i in range(0, len(middle_url_titles), 2):
            url_title_times.append({
                "url": middle_url_titles[i],
                "title": middle_url_titles[i + 1]
            })
        # print(url_title_times)
        for index, item in enumerate(url_title_times):
            url_title_times[index]['timestamp'] = times[index]

            if int(url_title_times[index]['timestamp'].split('-')
                   [0]) + year_range >= current_year:

                # print(result)
                self.crawlUrl2Document(url_title_times[index])
            else:
                logger.info('信息过于陈旧，不进行获取')

    def crawlMainUrl2(self):
        response = requests.get(url=main_urls[1]['address'],
                                headers=headers,
                                proxies=requests_proxies)
        if response.status_code != 200:
            if response.status_code == 404:
                logger.info(f'{item["url"]}页面不存在')
            else:
                logger.info(f'{item["url"]}页面异常')
        result = response.text
        logger.info(f'{main_urls[1]["address"]}请求完成')
        # 先注释掉，写完所有代码后再取消注释
        urls = []
        html_result = etree.HTML(result)
        total_page = html_result.xpath(
            "/html/body/div[4]/div/div[3]/form/p/text()")
        # 获取总页数
        total_page = int(total_page[2].split('总记录数：')[1].strip())
        for num in range(total_page):
            urls.append(url2_base_url + str(num + 1))
        # crawlUrl2PageUrls(urls)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 提交任务到线程池，并获取 Future 对象
            futures = [
                executor.submit(self.crawlUrl2PageUrls, url) for url in urls
            ]
            # 遍历 Future 对象，获取结果


if __name__ == "__main__":
    # 爬一回只将多个数据调用一次数据库入库操作
    mymariadb_instance = MyMariadb()
    crawl_url1_instance = CrawlUrl1(mymariadb_instance)
    crawl_url2_instance = CrawlUrl2(mymariadb_instance)
    try:
        # 命令行参数如果不传递其他参数时，执行爬取url1的数据
        if len(sys.argv) == 1:
            crawl_url1_instance.crawlMainUrl1()
        else:
            # 命令行参数如果传递其他参数，并传递的第二个参数为url1时，执行爬取url1的数据
            if sys.argv[1] == "url1":
                crawl_url1_instance.crawlMainUrl1()
            # 命令行参数如果传递其他参数，并传递的第二个参数为url2时，执行爬取url2的数据
            if sys.argv[1] == "url2":
                crawl_url2_instance.crawlMainUrl2()
            # 命令行参数如果传递其他参数，并传递的第二个参数为all时，执行爬取url1与url2的数据
            if sys.argv[1] == "all":
                t1 = threading.Thread(target=crawl_url1_instance.crawlMainUrl1)
                t2 = threading.Thread(target=crawl_url2_instance.crawlMainUrl2)
                t1.start()
                t2.start()
                t1.join()
                t2.join()
        logger.info('本次数据更新完成')
    except Exception as e:
        print(e)
        logger.info(e)
