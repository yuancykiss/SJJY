import json
import re
from urllib.parse import quote
import pymysql

import requests


def header():
    """
    请求头
    :return:
    """
    header = {
        'Accept': '*/*',
        'Accept - Encoding': 'gzip, deflate',
        'Accept-Language0': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        # 'Content-Length': '109',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Host': 'search.jiayuan.com',
        'Origin': 'http://search.jiayuan.com',
        'Referer': 'http://search.jiayuan.com/v2/index.php?key=&sex=f&stc=1:51,2:18.24,23:1&sn=default&sv=1&p=1&pt=2105&ft=off&f=select&mt=d',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
    }
    return header


def insert_data(data):
    """
    将字典动态插入数据库
    :param data:
    :return:
    """
    table = 'jiayuan_copy'
    keys = ','.join(data.keys())
    values = ','.join(['%s'] * len(data))

    sql = 'INSERT INTO {table}({keys}) VALUES ({values})'.format(table=table, keys=keys, values=values)
    try:
        cursor.execute(sql, tuple(data.values()))
        db.commit()
    except pymysql.err.IntegrityError:
        db.rollback()


def get_page(url):
    """
    爬取数据
    :param url:
    :return: 用户信息（字典）
    """
    data = {'sex': 'f',
            # 51:四川省 18.24:18-24岁 f:女 23:1:有照片?
            'stc': '1:51,2:18.24,23:1',
            'p': 1,
            }

    for i in range(3):
        try:
            response = requests.post(url, data='sex='+data['sex']+
                                           '&key=&stc='+quote(data['stc'])+
                                           '&sn=default&sv=1&p='+ str(i+1) +
                                           '&f=select&listStyle=bigPhoto&pri_uid=184432552&jsversion=v5', headers=header)
        except ConnectionError:
            i -= 1
            print('连接超时，重新爬取第%d页数据' % (i+1))
            continue
        cont = response.text.split('##jiayser##')[1]
        cont1 = json.loads(cont)
        print('正在爬取第',i+1,'页数据')
        for item in cont1['userInfo']:
            # 剔除不需要的字段
            item.pop('sexValue')
            item.pop('userIcon')
            item.pop('online')
            item.pop('matchCondition')
            # 剔除抓取到的span标签，只取内容
            item['randTag'] = ' '.join(re.findall(r'<span>(.*?)</span>', item['randTag']))
            item['randListTag'] = ' '.join(re.findall(r'<span>(.*?)</span>', item['randListTag']))
            insert_data(item)
    db.close()


def get_conn():
    """
    获取游标
    :return: 游标，连接（用于提交数据和关闭数据库连接）
    """
    db = pymysql.connect(host='127.0.0.1', user='root', password='123456',
                         database='spiders', port=3306, charset='utf8')
    cursor = db.cursor()
    return (cursor, db)


if __name__ == '__main__':
    header = header()
    url = 'http://search.jiayuan.com/v2/search_v2.php'
    cursor, db = get_conn()
    get_page(url)
