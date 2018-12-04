# asyncio爬虫、去重、入库
import asyncio
from urllib.parse import quote

import aiohttp
import aiomysql
from scrapy import Selector

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}
stopping = False
start_url = 'https://zhidao.baidu.com/search?word='
search_urls = []
title_urls = []
title_urls_set = set()


# sem = asyncio.Semaphore(4)

# 获取url内容
async def fetch(url, session):
    # async with sem:
    try:
        async with session.get(url) as resp:
            print("{}: {}".format(url,resp.status))
            if resp.status in [200, 201]:
                data = await resp.text(encoding='gb18030')
                return data
    except Exception as e:
        print('fetch error:',e)


# 2、抽取start_url,及添加title_url
async def extract_urls():
    async with aiohttp.ClientSession() as session:
        while True:
            if len(search_urls) == 0:
                await asyncio.sleep(0.01)
                continue

            search_url = search_urls.pop()
            question,url = search_url.split('##',1)
            html = await fetch(url, session)
            try:
                selector = Selector(text=html)
                urls = selector.xpath('//dd[@alog-group]/span[3]/a/@href').extract()
                for url in urls:
                    if url not in title_urls_set:
                        title_urls.append(question + '##' + url)
            except Exception as e:
                print('extract error:',e,html)


# 4、获取title_url并抽取相似問題入库
async def article_handler(url, session, pool):
    question,url = url.split('##',1)
    html = await fetch(url, session)
    question_selector = Selector(text=html)
    similar = question_selector.xpath('//h1[@accuse="qTitle"]/span/text()').extract()[0]

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            insert_sql = "insert into similar(question,similar) values(%s,%s)"
            await cur.execute(insert_sql,(question,similar))


# 3、title_urls池中读取url
async def consumer(pool):
    async with aiohttp.ClientSession() as session:
        while not stopping:
            if len(title_urls) == 0:
                await asyncio.sleep(0.01)
                continue

            url = title_urls.pop()
            print("start get url: {}".format(url))
            if url.find('http://zhidao.baidu.com')>=0:
                if url not in title_urls_set:
                    title_urls_set.add(url)
                    asyncio.ensure_future(article_handler(url, session, pool))

# 1、从数据库中取出所有需要查询问题
async def fetch_questions(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute("SELECT question FROM rs_question")
                return [row[0] for row in await cur.fetchall() if row]
            except Exception as e:
                print('error',e)


async def main(loop):
    # 等待mysql连接建立好
    pool = await aiomysql.create_pool(host='192.168.179.215', port=3306,
                                      user='xxxx', password='xxxxxx',
                                      db='test', loop=loop,
                                      charset="utf8", autocommit=True)

    questions = await fetch_questions(pool)

    if len(questions) == 0:
        await asyncio.sleep(0.5)
    for question in questions[:]:
        for i in range(0,100,10):
            url = start_url + quote(question.encode("utf8")) + '&pn=' + str(i)
            search_urls.append(question+'##'+url)

    asyncio.ensure_future(extract_urls())
    asyncio.ensure_future(consumer(pool))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(main(loop))
    loop.run_forever()
