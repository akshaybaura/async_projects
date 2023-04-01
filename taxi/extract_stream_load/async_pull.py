import warnings
import asyncio
import aiohttp
import backoff
import requests
from datetime import datetime
import re
import time
import pandas as pd
import duckdb
import random
import math
from taxi_producer import TaxiProducer


# async def fetch_data(url, session):
#     headers = {'Cache-Control': 'no-cache'}
#     async with session.get(url, headers=headers) as response:
#         data = await response.json()
#         return data

# async def fetch_all_data(urls):
#     ss = pd.DataFrame()
#     timeout = aiohttp.ClientTimeout(total=600)
#     async with aiohttp.ClientSession(timeout=timeout) as session:
#         tasks = []
#         results = []
#         for url in urls:
#             task = asyncio.create_task(fetch_data(url, session))
#             tasks.append(task)
#         for json_obj in await asyncio.gather(*tasks):
#             try:
#                 ss.append(json_obj)
#             except:
#                 print(json_obj)
#             else:
#                 print(ss.head())
#             # results.extend(json_obj)
#         return ss

# async def main():
#     warnings.filterwarnings("ignore", category=FutureWarning)
#     limit = 1
#     total = 1
#     print(limit, total)
#     base_url = f'https://data.cityofchicago.org/resource/wrvz-psew.json?$limit={limit}&$offset='#$$app_token=9OBjTz7cstV0QIa0gQg3aNrEs&
#     urls = [base_url + str(i) for i in range(0, total, limit)]
#     print(urls)
#     ss = await fetch_all_data(urls)
#     # ss = pd.DataFrame(data)
#     # process the data as per your requirement
#     con.sql('create or replace table dd as select * from ss')

# if __name__ == '__main__':
#     con = duckdb.connect('dev')
#     start = datetime.now()
#     asyncio.run(main())
#     print(datetime.now()-start)


async def fetch_data(session, url, params):
    async with session.get(url, params = params) as response:
        return await response.json()

@backoff.on_exception(backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=1)
async def fetch_page(session, page_size, page_num):
    params = {
        '$limit': page_size,
        '$offset': page_num * page_size,
        '$$app_token': '9OBjTz7cstV0QIa0gQg3aNrEs'
    }
    url = 'https://data.cityofchicago.org/resource/wrvz-psew.json'
    return await fetch_data(session, url=url, params=params)

def produce_async(sublist):
    count = 0
    try:
        for item in sublist:
            item['extracted_ts'] = time.time()
            p.send_record(item)        
    except TypeError as typeerr:
        print(f'Non blocking error encountered producing record to kafka. Error: {typeerr}')
        print(f'record: {item}')
    except Exception as e:
        print(f'Blocking error encountered producing record to kafka. Error: {e}')
        print(f'record: {item}')
        raise e
    else: 
        count += 1
    return count

async def fetch_all_data(total):
    timeout = aiohttp.ClientTimeout(total=6000)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        page_size = 10000
        page_num = 0
        count = 0 
        while count < total:
            print('gathering data for 5 pages...')
            page_data = await asyncio.gather(*(fetch_page(session, page_size, page_num + i) for i in range(5)))
            if not any(page_data):
                break
            page_num += 5
            for sublist in page_data:
                print('streaming data...')
                count += produce_async(sublist)
        return count

async def main():
    total = 500000 # replace with the total number of records you want to retrieve
    count = await fetch_all_data(total)
    print(f'Fetched {count} records')
    

if __name__ == '__main__':
    p = TaxiProducer()  
    start = datetime.now()
    print('async call initiated...')
    asyncio.run(main())
    print('async call finished and data streamed...', datetime.now()-start)






