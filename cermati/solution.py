from datetime import datetime
import aiohttp
import asyncio
import json
import re
import requests
from bs4 import BeautifulSoup
import polars as pl


async def fetch(session, url):
    async with session.get(url) as response:
        return await response.json()

async def fetch_all(session, urls):
    tasks = []
    for url in urls:
        tasks.append(asyncio.ensure_future(fetch(session, url)))
    responses = await asyncio.gather(*tasks)
    return responses

async def main(url):

    async with aiohttp.ClientSession() as session:
        print('fetching contents from main url...')
        start = datetime.now()
        response = await session.get(url)
        print('fetched in: ', datetime.now()-start)
        soup = BeautifulSoup(await response.text(), 'html.parser')
        script_tag = soup.find('script', {'id': 'initials', 'type': 'application/json'})
        initials = json.loads(script_tag.string)
        # get jobs from main url
        jobs = initials['smartRecruiterResult']['all']['content']
        jobs_df = pl.DataFrame(jobs)
        # create list for all the urls for jobs
        refs = jobs_df['ref'].to_list()
        # parallelize api calls to urls obtained
        print('fetching all job descriptions...')
        start = datetime.now()
        descriptions = await fetch_all(session, refs)
        print('fetched in: ', datetime.now()-start)
        # create dataframe from results
        descriptions_df = pl.DataFrame(descriptions)
    
    # transformation to extract fields from structs
    descriptions_df = descriptions_df.select(pl.col("name").alias('title'), pl.col("department").struct.field("label").alias('department'), pl.col("location").struct.field("city").alias('location'), pl.col("jobAd").struct.field("sections").struct.field("jobDescription").struct.field("text").alias('description'), pl.col("jobAd").struct.field("sections").struct.field("qualifications").struct.field("text").alias('qualification'), pl.col('creator').struct.field("name").alias('posted_by'))

    pattern = re.compile(r'<(?!\/(li)\b)[^<]+?>|&#xa0;')
    # transformation to clean up html tags and empty strings
    descriptions_df = descriptions_df.with_columns ([pl.col('description').apply(lambda x: pattern.sub('', x).split('</li>')).apply(lambda x: list(filter(lambda s: s and not s.isspace(), x))).alias("description"), pl.col('qualification').apply(lambda x: pattern.sub('', x).split('</li>')).apply(lambda x: list(filter(lambda s: s and not s.isspace(), x))).alias("qualification")])

    jobs_dict = {}
    # collect jobs per department
    print('collating data per department...')
    start = datetime.now()
    for row in descriptions_df.iter_rows():
        department = row[1]
        job = {
            'title': row[0],
            'location': row[2],
            'description': row[3],
            'qualification': row[4],
            'posted_by': row[5]
        }
        if department not in jobs_dict:
            jobs_dict[department] = []
        jobs_dict[department].append(job)
    # write to a file
    with open('solution.json', 'w') as f:
        json.dump(jobs_dict, f, indent=4)
    print('file generated in: ', datetime.now()-start)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main('https://www.cermati.com/karir'))
