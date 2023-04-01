import warnings
import asyncio
import aiohttp
import requests
from datetime import datetime
import re
import time
from patient_producer import PatientProducer

async def fetch_json(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()

async def main_f(url_list):
    full_url_list = []
    tasks = []
    for url in url_list:
        tasks.append(asyncio.create_task(fetch_json(url)))
    for json_obj in await asyncio.gather(*tasks):
        for item in json_obj['items']:
            media_link = item.get("mediaLink")
            nm = item.get('name')
            if media_link and not nm.startswith('index'):
                full_url_list.append(media_link)
    return full_url_list

async def fetch_text(session, url):
    async with session.get(url) as response:
        return await response.text()

async def get_data(full_url_list):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in full_url_list:
            tasks.append(fetch_text(session, url))
        results = await asyncio.gather(*tasks)
        
        record_count = 0
        for txt in results:
            try:
                #regex to parse data
                pattern = r"ID:\s*(?P<ID>\s*[^\n]+)\nNAME:\s*(?P<NAME>\s*[^\n]+)\nAGE:\s*(?P<AGE>\s*[^\n]+)\nINSTITUTION:\s*(?P<INSTITUTION>\s*[^\n]+)\nACTIVITY:\s*(?P<ACTIVITY>\s*[^\n]+)\nCOMMENT:\s*(?P<COMMENT>\s*[^$]+)"
                match = re.match(pattern, txt)
                ido, name, age, institution, activity, comment = match.groups()
                # prelim cleanup
                ido = ido.strip()
                age = re.search(r'\d+', age)
                age = int(age.group()) if age else -1
                institution = institution.strip()
                activity = activity.strip()
                comment = comment.strip()
            except Exception as e:
                print(txt)
                raise e
            #produce records to the topic
            p.send_record({'id':ido, 'age':age, 'institution':institution, 'activity':activity, 'comment':comment, 'extracted_ts': time.time()})
            record_count += 1
        return record_count

async def main():
    warnings.filterwarnings("ignore", category=FutureWarning)
    url = 'https://storage.googleapis.com/storage/v1/b/aip-data-engineer-assessment-files/o'
    url_list = [url]
    token = requests.get(url).json().get('nextPageToken')
    print('paginating')
    start = datetime.now()
    while token:
        # this part paginates through the pages to get the next page tokens
        # and makes a list of the urls
        new_url = url + f'?pageToken={token}'
        token = requests.get(new_url).json().get('nextPageToken')
        url_list.append(new_url)

    print('sequential pagination completed in: ',datetime.now()-start)
    print('fetching medialinks')
    start = datetime.now()
    # this does an async pull on the url list for pages to get the medialinks
    ss = await main_f(url_list)
    print('medialinks fetched in: ',datetime.now()-start)

    print('fetching data')
    start = datetime.now()
    # this does an async pull on the medialinks, parses the data and
    # streams it to a kafka topic
    records_sent = await get_data(ss)
    print('data pull and stream completed in: ',datetime.now()-start)
    print('records parsed and streamed: ', records_sent)

if __name__ == '__main__':
    # kafka topic Producer instance
    p = PatientProducer()   

    asyncio.run(main())



