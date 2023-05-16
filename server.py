import asyncio
import websockets
import requests
import json
import mysql.connector
import pathlib
from unidecode import unidecode
from itertools import groupby
from bs4 import BeautifulSoup
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import threading
import datetime
from urllib.parse import urlparse
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem

config_file = pathlib.Path(__file__).with_name("config.json")
with open(config_file) as json_file:
    data = json.load(json_file)
    DB_HOST = data['DB_HOST']
    DB_NAME = data['DB_NAME']
    DB_USER = data['DB_USER']
    DB_PASS = data['DB_PASS']
    PORT = data['PORT']
    SERVER = data['SERVER']
    SEO_RANK_API_Key = data['SEO_RANK_API_Key']


async def remove_duplicates():
    con = None
    cursor = None
    try:
        con = mysql.connector.connect(user=DB_USER,
                                      password=DB_PASS,
                                      host=DB_HOST,
                                      database=DB_NAME)
        cursor = con.cursor()
        sql = """DELETE t1 FROM search_result t1
        INNER JOIN (
            SELECT search_history_id, page_url, MIN(id) min_id
            FROM search_result
            GROUP BY search_history_id, page_url
            HAVING COUNT(*) > 1
        ) t2
        ON t1.search_history_id = t2.search_history_id
        AND t1.page_url = t2.page_url
        AND t1.id <> t2.min_id"""
        cursor.execute(sql)
        con.commit()
    except Exception as e:
        print(f'Error while removing duplicates: {e}')
    finally:
        cursor.close()
        con.close()


def getAPIDATA():
    api_data = [0, '', '', 0, '', 0, 0]
    try:
        sql = f"SELECT * FROM `api_data` ORDER BY `hits` ASC LIMIT 1"
        con = mysql.connector.connect(user=DB_USER,
                                      password=DB_PASS,
                                      host=DB_HOST,
                                      database=DB_NAME)
        cursor = con.cursor()
        cursor.execute(sql)
        api_data = list(cursor.fetchone())
        print(
            f"Success to select an api_key. ID: {api_data[0]}")
    except Exception as e:
        print(f'Failed to select an api_key.', e)
    finally:
        cursor.close()
        con.close()

    return api_data


def updateHistoryChanges(search_history_id):
    try:
        dt_now = datetime.datetime.now()

        sql = f"UPDATE `search_history` SET `last_modified`='{dt_now.strftime('%Y-%m-%d %H:%M:%S')}' WHERE `id`='{search_history_id}'"
        con = mysql.connector.connect(user=DB_USER,
                                      password=DB_PASS,
                                      host=DB_HOST,
                                      database=DB_NAME)
        cursor = con.cursor()
        cursor.execute(sql)
        con.commit()
    except Exception as e:
        print(f'UPDATE search_history failed.', e)
    finally:
        cursor.close()
        con.close()


def isExisting(search_history_id, domain, url):
    existing_urls = []
    try:
        con = mysql.connector.connect(user=DB_USER,
                                      password=DB_PASS,
                                      host=DB_HOST,
                                      database=DB_NAME)
        cursor = con.cursor()
        sql = f"SELECT `page_url` FROM `search_result` WHERE `search_history_id`='{search_history_id}' AND `domain` LIKE '{domain}'"
        cursor.execute(sql)
        existing_urls = [item[0] for item in cursor.fetchall()]
    except Exception as ex:
        print('This domain has already been checked, skipping', ex)
    finally:
        cursor.close()
        con.close()

    print('Existing urls', existing_urls)

    if url in existing_urls:
        return True
    return False


def increaseHits(api_id):
    try:
        sql = f"UPDATE `api_data` SET `hits` = `hits` + 1 WHERE `id` = {api_id}"
        con = mysql.connector.connect(user=DB_USER,
                                      password=DB_PASS,
                                      host=DB_HOST,
                                      database=DB_NAME)
        cursor = con.cursor()
        cursor.execute(sql)
        con.commit()
        print(
            f"Success to update a hit. ID: {api_id}")
    except Exception as e:
        print(f'Failed to update a hit.', e)
    finally:
        cursor.close()
        con.close()


def getSeoRankChecker(your_website_url):
    try:
        # Add https:// before your_website_url and a trailing slash at the end
        your_website_url = your_website_url.strip(
            '/')  # remove existing trailing slashes
        your_website_url = "https://" + your_website_url + "/"

        while True:
            seo_url = "https://seo-rank-checker.p.rapidapi.com/check"
            seo_querystring = {"metric": "mixed"}
            seo_payload = {"url": your_website_url}
            seo_headers = {
                "X-RapidAPI-Key": SEO_RANK_API_Key,
                "content-type": "application/json",
                "X-RapidAPI-Host": "seo-rank-checker.p.rapidapi.com"
            }
            seo_response = requests.request(
                "POST", seo_url, json=seo_payload, headers=seo_headers, params=seo_querystring)
            seo_json_object = json.loads(seo_response.text)

            # Check if hostname is a digit, otherwise try again with "www." added to the URL
            if not str(seo_json_object["result"]["semrush"]["links"]["hostname"]).isdigit():
                your_website_url = your_website_url.replace(
                    "https://", "https://www.")
                your_website_url = your_website_url.rstrip(
                    '/')  # remove any trailing slashes
                continue

            return seo_json_object
    except Exception as e:
        print('***** getSeoRankChecker *****')
        print(f'Error in {your_website_url}', e)
        print('**********')


def getTitleOfPage(page_url):

    # List of file extensions to check for
    file_extensions = (".pdf", ".doc", ".docx", ".PDF", ".DOC", ".DOCX")

    # Get random user agent
    software_names = [SoftwareName.CHROME.value]
    operating_systems = [OperatingSystem.WINDOWS.value,
                         OperatingSystem.LINUX.value]
    user_agent_rotator = UserAgent(
        software_names=software_names, operating_systems=operating_systems, limit=100)

    # Try getting title with requests + proxy + random user agent
    def simple_method(page_url, proxy=None):
        try:
            headers = {"User-Agent": user_agent_rotator.get_random_user_agent()}
            response = requests.get(
                page_url, proxies=proxy, headers=headers, timeout=10)
            soup = BeautifulSoup(response.content, "html.parser")
            title_tag = soup.find("title")
            if title_tag:
                title = title_tag.text
            else:
                title = None
        except Exception as e:
            title = None
        return title

    # If blocked, use APILayer meta tags API
    def get_meta_tags_using_api(page_url):
        api_url = f"https://api.apilayer.com/meta_tags?url={page_url}&proxy=true"
        headers = {
            "apikey": "fbBYQNnJgQsG7ES2dFQ7FGw53MWD3vyU"
        }
        try:
            response = requests.get(api_url, headers=headers, timeout=10)
            status_code = response.status_code
            result = response.text
            return status_code, result
        except Exception as e:
            return None, None

    # Grab title function
    PROXY = {"http": "http://193.164.199.29:3128"}

    def is_title_blocked(title, blocked_titles):
        return any(partial_title in title for partial_title in blocked_titles)

    # Grab title function
    def grabtitle():

        if page_url.endswith(file_extensions):
            return "[Unreachable]"

        title = simple_method(page_url, PROXY)

        blocked_titles = ["Robot Challenge Screen", "Access Denied", "Just a moment", "403 Forbidden", "503 Service Temporarily Unavailable", "Security check", "Not Acceptable!", "Temporary Page", "Sucuri WebSite Firewall - Access Denied", "502 Bad Gateway", "520", "StackPath"]

        if is_title_blocked(title, blocked_titles):
            status_code, result = get_meta_tags_using_api(page_url)

            if status_code is not None and result is not None:
                result_json = json.loads(result)  # Parse the JSON response
                title_from_api = result_json.get(
                    'title', None)  # Get the "title" value

                if title_from_api is not None:
                    return title_from_api
                else:
                    title = simple_method(page_url)
                    return title if title is not None else "[Unreachable]"
            else:
                title = simple_method(page_url)
                return title if title is not None else "[Unreachable]"
        else:
            return title if title is not None else "[Unreachable]"

    page_title = grabtitle()
    page_title = unidecode(page_title)
    page_title = page_title.replace("’", "'").replace("‘", "'").replace("&#039;", "'")
    page_title = re.sub("[^a-zA-Z0-9-.' ]+", " ", page_title)
    page_title = page_title.split('|')[0]
    page_title = page_title.split(' - ')[0]
    page_title = page_title.strip()
    if page_title == '':
        page_title = '[Unreachable]'
    else:
        url_parse = urlparse(page_url)
        if url_parse.path == '' or url_parse.path == '/':
            page_title = '[Homepage] ' + page_title

    print(f"page_url: {page_url}, page_title: {page_title}")

    return page_title


def data_processor(raw_message):
    print(f'Received data as: {raw_message}')
    json_object = json.loads(raw_message)
    return json_object


# A set of connected ws clients
connected = set()


def between_callback_thread(json_message, websocket):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(broadcast_messages(json_message, websocket))
    loop.close()


async def handler(websocket, path):
    remote_ip = websocket.remote_address[0]
    print(f'A client just connected at IP: {remote_ip}')
    connected.add(websocket)
    try:
        async for raw_message in websocket:
            print("Received message from client: " + raw_message)
            for conn in connected:
                if conn == websocket:
                    json_message = json.loads(raw_message)
                    if 'email' in json_message and 'loggedin' in json_message:
                        try:
                            sql = f"SELECT * FROM `user` WHERE `email` LIKE '{json_message['email']}'"
                            con = mysql.connector.connect(user=DB_USER,
                                                          password=DB_PASS,
                                                          host=DB_HOST,
                                                          database=DB_NAME)
                            cursor = con.cursor()
                            cursor.execute(sql)
                            users = cursor.fetchall()
                            if len(users) > 0 and json_message['loggedin'] == 1:
                                print("Authentication Success")
                                await websocket.send('pong')
                            else:
                                await websocket.close(1011, "Authentication failed")
                        except Exception as e:
                            print(f'Authentication failed.', e)
                            await websocket.close(1011, "Authentication failed")
                        finally:
                            cursor.close()
                            con.close()
                        continue
                    elif 'filtered_domain' in raw_message:
                        '''
                        # Thread sending #
                        '''
                        server = threading.Thread(target=between_callback_thread, args=(
                            json_message, websocket), daemon=True)
                        server.start()

                        print(f'success boradcast_messages to {remote_ip}')

    except websockets.exceptions.ConnectionClosed as e:
        print(f"{remote_ip} client just disconnected")
    finally:
        connected.remove(websocket)


async def broadcast(domain_json, search_history_id, your_website_url_da, websocket=None):
    domain = domain_json['domain']
    traffic_sum = domain_json['traffic_sum']
    stat_total_keywords = domain_json['total_keywords']

    api_data = getAPIDATA()
    SERANKING_TOKEN = api_data[1]
    proxy_ip = api_data[4]
    proxy_port = api_data[5]

    proxies = {
        'http': f'{proxy_ip}:{proxy_port}',
        'https': f'{proxy_ip}:{proxy_port}',
    }

    url = f"https://api4.seranking.com/research/us/keywords/?domain={domain}&type=organic&group=url&source=us&currency=USD&base_domain={domain}&sort=traffic_percent&sort_order=desc&limit=1000&offset=0"
    payload = {}
    headers = {
        'Authorization': f'Token {SERANKING_TOKEN}'
    }
    response = requests.request(
        "GET", url, headers=headers, data=payload, proxies=proxies)

    increaseHits(api_data[0])

    json_object = json.loads(response.text)

    json_object.sort(key=lambda content: content['url'])
    groups = groupby(json_object, lambda content: content['url'])

    stat_seo_viability = 0
    stat_page_authority = 0

    stat_domain_authority = 0
    stat_rank = 0
    stat_links = 0

    seo_json_object = getSeoRankChecker(domain)
    if 'success' in seo_json_object and seo_json_object['success']:
        stat_rank = seo_json_object['result']['semrush']['rank']
        stat_links = seo_json_object['result']['semrush']['links']['domain']
        stat_domain_authority = seo_json_object['result']['authority']['domain']

        if str(stat_rank).isdigit() == False:
            stat_rank = 0
        if str(stat_links).isdigit() == False:
            stat_links = 0
        if str(stat_domain_authority).isdigit() == False:
            stat_domain_authority = 0

    json_result = []
    for page_url, contents in groups:
        is_existing = isExisting(search_history_id, domain, page_url)
        if is_existing:
            continue
        stat_traffic_percentage = round(sum(content['traffic_percent']
                                            for content in contents), 2)
        json_result.append({
            'search_history_id': search_history_id,
            'page_title': '',
            'page_url': page_url,
            'domain': domain,
            'stat_seo_viability': stat_seo_viability,
            'stat_traffic_percentage': stat_traffic_percentage,
            'stat_total_keywords': stat_total_keywords,
            'stat_domain_authority': stat_domain_authority,
            'stat_page_authority': stat_page_authority,
            'stat_rank': stat_rank,
            'stat_links': stat_links,
        })

    json_result.sort(key=lambda content: -content['stat_traffic_percentage'])

    records = []
    record_cnt = 0
    batch_size = 3
    batch_records = []

    for content in json_result[:100]:
        try:
            con = mysql.connector.connect(user=DB_USER,
                  password=DB_PASS,
                  host=DB_HOST,
                  database=DB_NAME)
            cursor = con.cursor()

            record_cnt += 1
            search_history_id = content['search_history_id']
            page_url = content['page_url']
            domain = content['domain']
            stat_seo_viability = content['stat_seo_viability']
            stat_traffic_percentage = content['stat_traffic_percentage']
            stat_total_keywords = content['stat_total_keywords']
            stat_domain_authority = content['stat_domain_authority']
            stat_page_authority = content['stat_page_authority']
            stat_rank = content['stat_rank']
            stat_links = content['stat_links']

            if record_cnt <= 3:
                stat_seo_viability = 3
            if record_cnt <= 15:
                stat_seo_viability += 1
            if your_website_url_da > stat_domain_authority:
                stat_seo_viability += 1
            if your_website_url_da - 10 < stat_domain_authority:
                stat_seo_viability += 1
            if your_website_url_da - 20 < stat_domain_authority:
                stat_seo_viability += 1
            if stat_traffic_percentage > traffic_sum * 5 / 100:
                stat_seo_viability += 1
            if stat_traffic_percentage > traffic_sum * 20 / 100:
                stat_seo_viability += 1
            if stat_traffic_percentage > traffic_sum * 40 / 100:
                stat_seo_viability += 1

            # get page title
            page_title = getTitleOfPage(page_url)
            '''
            Insert search_result for page_url into database
            '''
            val = (
                search_history_id, page_title, page_url, domain, stat_seo_viability, stat_traffic_percentage, stat_total_keywords, stat_domain_authority, stat_page_authority, stat_rank, stat_links
            )

            sql = """
            INSERT INTO `search_result` 
            (search_history_id, page_title, page_url, domain, stat_seo_viability, 
            stat_traffic_percentage, stat_total_keywords, stat_domain_authority, 
            stat_page_authority, stat_rank, stat_links) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, val)
    
            lastrowid = cursor.lastrowid
            if lastrowid < 1:
                continue
    
            updateHistoryChanges(search_history_id)
    
            batch_records.append({
                'id': lastrowid,
                'search_history_id': search_history_id,
                'page_title': page_title,
                'page_url': page_url,
                'domain': domain,
                'stat_seo_viability': stat_seo_viability,
                'stat_traffic_percentage': stat_traffic_percentage,
                'stat_total_keywords': stat_total_keywords,
                'stat_domain_authority': stat_domain_authority,
                'stat_page_authority': stat_page_authority,
                'stat_rank': stat_rank,
                'stat_links': stat_links,
            })
    
            if record_cnt % batch_size == 0:
                con.commit()
    
                if websocket is not None:
                    try:
                        await websocket.send(json.dumps(batch_records))
                    except Exception as ex:
                        print('Exception in Websocket send', ex)
    
                print(f'***** Success to send {record_cnt} records *****')
                batch_records = []
    
        except Exception as ex:
            print('Exception in Content data within 100', ex)
    
    if batch_records:
        con.commit()
        if websocket is not None:
            try:
                await websocket.send(json.dumps(batch_records))
            except Exception as ex:
                print('Exception in Websocket send', ex)
    
        print(f'***** Success to send {record_cnt} records *****')
    
    # Don't forget to close the cursor and connection at the end
    cursor.close()
    con.close()


async def broadcast_messages(json_object, websocket):
    search_history_id = json_object['search_history_id']
    filtered_domain = json_object['filtered_domain']
    your_website_url = json_object['your_website_url']
    domains = json.dumps(filtered_domain)

    try:
        sql = f"UPDATE `search_history` SET `domains` = '{domains}' WHERE `id` = {search_history_id}"
        con = mysql.connector.connect(user=DB_USER,
                                      password=DB_PASS,
                                      host=DB_HOST,
                                      database=DB_NAME)
        cursor = con.cursor()
        cursor.execute(sql)
        con.commit()
        print(
            f"search_history domains update success. ID: {search_history_id}")
    except Exception as e:
        print(f'search_history domains update failed.', e)
    finally:
        cursor.close()
        con.close()

    seo_json_object = getSeoRankChecker(your_website_url)
    your_website_url_da = 0

    if 'success' in seo_json_object and seo_json_object['success']:
        your_website_url_da = seo_json_object['result']['authority']['domain']

    for domain_json in filtered_domain:
        try:
            await broadcast(domain_json, search_history_id, your_website_url_da, websocket)
        except Exception as e:
            print(f'There is an issue to broadcast in {your_website_url}', e)
    try:
        sql = f"UPDATE `search_history` SET `status`='COMPLETED' WHERE `id`={search_history_id}"
        con = mysql.connector.connect(user=DB_USER,
                                      password=DB_PASS,
                                      host=DB_HOST,
                                      database=DB_NAME)
        cursor = con.cursor()
        cursor.execute(sql)
        con.commit()
        print(
            f"search_history status updated as 'COMPLETED', ID: {search_history_id}")
    except Exception as e:
        print(f'search_history status updated failed.', e)
    finally:
        cursor.close()
        con.close()
    print('***** COMPLETED *****')
    await websocket.send('COMPLETED')


async def do_stuff_periodically(interval):
    while True:
        await asyncio.gather(
            asyncio.sleep(interval),
            search_history_checker(),
            remove_duplicates()
        )
    

def cron_search_progress_checker():
    print("***** cron_search_progress_checker starting...*****")
    asyncio.run(do_stuff_periodically(60*1))


async def search_history_checker():
    print('**** create search_history_checker...')

    try:
        dt_30mins_ago = datetime.datetime.now() - datetime.timedelta(minutes=1)
        con = mysql.connector.connect(user=DB_USER,
                                      password=DB_PASS,
                                      host=DB_HOST,
                                      database=DB_NAME)
        cursor = con.cursor()
        sql = f"SELECT * FROM `search_history` WHERE `status` LIKE 'PROGRESS' AND `domains` IS NOT NULL AND `last_modified` < '{dt_30mins_ago.strftime('%Y-%m-%d %H:%M:%S')}' ORDER By `last_modified` DESC LIMIT 1"
        cursor.execute(sql)
        search_history_rows = cursor.fetchall()
        cursor.close()
        con.close()

        for row in search_history_rows:
            
            id = row[0]
            your_website_url = row[6]
            
            try:
                filtered_domain = json.loads(row[11])
            except json.JSONDecodeError:
                print(f"Failed to decode JSON for row: {row}")
                continue
                
            if row[11] == '':
                continue
            filtered_domain = json.loads(row[11])
            if filtered_domain is None:
                continue

            seo_json_object = getSeoRankChecker(your_website_url)
            your_website_url_da = 0

            if 'success' in seo_json_object and seo_json_object['success']:
                your_website_url_da = seo_json_object['result']['authority']['domain']

            try:
                con = mysql.connector.connect(user=DB_USER,
                                              password=DB_PASS,
                                              host=DB_HOST,
                                              database=DB_NAME)
                cursor = con.cursor()
                sql = f"SELECT `domain` FROM `search_result` WHERE `search_history_id`={id} GROUP BY `domain`"
                cursor.execute(sql)
                domains = [item[0] for item in cursor.fetchall()]
                print(f'existing domains: {domains}')
                print(f'{sql} Success')
            except Exception as e:
                print(f'{sql} Failed', e)
            finally:
                cursor.close()
                con.close()

            for domain_json in filtered_domain:
                domain = domain_json['domain']
                if domain in domains:
                    continue
                try:
                    await broadcast(domain_json, id, your_website_url_da)
                except Exception as e:
                    print(
                        f'There is an issue to broadcast in search_history_checker')

            try:
                con = mysql.connector.connect(user=DB_USER,
                                              password=DB_PASS,
                                              host=DB_HOST,
                                              database=DB_NAME)
                cursor = con.cursor()
                sql = f"UPDATE `search_history` SET `status`='COMPLETED' WHERE `id`={id}"
                cursor.execute(sql)
                con.commit()
                print(
                    f"search_history status updated as 'COMPLETED', ID: {id}")
            except Exception as e:
                print(f'search_history status updated failed.', e)
            finally:
                cursor.close()
                con.close()
            print('***** COMPLETED *****')
    except Exception as e:
        print(f'search_history PROGRESS query failed.', e)

    print('**** search_history_checker end...')

if __name__ == "__main__":
    api_data = getAPIDATA()
    print(api_data)
    print(api_data[6])
    _cron_thread = threading.Thread(target=cron_search_progress_checker)
    _cron_thread.start()

    print('socket is starting...')
    start_server = websockets.serve(handler, SERVER, PORT, ssl=None)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()
