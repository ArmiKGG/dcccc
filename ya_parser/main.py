import pickle
import time

from elasticsearch import Elasticsearch
import gc
import tensorflow_hub as hub
import tensorflow_text
from bs4 import BeautifulSoup
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.request import urlretrieve
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import os
from twocaptcha import TwoCaptcha
from elasticsearch import helpers

gc.enable()


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch(
        ["https://c-c9qbvn9fqt1e60l616u1.rw.mdb.yandexcloud.net:9200"],
        basic_auth=("admin", "tMwQHCgwPHWcyvQ9XXwwMc38"),
        verify_certs=False
    )
    if _es.ping():
        print("ES connected")
    else:
        print("Could not connect to ES!")
    return _es

def embed_text(title):
    module_url = 'https://tfhub.dev/google/universal-sentence-encoder-multilingual/3'
    embed = hub.load(module_url)
    embeddings = embed(title)
    return embeddings.numpy()


def get_all(es_, size=50):
    query_es = {
        "query": {
            "match_all": {}
        }
    }
    items = es_.search(index="cats_yandex_api", body=query_es, size=size)["hits"]["hits"]
    if items:
        items = [item["_source"] for item in items]
    return items


def check_exists_by_xpath(xpath):
    try:
        driver.find_element(By.XPATH, xpath)
    except NoSuchElementException:
        return False
    return True


def pass_captcha():
    captcha = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="root"]/div/div/form')))
    driver.get(captcha.get_attribute("action"))
    img = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="advanced-captcha-form"]/div/div[1]/img')))
    src = img.get_attribute('src')
    urlretrieve(src, "image.png")
    solved = solver.normal(r'./image.png')
    if solved.get("code"):
        print(solved['code'])
        input_fld = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="xuniq-0-1"]')))
        input_fld.send_keys(solved['code'])
        input_fld.send_keys(Keys.ENTER)


def prepare_driver():
    options = webdriver.FirefoxOptions()
    options.add_argument('--disable-gpu')
    options.add_argument(f'--host=https://market.yandex.ru/')
    _driver = webdriver.Remote(
        command_executor=f'http://51.250.75.50:4444/wd/hub', options=options)
    if os.path.exists('cookies.pkl'):
        _cookies = pickle.load(open("cookies.pkl", "rb"))
        for _cookie in _cookies:
            print(f'cookie added {_cookie}!')
            _driver.add_cookie(_cookie)
    return _driver


api_key = os.getenv('APIKEY_2CAPTCHA', '480b03b270dfb2725f9e6023f0554efc')

solver = TwoCaptcha(api_key)


def __scroll_down_page(speed=12):
    current_scroll_position, new_height = 0, 1
    while current_scroll_position <= new_height:
        current_scroll_position += speed
        driver.execute_script("window.scrollTo(0, {});".format(current_scroll_position))
        new_height = driver.execute_script("return document.body.scrollHeight")


def get_items(s: BeautifulSoup, main: str, sub_2: str, sub_1 = "no_sub"):
    articles = s.select("article._2vCnw.cia-vs.cia-cs")
    all_items = []
    if articles:
        for article in articles:
            try:
                a_tag = article.select_one("a._2f75n._24Q6d.cia-cs")
                text, href = a_tag.text, "https://market.yandex.ru" + a_tag.get("href")
                img = "https:" + article.select_one("img._2UO7K").get("src")
                try:
                    price = article.select_one("div._3NaXx._33ZFz._2m5MZ") \
                        .select_one("span").text
                except:
                    price = "0"
                specification = article.select("ul.fUyko._2LiqB li")
                all_specs = []
                for specific in specification:
                    initial_val = specific.text
                    if ":" in initial_val:
                        val_type = initial_val.split(":")[0]
                        val_val = initial_val.split(":")[1].strip()
                    else:
                        val_type, val_val = "", ""
                    all_specs.append({"initial": initial_val, "parsed_type": val_type, "parse_value": val_val})
                all_items.append({"title": text, "url": href, "price": price, "img": img, "specs": all_specs,
                                  "main": main, "sub_1": sub_1, "sub_2": sub_2,
                                  "_index": "items_yandex_agg"})
            except:
                pass
    return all_items


es = connect_elasticsearch()
driver = prepare_driver()
all_d = get_all(es)
while True:
    for ii, kk in enumerate(all_d):
        try:
            _main = kk["main"]
            for j in kk["subs"]:
                _sub_1 = j["title"]
                if j.get("subs"):
                    for k in j["subs"]:
                        _sub_2 = k["title"]
                        driver.get(k["url"] + f"&page={ii+1}")
                        if check_exists_by_xpath('//*[@id="root"]/div/div/form'):
                            pass_captcha()
                        __scroll_down_page()
                        soup = BeautifulSoup(driver.page_source, "lxml")
                        item_data = get_items(soup, main=_main, sub_1=_sub_1, sub_2=_sub_2)
                        vectors = [tt["title"] for tt in item_data]
                        vectors = embed_text(vectors)
                        for ind, item in enumerate(item_data):
                            item["title_vector"] = vectors[ind].tolist()
                        print(f"{_sub_2} - {len(item_data)}")
                        helpers.bulk(es, item_data)
                else:
                    driver.get(j["url"] + f"&page={ii+1}")
                    if check_exists_by_xpath('//*[@id="root"]/div/div/form'):
                        pass_captcha()
                    __scroll_down_page()
                    soup = BeautifulSoup(driver.page_source, "lxml")
                    item_data = get_items(soup, main=_main, sub_2=_sub_1)
                    vectors = [tt["title"] for tt in item_data]
                    vectors = embed_text(vectors)
                    for ind, item in enumerate(item_data):
                        item["title_vector"] = vectors[ind].tolist()
                    print(f"{_sub_1} - {len(item_data)}")
                    helpers.bulk(es, item_data)
            print(f"{ii} did bulk: ")
        except Exception as e:
            print(e)

