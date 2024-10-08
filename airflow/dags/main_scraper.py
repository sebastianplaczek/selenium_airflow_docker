import bs4
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup
import pdb
import json
import os
import yaml

import cProfile
import pstats


import pandas as pd
import numpy as np
import requests
import sys
from datetime import datetime
import timeit


from importlib.machinery import SourceFileLoader

pwd = os.path.dirname(os.path.realpath(__file__)) + "/models.py"
models = SourceFileLoader("models", pwd).load_module()

from functools import wraps

from models import (
    Session,
    Offers,
    OtodomWebsite,
    ScrapInfo,
    OffersLoc,
    Runtime,
    ErrorLogs,
    CeleryTasks,
)


WEBS = {
    "dom_pierwotny": "https://www.otodom.pl/pl/wyniki/sprzedaz/dom%2Crynek-pierwotny/cala-polska?ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&viewType=listing&limit=72",
    "mieszkanie_pierwotny": "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie%2Crynek-pierwotny/cala-polska?limit=72&ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&viewType=listing",
    "dom_wtorny": "https://www.otodom.pl/pl/wyniki/sprzedaz/dom,rynek-wtorny/cala-polska?limit=72&ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&viewType=listing",
    "mieszkanie_wtorny": "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie%2Crynek-wtorny/cala-polska?ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&viewType=listing&limit=72",
    "dzialki": "https://www.otodom.pl/pl/wyniki/sprzedaz/dzialka/cala-polska?ownerTypeSingleSelect=ALL&viewType=listing&limit=72",
}


class WebDriverManager():

    def __init__(self,system):
        self.set_options(system)

    def set_options(self,system):
        self.firefox_options = Options()
        self.firefox_options.add_argument("--headless")
        self.firefox_options.add_argument("--no-sandbox")
        self.firefox_options.add_argument("--disable-gpu")
        self.firefox_options.add_argument("--incognito")
        self.firefox_options.add_argument("--window-size=1600,900")
        self.firefox_options.add_argument("--disable-dev-shm-usage")

        if system == "linux":
            self.service = FirefoxService(executable_path="/usr/local/bin/geckodriver")
        elif system == "windows":
            firefox_binary_path = "C:\\Program Files\\Mozilla Firefox\\firefox.exe"
            self.service = FirefoxService(
                executable_path="C:\projects\small_scrapper\geckodriver\geckodriver.exe"
            )
            self.firefox_options.binary_location = firefox_binary_path

        else:
            raise ValueError(f"Wrong system type {system}")
    
    def init_driver(self):
        self.driver = webdriver.Firefox(options=self.firefox_options, service=self.service)

    def close_driver(self):
        if self.driver:
            self.driver.quit()
            self.driver.close()

class DatabaseManager:

    def __init(self,enabled = True):
        self.enabled = enabled
        self.read_conf()

    def requires_enabled(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self.enabled:
                return
            return func(self, *args, **kwargs)
        return wrapper

    @requires_enabled
    def read_conf(self):
        with open("conf\conf_db.yaml", "r") as file:
            self.conf = yaml.safe_load(file)

        db_url = f'mysql+pymysql://{conf["username"]}:{conf["password"]}@{conf["database_ip"]}/{conf["database_name"]}'

    @requires_enabled
    def create_session(self):
        db_url = f'mysql+pymysql://{self.conf["username"]}:{self.conf["password"]}@{self.conf["database_ip"]}/{self.conf["database_name"]}'

        engine = create_engine(
            db_url,
            pool_size=10,  # Domyślnie 5
            max_overflow=20,  # Domyślnie 10
            pool_timeout=30,  # Domyślnie 30 sekund
            pool_recycle=1800,  # Recykluj połączenia co 30 minut
        )
        self.session = sessionmaker(bind=engine)

    @requires_enabled
    def end_session(self):
        self.session.close()

    @requires_enabled
    def commit(self):
        self.session.commit()

    @requires_enabled
    def add(self,object):
        self.session.add(object)


class MainScraper:

    def __init__(self,system,save_to_db=True):    
        self.web_driver_manager = FirefoxWebDriverManager(system=system)
        self.database_manager = DatabaseManager(enabled=save_to_db)
        self.set_conf_path(system)
    
    def set_conf_path(self,system):
        if system=="windows":
            self.conf_path = "conf\"
        elif system== "linux":
            self.conf_path = "conf/"
        else:
            raise ValueError(f"Wrong system type {system}")
    
    def parse_web(self,website)
        self.web_driver_manager.init_driver()
        self.web_driver_manager.driver.get(website)
        html = self.web_driver_manager.driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        self.web_driver_manager.close_driver()
        return soup
    
    #parsowanie i logika dla paczki stron


    def scrap_offers(self, type, page_num):
        website = WEBS[type] + f"&page={page_num}"

        soup = self.parse_web(website)

        offers = soup.find_all("div", {"class": ["css-13gthep eeungyz2"]})

        n_offers = len(offers)

        session = Session()

        self.elapsed_time = 0
        for position, offer in enumerate(offers):
            # try:
            (
                link,
                price,
                address,
                title,
                size,
                floor,
                rooms,
                seller,
                seller_type,
                bumped,
            ) = (None, None, None, None, None, None, None, None, None, None)
            try:
                link_element = offer.find("a", {"class": ["css-16vl3c1 e17g0c820"]})
                link = "otodom.pl" + link_element["href"]
            except Exception as e:
                error = ErrorLogs(exception=e)
                session.add(error)

            try:
                price_element = offer.find("div", {"class": ["css-fdwt8z escb8gg0"]})
                price = price_element.text
                bad_character = self.find_wrong_letters(price[:-2])
                if price == "Zapytaj o cenę":
                    price = None
                else:
                    price = float(
                        price.replace(bad_character, "")[:-2].replace(",", ".")
                    )
            except Exception as e:
                value = price_element.text if price_element else None
                error = ErrorLogs(
                    type=type, exception=e, value=value, value_type="price"
                )
                session.add(error)

            try:
                address_element = offer.find("div", {"class": ["css-12h460e escb8gg1"]})
                address = address_element.text
            except Exception as e:
                value = address_element.text if address_element else None
                error = ErrorLogs(
                    exception=e, value=value, type=type, value_type="address"
                )
                session.add(error)

            try:
                title_element = offer.find("p", {"class": ["css-u3orbr e1g5xnx10"]})
                title = title_element.text
            except Exception as e:
                value = title_element.text if title_element else None
                error = ErrorLogs(
                    exception=e, value=value, type=type, value_type="title"
                )
                session.add(error)

            params = offer.find("div", {"class": ["css-1c1kq07 e1clni9t0"]})
            params_dd = params.find_all("dd")

            if type in ("dom_pierwotny", "dom_wtorny"):
                try:
                    rooms = int(params_dd[0].text.split(" ")[0].replace("+", ""))
                except Exception as e:
                    value = params_dd[0].text
                    error = ErrorLogs(
                        exception=e, value=value, type=type, value_type="rooms"
                    )
                    session.add(error)
                try:
                    size = float(params_dd[1].text.split(" ")[0])
                except Exception as e:
                    value = params_dd[1].text
                    error = ErrorLogs(
                        exception=e, value=value, type=type, value_type="size"
                    )
                    session.add(error)
            elif params_dd and type == "dzialki":
                try:
                    size = float(params_dd[0].text.split(" ")[0])
                except Exception as e:
                    value = params_dd[0].text
                    error = ErrorLogs(
                        exception=e, value=value, type=type, value_type="size"
                    )
                    session.add(error)
            elif params_dd and type in (
                "mieszkanie_pierwotny",
                "mieszkanie_wtorny",
            ):
                try:
                    rooms = int(params_dd[0].text.split(" ")[0].replace("+", ""))
                except Exception as e:
                    value = params_dd[0].text
                    error = ErrorLogs(
                        exception=e, value=value, type=type, value_type="rooms"
                    )
                    session.add(error)

                try:
                    size = float(params_dd[1].text.split(" ")[0])
                except Exception as e:
                    value = params_dd[1].text
                    error = ErrorLogs(
                        exception=e, value=value, type=type, value_type="size"
                    )
                    session.add(error)
                try:
                    if len(params_dd) > 3:
                        floor_elem = params_dd[3].text
                    else:
                        floor_elem = params_dd[2].text
                    if floor_elem == "parter":
                        floor = 0
                    elif "piętro" in floor_elem:
                        floor = int(floor_elem.split(" ")[0].replace("+", ""))
                    else:
                        floor = int(floor_elem)

                    test = 0

                except Exception as e:
                    value = floor_elem
                    error = ErrorLogs(
                        exception=e, value=value, type=type, value_type="floor"
                    )
                    session.add(error)
            try:
                price_per_m = np.round(price / size, 2) if price and size else None
            except Exception as e:
                value = f"{price}/{size}" if price and size else None
                error = ErrorLogs(
                    exception=e, value=value, type=type, value_type="price_per_m"
                )
                session.add(error)

            try:
                seller_element = offer.find("div", {"class": ["css-1sylyl4 es3mydq3"]})
                seller = seller_element.text
            except Exception as e:
                seller = None

            try:
                seller_type_element = offer.find(
                    "div", {"class": ["css-196u6lt es3mydq4"]}
                )
                seller_type = seller_type_element.text if seller_type_element else None
            except Exception as e:
                seller_type = None

            try:
                bumped_element = offer.find("div", {"class": ["css-gduqhf es3mydq5"]})
                bumped_text = bumped_element.text
                if bumped_text == "Podbite":
                    bumped = True
                else:
                    bumped = False
            except Exception as e:
                value = bumped_element.text if bumped_element else None
                error = ErrorLogs(
                    exception=e, value=value, type=type, value_type="bumped"
                )
                session.add(error)

            new_offer = Offers(
                type=type,
                link=link,
                title=title,
                seller=seller,
                seller_type=seller_type,
                bumped=bumped,
                page=page_num,
                position=position,
                n_scrap=self.n_scrap,
                address=address,
                rooms=rooms,
                floor=floor,
                size=size,
                price=price,
                price_per_m=price_per_m,
                additional_params=str(params_dd),
            )

            session.add(new_offer)

            if self.test_run:
                self.data.append(
                    {
                        "type": type,
                        "link": link,
                        "title": title,
                        "seller": seller,
                        "seller_type": seller_type,
                        "bumped": bumped,
                        "page": page_num,
                        "position": position,
                        "n_scrap": self.n_scrap,
                        "address": address,
                        "rooms": rooms,
                        "floor": floor,
                        "size": size,
                        "price": price,
                        "price_per_m": price_per_m,
                        "additional_params": str(params_dd),
                    }
                )

        # except Exception as e:
        #     print(e)
        end_time = datetime.now()
        elapsed_time = np.round((end_time - start_time).total_seconds(), 1)
        time_per_offer = np.round(elapsed_time / n_offers, 2)
        print(
            f"{type}, page {page_num}, offers {n_offers}: {elapsed_time}s, {time_per_offer}s"
        )

        page_runtime = Runtime(
            type=type,
            n_offers=n_offers,
            page=page_num,
            n_scrap=self.n_scrap,
            threads=self.threads,
            time_s=elapsed_time,
            time_per_offer=time_per_offer,
        )
        session.add(page_runtime)

        if self.save_to_db:
            session.commit()

    def scrap_pages(self, type, start_page, chunk_size):
        self.init_driver()
        session = Session()
        task = CeleryTasks(
            type=type,
            status="QUEUE",
            time_start=datetime.now(),
            pages=chunk_size,
            start_page=start_page,
            threads=self.threads,
        )
        session.add(task)
        session.commit()
        # odpalic selenium i strzelac linkami zapisujac kontent
        # retry = 0
        # completed = 0
        for page_num in range(start_page, start_page + chunk_size):
            for z in range(0, 3):
                try:
                    self.scrap_offers(type, page_num)
                    break
                except Exception as e:
                    print(e)
                    # self.driver.close()
                    self.driver.quit()
                    self.init_driver()
                    print(f"Retrying open the website {z}")
        # self.driver.close()
        self.driver.quit()
        task.time_end = datetime.now()
        task.runtime = np.round((task.time_end - task.time_start).total_seconds(), 1)
        task.status = "FINISHED"
        session.commit()
        session.close()

    def pages_from_db(self):
        session = Session()
        pages_to_scrap = (
            session.query(OtodomWebsite).filter(OtodomWebsite.active == 1).all()
        )
        session.close()
        return pages_to_scrap

    def run_tests(self):
        print("Start test run")
        for type in [
            "dzialki",
            "mieszkanie_pierwotny",
            "mieszkanie_wtorny",
            "dom_pierwotny",
            "dom_wtorny",
        ]:
            chunk_size = 1
            n_pages = 1
            for i in range(0, 1, chunk_size):
                start = i + 1
                size = min(chunk_size, n_pages - i)
                self.scrap_pages(type, start, size)

            df = pd.DataFrame(self.data)

            df_size = df.shape[0]
            print(df_size)

            if df_size == 0:
                raise ValueError(f"No offers in type {type}")

            for col in df.columns:
                null_count = df[col].isnull().sum()
                null_percent = null_count / df_size
                if col in [
                    "type",
                    "link",
                    "title",
                    "bumped",
                    "page",
                    "position",
                    "n_scrap",
                    "address",
                    "size",
                    "price",
                    "price_per_m",
                    "additional_parms",
                ]:
                    if null_percent > 0.9:
                        raise ValueError(
                            f"Type: {type} column: {col} has {round(null_cont/df_size*100,2)}% null values"
                        )
                elif col in ["seller", "seller_type"]:
                    if null_count == df_size:
                        raise ValueError(
                            f"Type: {type} column: {col} has all values null"
                        )
                elif col == "rooms" and type != "dzialki":
                    if null_percent > 0.9:
                        raise ValueError(
                            f"Type: {type} column: {col} has {round(null_cont/df_size*100,2)}% null values"
                        )
                elif col == "floor" and type in [
                    "mieszkanie_wtorny",
                    "mieszkanie_pierwotny",
                ]:
                    if null_percent > 0.9:
                        raise ValueError(
                            f"Type: {type} column: {col} has {round(null_cont/df_size*100,2)}% null values"
                        )
                else:
                    pass

            self.data = []
