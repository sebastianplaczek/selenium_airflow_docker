from importlib.machinery import SourceFileLoader
import os

pwd = os.path.dirname(os.path.realpath(__file__)) + "/models.py"
models = SourceFileLoader("models", pwd).load_module()

# Session = models.Session()
# Offers = models.Offers()
# OffersLoc = models.OffersLoc()
# NominatimApi = models.NominatimApi()
# CeleryTasks = models.CeleryTasks()
# engine = models.engine


from models import (
    Session,
    Offers,
    OtodomWebsite,
    ScrapInfo,
    OffersLoc,
    Runtime,
    ErrorLogs,
    CeleryTasks,
    NominatimApi,
    engine,
)


import bs4
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup
import re


import requests
import pandas as pd
import json
from datetime import datetime
import numpy as np
import yaml


class Filler:

    def __init__(self, threads=None):
        self.threads = threads

    def init_driver(self):
        firefox_options = Options()
        firefox_options.add_argument("--headless")
        firefox_options.add_argument("--no-sandbox")
        firefox_options.add_argument("--disable-gpu")
        firefox_options.add_argument("--incognito")
        firefox_options.add_argument("--window-size=1600,900")
        firefox_options.add_argument("--disable-dev-shm-usage")
        service = FirefoxService(executable_path="/usr/local/bin/geckodriver")
        self.driver = webdriver.Firefox(options=firefox_options, service=service)

    def conf_for_filler(
        self, columns="id", from_table="offers", where_cond="where n_scrap>0"
    ):

        query = f"""
        SELECT min(id) as start_id,max(id) as end_id
        FROM {from_table}
        {where_cond}

        """
        with engine.connect() as conn:
            self.df = pd.read_sql(sql=query, con=conn.connection)

        conf = {}
        conf["start_id"] = int(self.df["start_id"][0])
        conf["end_id"] = int(self.df["end_id"][0])

        with open("fill_conf.yaml", "w") as file:
            yaml.dump(conf, file, default_flow_style=False)

    def nominatim_request(self, address, offer_id):
        address_list = re.split(r"[ ,]+", address)

        for i in range(0, len(address_list)):
            try:
                address = " ".join(address_list[i:])
            except:
                pass
            url = f"http://172.22.0.3:8080/search?q={address}&format=json&addressdetails=1&limit=1"
            # url = f"http://localhost:8999/search?q={address}&format=json&addressdetails=1&limit=1"
            response = requests.get(url)
            status_code = response.status_code

            try:
                data = response.json()
            except Exception as e:
                data = []
                print(e)

            if status_code == 200 and data != []:
                data = response.json()
                empty = False
            else:
                data = None
                empty = True

            session_db = Session()
            object_db = NominatimApi(
                link=url, status_code=status_code, empty=empty, offer_id=offer_id
            )
            session_db.add(object_db)
            session_db.commit()

            try:
                city = data[0]["address"]["town"]
            except:
                try:
                    city = data[0]["address"]["city"]
                except:
                    try:
                        city = data[0]["address"]["village"]
                    except:
                        city = None

            if city:
                return data

        return data

    def update_row(self, offer_id):

        session = Session()
        offer = session.query(Offers).get(offer_id)

        if offer:
            offer_loc = (
                session.query(OffersLoc).filter(OffersLoc.link == offer.link).first()
            )

            address = offer.address
            print(address)
            if not offer_loc:
                if address != "" and address:

                    data = self.nominatim_request(address, offer_id)
                    if data:
                        lat = float(data[0]["lat"])
                        lon = float(data[0]["lon"])
                        api_address = data[0]["address"]

                        try:
                            road = api_address["road"]
                        except:
                            road = None
                        try:
                            city = api_address["town"]
                        except:
                            try:
                                city = api_address["city"]
                            except:
                                try:
                                    city = api_address["village"]
                                except:
                                    city = None
                        try:
                            municipality = api_address["municipality"]
                        except:
                            municipality = None
                        try:
                            county = api_address["county"]
                        except:
                            county = None
                        try:
                            vivodeship = api_address["state"].split(" ")[1]
                        except:
                            vivodeship = None
                        try:
                            postcode = api_address["postcode"]
                        except:
                            postcode = None

                        new_offer_loc = OffersLoc(
                            lat=lat,
                            lon=lon,
                            city=city,
                            municipality=municipality,
                            county=county,
                            vivodeship=vivodeship,
                            postcode=postcode,
                            link=offer.link,
                            address=offer.address,
                            type=offer.type,
                            filled=1,
                        )

                else:
                    new_offer_loc = OffersLoc(address="", filled=1)

                session.add(new_offer_loc)
                session.commit()
                offer.offer_loc_id = new_offer_loc.id
                session.commit()
        session.close()

    def scrap_additional_params(self, offer_id):

        offer = self.session.query(OffersLoc).get(offer_id)

        self.driver.get("http://www." + offer.link)
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        html = self.driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        additional_params = soup.find_all("div", {"class": ["css-1ivc1bc e26jmad1"]})

        params_dict = {}
        for params in additional_params:
            label = params.find("div", {"class": ["css-rqy0wg e26jmad3"]}).text
            try:
                value = params.find("div", {"class": ["css-1wi2w6s e26jmad5"]}).text
            except:
                value = ""

            if value != "":
                params_dict[label] = value

        offer.filled = 2
        offer.additional_params = json.dumps(params_dict)
        self.session.commit()

    def update_chunk_rows(self, start, chunk_size):
        self.session = Session()
        task = CeleryTasks(
            type="nominatim",
            status="QUEUE",
            time_start=datetime.now(),
            pages=chunk_size,
            threads=self.threads,
        )
        for i in range(start, start + chunk_size):
            self.update_row(i)

        task.time_end = datetime.now()
        task.runtime = np.round((task.time_end - task.time_start).total_seconds(), 1)
        task.status = "FINISHED"
        self.session.commit()
        self.session.close()

    def scrap_chunk_additional_params(self, id_list):
        self.init_driver()
        self.session = Session()
        for offer_id in id_list:
            self.scrap_additional_params(offer_id)
        self.session.close()
        self.driver.close()
        self.driver.quit()


if __name__ == "__main__":
    model = Filler()
    model.conf_for_filler(
        columns="id", from_table="offers", where_cond="where n_scrap>0"
    )
    # id_list = [54567]
    # model.update_chunk_rows(id_list, 1)

# if __name__ == "__main__":
#     model = Filler()
#     # id_list = model.take_data_from_db(filled_type=1,column='id')
#     id_list = [278338]
#     model.scrap_chunk_additional_params(id_list)


# przetestowac predkosc requests i aiohttp
