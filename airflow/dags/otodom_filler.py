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

    def __init__(self, threads=None, save_to_db=True, save_to_csv=False):
        self.threads = threads
        self.save_to_db = save_to_db
        self.save_to_csv = save_to_csv
        self.data = []

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
            url = f"http://172.22.0.5:8080/search?q={address}&format=json&addressdetails=1&limit=1"
            # url = f"http://localhost:8999/search?q={address}&format=json&addressdetails=1&limit=1"
            print(url)
            response = requests.get(url)
            print(response)
            status_code = response.status_code
            print(status_code)

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
                params = {}
                params["lat"] = float(data[0]["lat"])
                params["lon"] = float(data[0]["lon"])
                api_address = data[0]["address"]

                try:
                    params["road"] = api_address["road"]
                except:
                    params["road"] = None
                try:
                    params["city"] = api_address["town"]
                except:
                    try:
                        params["city"] = api_address["city"]
                    except:
                        try:
                            params["city"] = api_address["village"]
                        except:
                            params["city"] = None
                try:
                    params["municipality"] = api_address["municipality"]
                except:
                    params["municipality"] = None
                try:
                    params["county"] = api_address["county"]
                except:
                    params["county"] = None
                try:
                    params["vivodeship"] = api_address["state"].split(" ")[1]
                except:
                    params["vivodeship"] = None
                try:
                    params["postcode"] = api_address["postcode"]
                except:
                    params["postcode"] = None

                return params
        else:
            return None

    def update_row(self, offer_id):
        print(f"Offer id {offer_id}")
        session = Session()
        offer = session.query(Offers).get(offer_id)

        if offer and offer.offer_loc_id is None:
            offer_loc = (
                session.query(OffersLoc)
                .filter(OffersLoc.link == offer.link)
                .order_by(OffersLoc.id.desc())
                .first()
            )

            address = offer.address
            print(address)

            if not offer_loc:
                if address != "" and address:

                    params = self.nominatim_request(address, offer_id)

                    if params:
                        new_offer_loc = OffersLoc(
                            lat=params["lat"],
                            lon=params["lon"],
                            city=params["city"],
                            municipality=params["municipality"],
                            county=params["county"],
                            vivodeship=params["vivodeship"],
                            postcode=params["postcode"],
                            link=offer.link,
                            address=offer.address,
                            type=offer.type,
                            filled=1,
                        )
                        add_new_offer_loc = True
                    else:
                        new_offer_loc = OffersLoc(address="", filled=1)
                        add_new_offer_loc = True
                else:
                    new_offer_loc = OffersLoc(address="", filled=1)
                    add_new_offer_loc = True
            else:
                if offer_loc.city:
                    add_new_offer_loc = False
                else:
                    add_new_offer_loc = False
                    if address != "" and address:
                        params = self.nominatim_request(address, offer_id)
                        offer_loc.lat = params["lat"]
                        offer_loc.lon = params["lon"]
                        offer_loc.city = params["city"]
                        offer_loc.municipality = params["municipality"]
                        offer_loc.county = params["county"]
                        offer_loc.vivodeship = params["vivodeship"]
                        offer_loc.postcode = params["postcode"]
                        offer_loc.link = offer.link
                        offer_loc.address = offer.address
                        offer_loc.type = offer.type
                        offer_loc.filled = 1
                        session.commit()

            if self.save_to_db:
                if add_new_offer_loc:
                    session.add(new_offer_loc)
                    session.commit()
                    offer.offer_loc_id = new_offer_loc.id
                    session.commit()
                else:
                    if offer.offer_loc_id is None:
                        offer.offer_loc_id = offer_loc.id
                        session.commit()

            if self.save_to_csv:
                self.data.append(
                    {
                        "lat": new_offer_loc.lat,
                        "lon": new_offer_loc.lon,
                        "city": new_offer_loc.city,
                        "municipality": new_offer_loc.municipality,
                        "county": new_offer_loc.county,
                        "vivodeship": new_offer_loc.vivodeship,
                        "postcode": new_offer_loc.postcode,
                        "link": new_offer_loc.link,
                        "address": new_offer_loc.address,
                        "type": new_offer_loc.type,
                        "filled": new_offer_loc.filled,
                    }
                )
        else:
            print("Offer not found")
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
        print("Start")
        for i in range(start, start + chunk_size):
            print(f"Start update {i} row")
            self.update_row(i)
            print(f"Finished update {i} row")

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
    # model.conf_for_filler(
    #     columns="id", from_table="offers", where_cond="where n_scrap>0"
    # )
    model.update_row(54567)

# if __name__ == "__main__":
#     model = Filler()
#     # id_list = model.take_data_from_db(filled_type=1,column='id')
#     id_list = [278338]
#     model.scrap_chunk_additional_params(id_list)


# przetestowac predkosc requests i aiohttp
