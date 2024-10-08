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

class Scraper():

    def __init__(self,system):
        self.system = system

    def init_driver(self):
        firefox_options = Options()
        firefox_options.add_argument("--headless")
        firefox_options.add_argument("--no-sandbox")
        firefox_options.add_argument("--disable-gpu")
        firefox_options.add_argument("--incognito")
        firefox_options.add_argument("--window-size=1600,900")
        firefox_options.add_argument("--disable-dev-shm-usage")

        if self.system=="linux":
            service = FirefoxService(executable_path="/usr/local/bin/geckodriver")
        elif self.system=="windows":
            firefox_binary_path = "C:\\Program Files\\Mozilla Firefox\\firefox.exe"
            service = FirefoxService(
            executable_path="C:\projects\small_scrapper\geckodriver\geckodriver.exe")
            firefox_options.binary_location = firefox_binary_path

        else:
            raise ValueError(f"Wrong system type {system}")

        self.driver = webdriver.Firefox(options=firefox_options, service=service)

    def scrap_page(self,i):
        website = f"https://it.pracuj.pl/praca/it%20-%20rozw%C3%B3j%20oprogramowania;cc,5016?pn={i}"
        self.driver.get(website)
        html = self.driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        offers = soup.find_all("div", {"class": "tiles_b18pwp01 core_po9665q"})
        for offer in offers:
            offer_dict = {}
            link = offer.find("a", {"class": "tiles_o1859gd9 core_n194fgoq"})[
                "href"
            ]
            title = offer.find("a", {"class": "tiles_o1859gd9 core_n194fgoq"}).text
            try:
                salary = offer.find(
                    "span", {"class", "tiles_s1x1fda3"}
                ).text.replace("\xa0", " ")
            except:
                salary = None
            company = offer.find(
                "h3", {"class": "tiles_chl8gsf size-caption core_t1rst47b"}
            ).text
            location = offer.find(
                "h4", {"class": "tiles_r11dm8ju size-caption core_t1rst47b"}
            ).text

            tags = offer.find_all(
                "span",
                {
                    "class": "_chip_hmm6b_1 _chip--highlight_hmm6b_1 _chip--small_hmm6b_1 _chip--full-corner_hmm6b_1"
                },
            )
            tags_list = json.dumps([_.text for _ in tags])

            date_pub = offer.find(
                "p",
                {
                    "class": "tiles_arxx4s5 tiles_s1ojn2a1 tiles_bg8mbli core_pk4iags size-caption core_t1rst47b"
                },
            ).text

            offer_dict["link"] = link
            offer_dict["title"] = title
            offer_dict["salary"] = link
            offer_dict["company"] = company
            offer_dict["location"] = location
            offer_dict["tags"] = tags
    def run(self):

        # self.init_driver()
        # website = "https://it.pracuj.pl/praca/it%20-%20rozw%C3%B3j%20oprogramowania;cc,5016?pn=1"
        # self.driver.get(website)
        # html = self.driver.page_source
        # soup = BeautifulSoup(html, "html.parser")
        # page_num = int(soup.find("span", {"data-test": "top-pagination-max-page-number"}).text)
        # self.driver.close()
        page_num = 50

        offers_list = []

        for i in range(0,page_num):
            self.init_driver()
            self.scrap_page(i)
            self.driver.close()


obj = Scraper(system="windows")
obj.run()

"""
"""
