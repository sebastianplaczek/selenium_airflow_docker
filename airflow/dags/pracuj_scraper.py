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

from main_scraper import Scraper
from models import PracujJobOffers


class PracujScraper(Scraper):

    def scrap_one_page(self, i):
        website = f"https://it.pracuj.pl/praca/it%20-%20rozw%C3%B3j%20oprogramowania;cc,5016?pn={i}"

        soup = self.web_driver_manager.parse_web(website)

        offers = soup.find_all("div", {"class": "tiles_b18pwp01 core_po9665q"})
        for offer in offers:
            offer_dict = {}
            try:
                link = offer.find("a", {"class": "tiles_o1859gd9 core_n194fgoq"})[
                    "href"
                ]
            except:
                link = None
            try:
                title = offer.find("a", {"class": "tiles_o1859gd9 core_n194fgoq"}).text
            except:
                title = None
            try:
                salary = offer.find("span", {"class", "tiles_s1x1fda3"}).text.replace(
                    "\xa0", " "
                )
            except:
                salary = None
            try:
                company = offer.find(
                    "h3", {"class": "tiles_chl8gsf size-caption core_t1rst47b"}
                ).text
            except:
                company = None
            try:
                location = offer.find(
                    "h4", {"class": "tiles_r11dm8ju size-caption core_t1rst47b"}
                ).text
            except:
                location = None

            try:
                tags = offer.find_all(
                    "span",
                    {
                        "class": "_chip_hmm6b_1 _chip--highlight_hmm6b_1 _chip--small_hmm6b_1 _chip--full-corner_hmm6b_1 tiles_ccjb265"
                    },
                )
                tags_list = json.dumps([_.text for _ in tags])
            except:
                tags_list = None

            try:
                date_pub = offer.find(
                    "p",
                    {
                        "class": "tiles_arxx4s5 tiles_s1ojn2a1 tiles_bg8mbli core_pk4iags size-caption core_t1rst47b"
                    },
                ).text
            except:
                date_pub = None

            try:
                additional_info = offer.find_all(
                    "li", {"class": "mobile-hidden tiles_i14a41ct"}
                )
                additional_info_list = json.dumps([_.text for _ in additional_info])
            except:
                additional_info = None

            offer_dict["link"] = link
            offer_dict["title"] = title
            offer_dict["salary"] = salary
            offer_dict["company"] = company
            offer_dict["location"] = location
            offer_dict["tags"] = tags_list
            offer_dict["additional_info"] = additional_info_list
            offer_dict["date_pub"] = date_pub
            

            new_offer = PracujJobOffers(**offer_dict)
            self.database_manager.commit_object(new_offer)






    def check_number_of_pages(self):
        self.web_driver_manager.init_driver()
        website = "https://it.pracuj.pl/praca/it%20-%20rozw%C3%B3j%20oprogramowania;cc,5016?pn=1"
        soup = self.web_driver_manager.parse_web(website)
        page_num = int(
            soup.find("span", {"data-test": "top-pagination-max-page-number"}).text
        )
        self.web_driver_manager.close_driver()


obj = PracujScraper(system="windows", database="gcp")
obj.scrap_chunk_pages(1, 1)
test = 0
