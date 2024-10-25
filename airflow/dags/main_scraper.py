from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import os
import yaml

from sqlalchemy.orm import sessionmaker
from abc import abstractmethod
from functools import wraps


WEBS = {
    "dom_pierwotny": "https://www.otodom.pl/pl/wyniki/sprzedaz/dom%2Crynek-pierwotny/cala-polska?ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&viewType=listing&limit=72",
    "mieszkanie_pierwotny": "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie%2Crynek-pierwotny/cala-polska?limit=72&ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&viewType=listing",
    "dom_wtorny": "https://www.otodom.pl/pl/wyniki/sprzedaz/dom,rynek-wtorny/cala-polska?limit=72&ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&viewType=listing",
    "mieszkanie_wtorny": "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie%2Crynek-wtorny/cala-polska?ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&viewType=listing&limit=72",
    "dzialki": "https://www.otodom.pl/pl/wyniki/sprzedaz/dzialka/cala-polska?ownerTypeSingleSelect=ALL&viewType=listing&limit=72",
}


class WebDriverManager:

    def __init__(self, system):
        self.set_options(system)

    def set_options(self, system):
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
        self.driver = webdriver.Firefox(
            options=self.firefox_options, service=self.service
        )
    

    def close_driver(self):
        if self.driver:
            self.driver.quit()
    
    def parse_web(self, website):
        self.driver.get(website)
        html = self.driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        return soup


class ConfigReader:
    def __init__(self, filename):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.conf_path = os.path.join(current_dir, "conf", filename)
        self.read_config()

    def read_config(self):
        with open(self.conf_path, "r") as file:
            self.config = yaml.safe_load(file)


class DatabaseManager:
    def __init__(self, database, enabled=True):
        self.enabled = enabled
        self.database = database
        self.conf = ConfigReader("conf_db.yaml").config[database]

        db_url = f'mysql+pymysql://{self.conf["username"]}:{self.conf["password"]}@{self.conf["database_ip"]}/{self.conf["database_name"]}'
        self.engine = create_engine(
            db_url,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=1800,
        )

    def requires_enabled(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self.enabled:
                return
            return func(self, *args, **kwargs)

        return wrapper

    @requires_enabled
    def create_session(self):
        self.session = sessionmaker(bind=self.engine)()

    @requires_enabled
    def end_session(self):
        self.session.close()

    @requires_enabled
    def commit(self):
        self.session.commit()

    @requires_enabled
    def add(self, object):
        self.session.add(object)
    
    def commit_object(self,object):
        self.create_session()
        self.add(object)
        self.commit()
        self.end_session()


class Scraper:

    def __init__(self, system,database, save_to_db=True):
        self.web_driver_manager = WebDriverManager(system=system)
        self.database_manager = DatabaseManager(database=database,enabled=save_to_db)

    def scrap_chunk_pages(self, start_page, chunk_size):
        self.web_driver_manager.init_driver()
        for page_num in range(start_page, start_page + chunk_size):
            for z in range(0, 3):
                try:
                    self.scrap_one_page(page_num)
                    break
                except Exception as e:
                    print(e)
                    self.web_driver_manager.close_driver()
                    self.web_driver_manager.init_driver()
                    print(f"Retrying open the website {z}")
        self.web_driver_manager.close_driver()

    @abstractmethod
    def scrap_one_page(self):
        pass

    @abstractmethod
    def check_number_of_pages(self):
        pass
