import pandas as pd
import numpy as np
import os
import yaml

from main_scraper import Scraper
from models import OtodomOffers,ErrorLogs,OtodomWebsite

class OtodomScraper(Scraper):

    def find_wrong_letters(self, my_str):
        x = ""
        for x in my_str:
            try:
                int(x)
            except:
                break
        return x

    def scrap_one_page(self, page_num):
        website = self.WEBS[self.type].replace("$PAGE",str(page_num))
        soup = self.web_driver_manager.parse_web(website)
        offers = soup.find_all("div", {"class": ["css-13gthep eeungyz2"]})

        for position, offer in enumerate(offers):
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
                price_per_m
            ) = (None, None, None, None, None, None, None, None, None, None, None)
            try:
                link_element = offer.find("a", {"class": ["css-16vl3c1 e17g0c820"]})
                link = "otodom.pl" + link_element["href"]
            except Exception as e:
                error = ErrorLogs(exception=e)
                self.database_manager.add(error)

            try:
                price_element = offer.find("span", {"class": ["css-2bt9f1 evk7nst0"]})
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
                    type=self.type, exception=e, value=value, value_type="price"
                )
                self.database_manager.add(error)

            try:
                address_element = offer.find("p", {"class": ["css-42r2ms eejmx80"]})
                address = address_element.text
            except Exception as e:
                value = address_element.text if address_element else None
                error = ErrorLogs(
                    exception=e, value=value, type=self.type, value_type="address"
                )
                self.database_manager.add(error)

            try:
                title_element = offer.find("p", {"class": ["css-u3orbr e1g5xnx10"]})
                title = title_element.text
            except Exception as e:
                value = title_element.text if title_element else None
                error = ErrorLogs(
                    exception=e, value=value, type=self.type, value_type="title"
                )
                self.database_manager.add(error)

            params = offer.find("div", {"class": ["css-1c1kq07 e1clni9t0"]})
            params_dd = params.find_all("dd")

            if self.type in ("dom_pierwotny", "dom_wtorny"):
                try:
                    rooms = int(params_dd[0].text.split(" ")[0].replace("+", ""))
                except Exception as e:
                    value = params_dd[0].text
                    error = ErrorLogs(
                        exception=e, value=value, type=self.type, value_type="rooms"
                    )
                    self.database_manager.add(error)
                try:
                    size = float(params_dd[1].text.split(" ")[0])
                except Exception as e:
                    value = params_dd[1].text
                    error = ErrorLogs(
                        exception=e, value=value, type=self.type, value_type="size"
                    )
                    self.database_manager.add(error)
            elif params_dd and self.type == "dzialki":
                try:
                    size = float(params_dd[0].text.split(" ")[0])
                    test=0
                except Exception as e:
                    value = params_dd[0].text
                    error = ErrorLogs(
                        exception=e, value=value, type=self.type, value_type="size"
                    )
                    self.database_manager.add(error)
            elif params_dd and self.type in (
                "mieszkanie_pierwotny",
                "mieszkanie_wtorny",
            ):
                try:
                    rooms = int(params_dd[0].text.split(" ")[0].replace("+", ""))
                except Exception as e:
                    value = params_dd[0].text
                    error = ErrorLogs(
                        exception=e, value=value, type=self.type, value_type="rooms"
                    )
                    self.database_manager.add(error)

                try:
                    size = float(params_dd[1].text.split(" ")[0])
                except Exception as e:
                    value = params_dd[1].text
                    error = ErrorLogs(
                        exception=e, value=value, type=self.type, value_type="size"
                    )
                    self.database_manager.add(error)
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
                except Exception as e:
                    value = floor_elem
                    error = ErrorLogs(
                        exception=e, value=value, type=self.type, value_type="floor"
                    )
                    self.database_manager.add(error)
            try:
                price_per_m = np.round(price / size, 2) if price and size else None
            except Exception as e:

                value = f"{price}/{size}" if price and size else None
                error = ErrorLogs(
                    exception=e, value=value, type=self.type, value_type="price_per_m"
                )
                self.database_manager.add(error)

            try:
                seller_element = offer.find("div", {"class": ["css-1sylyl4 es3mydq3","css-zmiifc es3mydq2"]})
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
                    exception=e, value=value, type=self.type, value_type="bumped"
                )
                self.database_manager.add(error)
            
            offer_dict = {
                        "type": self.type,
                        "link": link,
                        "title": title,
                        "seller": seller,
                        "seller_type": seller_type,
                        "bumped": bumped,
                        "page": page_num,
                        "position": position,
                        "address": address,
                        "rooms": rooms,
                        "floor": floor,
                        "size": size,
                        "price": price,
                        "price_per_m": price_per_m,
                        "additional_params": str(params_dd),
                        "data_date" : self.create_date
                    }

            new_offer = OtodomOffers(**offer_dict
            )

            self.database_manager.add(new_offer)

            if self.store_data:
                self.data.append(
                    offer_dict
                )

    def number_of_pages_from_soup(self,soup):
        buttons = soup.find_all("li", {"class": "css-43nhzf"})
        number_of_pages = int(buttons[-1].text)
        return number_of_pages

    def run_tests(self):
        self.data = []
        self.store_data = True
        self.database_manager.enabled = False
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
                self.scrap_chunk_pages(start, size, type)

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
                            f"Type: {type} column: {col} has {round(null_count/df_size*100,2)}% null values"
                        )
                elif col in ["seller", "seller_type"]:
                    if null_count == df_size:
                        raise ValueError(
                            f"Type: {type} column: {col} has all values null"
                        )
                elif col == "rooms" and type != "dzialki":
                    if null_percent > 0.9:
                        raise ValueError(
                            f"Type: {type} column: {col} has {round(null_count/df_size*100,2)}% null values"
                        )
                elif col == "floor" and type in [
                    "mieszkanie_wtorny",
                    "mieszkanie_pierwotny",
                ]:
                    if null_percent > 0.9:
                        raise ValueError(
                            f"Type: {type} column: {col} has {round(null_count/df_size*100,2)}% null values"
                        )
                else:
                    pass
            
            self.database_manager.enabled = False   


# obj = OtodomScraper(name='otodom',system='windows',database='gcp')
