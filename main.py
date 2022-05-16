from multiprocessing.connection import wait
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException
from pandas.tseries.offsets import BDay
from os import path
import json
import time
import mysql.connector
import datetime


class ScrapeBVB:
    def __init__(self) -> None:
        self.companiesList = []
        self.companies = []
        self.tickers = []
        self.page = 1
        chrome_options = Options()
        chrome_options.add_argument("--lang=ro-RO")
        prefs = {
            "translate_whitelists": {"your native language": "ro"},
            "translate": {"enabled": "True"}
        }
        chrome_options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        self.driver.get("https://bvb.ro/FinancialInstruments/Markets/Shares")
        self.scrape_page()
        
    def scrape_page(self):
        companies_rows = self.driver.find_elements(By.CSS_SELECTOR, ".dataTable > tbody > tr")
        for company_element in companies_rows:
            self.driver.execute_script("arguments[0].scrollIntoView();", company_element)
            company_ticker = company_element.find_element(By.CSS_SELECTOR, "td:first-of-type strong").text
            company_name = company_element.find_element(By.CSS_SELECTOR, "td:nth-of-type(2)").text
            company_link = company_element.find_element(By.CSS_SELECTOR, "td:first-of-type a").get_attribute('href')

            ticker_details = {
                "ticker": company_ticker,
                "company": company_name,
                "link": company_link
            }
            self.companiesList.append(ticker_details)
        self.page += 1
        self.get_next_page()
    
    def get_next_page(self):
        try:
            next_link = self.driver.find_element(By.CSS_SELECTOR, ".paginate_button.next:not(.disabled)")
            next_link.click()
            self.scrape_page()
        except NoSuchElementException:
            self.get_prices()
            self.write_json()
            #self.insert_into_db()
            
    def get_prices(self):
        time_str = time.strftime("%Y-%m-%d")
        for index in range(len(self.companiesList)):

            ticker = self.companiesList[index]['ticker']
            company_name = self.companiesList[index]['company']
            company_link = self.companiesList[index]['link']

            self.driver.get(company_link)
            last_price = self.driver.find_element(By.XPATH, "//*[text()='Ultimul pret']//following::td[1]").text
            var = self.driver.find_element(By.XPATH, "//*[text()='Var']//following::td[1]").text
            var_percentage = self.driver.find_element(By.XPATH, "//*[text()='Var (%)']//following::td[1]").text
            opening_price = self.driver.find_element(By.XPATH, "//*[text()='Pret deschidere']//following::td[1]").text
            maximum_price = self.driver.find_element(By.XPATH, "//*[text()='Pret maxim']//following::td[1]").text
            minimum_price = self.driver.find_element(By.XPATH, "//*[text()='Pret minim']//following::td[1]").text
            max_52_weeks = self.driver.find_element(By.XPATH, "//*[text()='Max. 52 saptamani']//following::td[1]").text
            min_52_weeks = self.driver.find_element(By.XPATH, "//*[text()='Min. 52 saptamani']//following::td[1]").text
            try:
                price_earning_ratio = self.driver.find_element(By.XPATH, "//*[text()='PER']//following::td[1]").text
            except NoSuchElementException:
                price_earning_ratio = '0,00'

            company_info = self.driver.find_element(By.XPATH, "//input[@type='submit'][@value='Emitent']")
            self.driver.execute_script("arguments[0].scrollIntoView();", company_info)
            company_info.click()
            try:
                activity_domain = WebDriverWait(self.driver, 10).until(lambda d: d.find_element(By.XPATH, "//td[text()='Domeniu de activitate']//following::td[1]").text)
            except NoSuchElementException:
                activity_domain = ''
            except TimeoutException:
                activity_domain = ''

            company_data = {
                "ticker": ticker,
                "company_name": company_name,
                "link": company_link,
                "activity_domain": activity_domain
            }
            self.companies.append(company_data)

            ticker_data = {
                "ticker": ticker,
                "opening_price": opening_price.replace(',', '.'),
                "minimum_price": minimum_price.replace(',', '.'),
                "maximum_price": maximum_price.replace(',', '.'),
                "last_price": last_price.replace(',', '.'),
                "var": var.replace(',', '.'),
                "var_percentage": var_percentage.replace(',', '.'),
                "max_52_weeks": max_52_weeks.replace(',', '.'),
                "min_52_weeks": min_52_weeks.replace(',', '.'),
                "price_earning_ratio": price_earning_ratio.replace(',', '.'),
                "ticker_date": time_str
            }
            self.tickers.append(ticker_data)
            
    def write_json(self):
        time_str = time.strftime("%Y%m%d")
        companies_json = json.dumps(self.companies, indent=4)
        tickers_json = json.dumps(self.tickers, indent=4)
        with open("./json_data/companies_" + time_str + ".json", "w") as f:
            f.write(companies_json)
        with open("./json_data/tickers_" + time_str + ".json", "w") as f:
            f.write(tickers_json)



class UpdateJsons:
    def __init__(self) -> None:
        current_date = datetime.date.today()
        self.current_date_str = current_date.strftime("%Y%m%d")
        previous_date = current_date - BDay(0)
        self.previous_date_str = previous_date.strftime("%Y%m%d")

        with open('json_data/tickers_' + self.previous_date_str + '.json') as json_file:
            self.previous_day_ticker = json.load(json_file)
        self.prev_date_values_dict = {}
        for item in self.previous_day_ticker:
            var = item.get('var', '0')
            negatives_since_positive = item.get('negatives_since_positive', '0')
            positives_since_negative = item.get('positives_since_negative', '0')
            self.prev_date_values_dict[item.get('ticker')] = {
                'var': var,
                'negatives_since_positive': negatives_since_positive,
                'positives_since_negative': positives_since_negative,
            }
        with open('json_data/tickers_' + self.current_date_str + '.json') as json_file:
            self.current_day_ticker = json.load(json_file)
        self.update_variation_count()

    def update_variation_count(self):
        for item in self.current_day_ticker:
            prev_date_ticker = self.prev_date_values_dict[item['ticker']]
            prev_negatives_since_positive = prev_date_ticker.get('negatives_since_positive', '0')
            prev_positives_since_negative = prev_date_ticker.get('positives_since_negative', '0')
            negatives_since_positive = int(prev_negatives_since_positive) + 1 if (float(item['var']) < 0) else 0
            positives_since_negative = int(prev_positives_since_negative) + 1 if (float(item['var']) > 0) else 0
            item['negatives_since_positive'] = str(negatives_since_positive)
            item['positives_since_negative'] = str(positives_since_negative)

        tickers_json = json.dumps(self.current_day_ticker, indent=4)
        with open("./json_data/tickers_" + self.current_date_str + ".json", "w") as f:
            f.write(tickers_json)

class InsertIntoDB:
    def __init__(self) -> None:
        current_date = datetime.date.today()
        self.current_date_str = current_date.strftime("%Y%m%d")
        self.connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="zap"
        )
        self.update_companies()
        self.update_tickers()
        self.close_connection()

    def update_companies(self):
        with open('json_data/companies_' + self.current_date_str + '.json') as json_file:
            current_date_companies = json.load(json_file)

        try:
            cursor = self.connection.cursor()

            sql = "INSERT INTO companies (ticker, company_name, link, activity_domain) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE ticker = VALUES(ticker), company_name = VALUES(company_name), link = VALUES(link), activity_domain = VALUES(activity_domain)"
            for item in current_date_companies:
                ticker_name = item.get('ticker')
                company_name = item.get('company_name')
                company_link = item.get('link')
                activity_domain = item.get('activity_domain')
                val = (
                    ticker_name,
                    company_name,
                    company_link,
                    activity_domain
                )
                cursor.execute(sql, val)
            self.connection.commit()
        finally:
            cursor.close()
            print("Companies updated in DB")

    def update_tickers(self):
        with open('json_data/tickers_' + self.current_date_str + '.json') as json_file:
            current_date_tickers = json.load(json_file)
        try:
            cursor = self.connection.cursor()

            sql = "INSERT INTO tickers (ticker, opening_price, minimum_price, maximum_price, last_price, var, var_percentage, max_52_weeks, min_52_weeks, price_earning_ratio, negatives_since_positive, positives_since_negative, ticker_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE opening_price = VALUES(opening_price), minimum_price = VALUES(minimum_price), maximum_price = VALUES(maximum_price), last_price = VALUES(last_price), var = VALUES(var), var_percentage = VALUES(var_percentage), max_52_weeks = VALUES(max_52_weeks), min_52_weeks = VALUES(min_52_weeks), price_earning_ratio = VALUES(price_earning_ratio), negatives_since_positive = VALUES(negatives_since_positive), positives_since_negative = VALUES(positives_since_negative), ticker_date = VALUES(ticker_date)"
            for item in current_date_tickers:
                ticker_name = item.get('ticker')
                opening_price = item.get('opening_price')
                minimum_price = item.get('minimum_price')
                maximum_price = item.get('maximum_price')
                last_price = item.get('last_price')
                var = item.get('var')
                var_percentage = item.get('var_percentage')
                max_52_weeks = item.get('max_52_weeks')
                min_52_weeks = item.get('min_52_weeks')
                price_earning_ratio = item.get('price_earning_ratio')
                negatives_since_positive = item.get('negatives_since_positive')
                positives_since_negative = item.get('positives_since_negative')
                ticker_date = self.current_date_str
                val = (
                    ticker_name,
                    opening_price,
                    minimum_price,
                    maximum_price,
                    last_price,
                    var,
                    var_percentage,
                    max_52_weeks,
                    min_52_weeks,
                    price_earning_ratio,
                    negatives_since_positive,
                    positives_since_negative,
                    ticker_date
                )
                cursor.execute(sql, val)
            self.connection.commit()
        finally:
            cursor.close()
            print("Tickers updated in DB")

    def close_connection(self):
        if self.connection.is_connected():
            self.connection.close()
            print("MySQL connection is closed")


ScrapeBVB()
UpdateJsons()
InsertIntoDB()


# chrome_options = Options()
# chrome_options.add_argument("--lang=ro-RO")
# prefs = {
#     "translate_whitelists": {"your native language": "ro"},
#     "translate": {"enabled": "True"}
# }
# chrome_options.add_experimental_option("prefs", prefs)
# # chrome_options.add_experimental_option("detach", True)
# driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), chrome_options=chrome_options)
# driver.get("https://bvb.ro/FinancialInstruments/Details/FinancialInstrumentsDetails.aspx?s=TLV")
# emitent_info = driver.find_element(By.XPATH, "//input[@type='submit'][@value='Emitent']")
# driver.execute_script("arguments[0].scrollIntoView();", emitent_info)
# emitent_info.click()
# activity_domain_el = WebDriverWait(driver, 20).until(lambda d: d.find_element(By.XPATH, "//td[text()='Domeniu de activitate']//following::td[1]").text)
# print(activity_domain_el)
