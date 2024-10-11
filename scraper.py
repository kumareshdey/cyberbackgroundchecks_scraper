
import logging

from bs4 import BeautifulSoup
from setup import get_driver, retry, proxied_request
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import pandas as pd
import os
class Usps:
    def __init__(self, log: logging, zip) -> None:
        self.log = log
        self.zip = zip


    def unique_city(self, city_list):
        unique_cities = []
        seen_prefixes = set()

        for city in city_list:
            prefix = city[:3]
            if prefix not in seen_prefixes:
                unique_cities.append(city)
                seen_prefixes.add(prefix)
        
        return unique_cities


    @retry(tries=4, delay=10)
    def get_city_from_zipcode(self):
        self.log.info(f"Fetching city of zipcode = {self.zip}")
        with get_driver() as driver:
            driver.get("https://tools.usps.com/zip-code-lookup.htm?citybyzipcode")
            zip_field = driver.find_element(By.ID, "tZip")
            zip_field.send_keys(self.zip)
            submit = driver.find_element(By.ID, """cities-by-zip-code""")
            submit.click()
            wait = WebDriverWait(driver, 20)
            wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "recommended-cities")))
            soup = BeautifulSoup(driver.page_source, "lxml")
            recommended = [text.text for text in soup.find(class_="recommended-cities").find_all(class_="row-detail-wrapper")]
            others = [text.text for text in soup.find(class_="other-city-names").find_all(class_="row-detail-wrapper")]
            recommended.extend(others)
            recommended = self.unique_city(recommended)
            self.log.info(f"Found cities: {recommended}")
            return recommended
        

class Cyberbackgroundchecks:
    def __init__(self, first_name: str, last_name: str, street: str, city: str, dist: str, zip: str, log: logging) -> None:
        self.first_name = first_name
        self.last_name = last_name
        self.street = street
        self.city = city
        self.dist = dist
        self.zip = zip
        self.log = log
        self.base_url = 'https://www.cyberbackgroundchecks.com'

    def generate_url(self):
        url = f"""{self.base_url}/people/{self.first_name.split(' ')[0].lower()}-{self.last_name.split(' ')[-1].lower()}/{self.dist.lower()}/{self.city.replace(" ", "-").lower()}"""
        self.log.info(f"Querying url: {url}")
        return url
    
    def verify_data(self, card):
        if all([x.lower() in card.get_text().lower() for x in [self.first_name.split(' ')[0], self.last_name.split(' ')[-1], self.city]]):
            self.log.info("Verified data. Extracting email now.")
            return True
        return False
    
    def extract_email(self, url):
        def extract_email_from_href(entry):
            href = entry.a['href']
            email = href.split('/')[-1].replace("_.", "@")
            return email
        
        allowed_domains = [
            "@yahoo.com",
            "@hotmail.com",
            "@gmail.com",
            "@aol.com",
            "@msn.com",
            "@outlook.com",
            "@live.com"
        ]
        self.log.info(f"Extracting email from url: {url}")
        response = proxied_request(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        emails = []
        for section in soup.find_all(class_='text-secondary'):
            if 'email addresses' in section.get_text().lower():
                emails.extend([extract_email_from_href(email) for email in section.find_all('h3')])
        final_email = []
        for email in emails:
            if any(domain in email for domain in allowed_domains):
                final_email.append(email)
        return final_email
    
    def cyberbackgroundchecks_manager(self):
        url = self.generate_url()
        try:
            response = proxied_request(url)
        except:
            self.log.info(f"Could not find the page. Trying with first name and last name only.")
            response = proxied_request(f"""{self.base_url}/people/{self.first_name.split(' ')[0].lower()}-{self.last_name.split(' ')[-1].lower()}""")
        self.log.info(f"Got response: {response.status_code}")
        soup = BeautifulSoup(response.text, 'html.parser')
        cards = soup.find_all(class_='card card-hover')
        emails = []
        self.log.info(f"Verifying {len(cards)} cards.")
        for card in cards:
            if 'VIEW DETAILS' in card.get_text() and self.verify_data(card):
                view_details = card.find_all('a')
                view_details = [x for x in view_details if 'VIEW DETAILS' in x.get_text()]
                view_details = view_details[0]
                if view_details and view_details.get('href'):
                    email_url = self.base_url + view_details['href']
                    emails.extend(self.extract_email(email_url))
        self.log.info(f"Got emails: {emails}")
        return emails

def process_row(row, result_excel_file_path, log: logging):
    rows = []
    try:
        log.info(f"Scraping for:\n{row}")
        usps = Usps(zip=row["ZIP"], log=log)
        cities = usps.get_city_from_zipcode()
        for city in cities:
            city = city.split(" ")
            city, dist = ' '.join(city[:-1]), city[-1]
            cyberbackgroundchecks = Cyberbackgroundchecks(
                first_name=row["FIRST_NAME"],
                last_name=row["LAST_NAME"],
                street=row["STREET"],
                city=city,
                dist=dist,
                zip=str(row["ZIP"]),
                log=log
            )
            emails = cyberbackgroundchecks.cyberbackgroundchecks_manager()
            new_row = {
                "FIRST_NAME": row["FIRST_NAME"],
                "LAST_NAME": row["LAST_NAME"],
                "STREET": row["STREET"],
                "CITY": city,
                "DIST": dist,
                "ZIP": row["ZIP"],
                "EMAIL": emails,
                "STATUS": 'SUCCESS'
            }
            rows.append(new_row)
    except Exception as e:
        try:
            new_row = {
                "FIRST_NAME": row["FIRST_NAME"],
                "LAST_NAME": row["LAST_NAME"],
                "STREET": row["STREET"],
                "CITY": city,
                "DIST": dist,
                "ZIP": row["ZIP"],
                "EMAIL": [],
                "STATUS": "ERROR"
            }
        except:
            new_row = {
                "FIRST_NAME": row["FIRST_NAME"],
                "LAST_NAME": row["LAST_NAME"],
                "STREET": row["STREET"],
                "CITY": '',
                "DIST": '',
                "ZIP": row["ZIP"],
                "EMAIL": [],
                "STATUS": "ERROR"
            }
        rows.append(new_row)

    df = pd.DataFrame(rows)
    if os.path.exists(result_excel_file_path):
        existing_df = pd.read_excel(
            result_excel_file_path, names=["FIRST_NAME", "LAST_NAME", "STREET", "CITY", "DIST", "ZIP", "EMAIL", "STATUS"], engine="openpyxl"
        )
        existing_df = pd.concat([existing_df, df], ignore_index=True)
        df = existing_df
    else:
        with open(result_excel_file_path, "w"):
            pass

    df = df.explode("EMAIL", ignore_index=True)
    duplicated_rows = df.duplicated(subset=["FIRST_NAME", "LAST_NAME", "STREET", "CITY", "DIST", "ZIP"])
    df.loc[duplicated_rows, ["FIRST_NAME", "LAST_NAME", "STREET", "CITY", "DIST", "ZIP"]] = ""
    log.info(f"Saved to excel: {result_excel_file_path}")

    return df