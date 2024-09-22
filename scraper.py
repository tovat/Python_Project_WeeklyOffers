from bs4 import BeautifulSoup
import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import os
import logging
from datetime import datetime
from funcs import set_up_logging


set_up_logging()
logger = logging.getLogger(__name__)


class Scraper:
    """This class provides functionality to scrape the website 'ereklamblad.se' 
    for weekly offers from grocery stores in Malmö and save it to a CSV file.
    """
    
    def __init__(self, url):
        self.url = url
        self.data = []
        self.driver = None
        logger.info('A scraper object was instantiated with URL: %s', {url})

    def set_up_driver(self):
        """This method sets up the Selenium WebDriver for automating browser interaction."""
        try:
            driver_path = 'chromedriver.exe'
            
            # Create a temporary directory
            temp_user_data_dir = os.path.join(os.getcwd(), "temp_chrome_user_data")

            # Set Chrome options
            chrome_options = Options()

            # Use a temporary user data directory to avoid default browser prompt
            chrome_options.add_argument(f"--user-data-dir={temp_user_data_dir}")

            # Disable prompts and first-run experiences
            chrome_options.add_argument("--no-first-run")
            chrome_options.add_argument("--no-default-browser-check")
            chrome_options.add_argument("--disable-default-apps")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-popup-blocking")
            chrome_options.add_argument("--disable-infobars")
            chrome_options.add_argument("--disable-features=EnableEphemeralFlashPermission")
            chrome_options.add_argument("--disable-background-networking")
            chrome_options.add_argument("--disable-sync")
            chrome_options.add_argument("--disable-translate")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")

            service = Service(executable_path=driver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info('WebDriver set up successfully.')
            
        except Exception as e:
            logger.error('An error occurred while setting up the driver: %s', e)
            self.driver = None
        
    def load_page(self):
        if self.driver is None:
            logger.debug('Driver not initialized.')
            self.set_up_driver()
            
        if self.driver is not None:
            self.driver.get(self.url)
            time.sleep(2)
            logger.info('Page successfully loaded.')
        else:
            logger.error('Driver setup failed, cannot load page.')
            
    def scroll_to_bottom(self):
        """This method scrolls to the bottom of the page to ensure all offers are loaded. 
        For further explanation please visit https://python-forum.io/thread-20175.html.
        """
        if self.driver is not None:
            try: 
                SCROLL_PAUSE_TIME = 2 
                last_height = self.driver.execute_script('return document.body.scrollHeight')
        
                while True:
                    self.driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
                    time.sleep(SCROLL_PAUSE_TIME)
                    new_height = self.driver.execute_script('return document.body.scrollHeight')
                    if new_height == last_height:
                        break
                    last_height = new_height
            
                self.full_content = self.driver.page_source
                self.driver.quit()
                logger.info('The content of the page was successfully gathered.')
            except Exception as e:
                logger.error('An error occured while gathering the page content.')
        
    def find_elements(self):
        """This method creates a BeautifulSoup object and parses the HTML-data."""
        if self.full_content is not None:
            self.soup = BeautifulSoup(self.full_content, 'html.parser')
            self.offers = self.soup.find_all('li', class_='OfferList__OfferListItem-sc-bj82vg-1')
            logger.info('The OfferList class was successfully parsed.')
        if not self.offers:
            logger.error('The OfferList could not be found.')
    
    def extract_data(self):
        """This method extracts relevant data from the offers and appends them to the data attribute."""
        if self.offers is not None:
            for offer in self.offers:
                try:
                    name = offer.find('header', class_='OfferList___StyledHeader-sc-bj82vg-11').get_text(strip=True)
                    price = offer.find('span', class_='OfferList___StyledSpan2-sc-bj82vg-14').get_text(strip=True)
                    details = offer.find('div', class_='OfferList__OfferPcs-sc-bj82vg-7').get_text(strip=True)
                    store = offer.find('meta', itemprop='name')['content']
                    valid_from = offer.find('meta', itemprop='validFrom')['content']
                    valid_through = offer.find('meta', itemprop='validThrough')['content']
                    valid_until = offer.find('meta', itemprop='priceValidUntil')['content']

                    self.data.append({
                        'Name': name,
                        'Price': price,
                        'Details': details,
                        'Store': store,
                        'ValidFrom': valid_from,
                        'ValidThrough': valid_through,
                        'ValidUntil': valid_until
                    })
                except AttributeError as e:
                    logger.error('An error occured while extracting data from an offer: %s', e)  
            print(f"Number of offers found: {len(self.offers)}") 
            return self.data  
        else:
            logger.error("An error occured while trying to extract the data. No offers found.")
                
    #def save_to_csv(self):
        #"""This method saves the extracted data to a CSV-file."""
        #if self.data:
            #date_today = datetime.now().strftime('%Y-%m-%d')
            #filename = f'offers_{date_today}.csv'
            #df = pd.DataFrame(self.data)
            #df.to_csv(filename, index=False)
            #logger.info('Data saved to %s', {filename})
            #print(f"Data saved to {filename}")
        #else:
            #print("There is no data to be saved.")
        
    def scrape(self):
        self.load_page()
        self.scroll_to_bottom()
        self.find_elements()
        self.extract_data()
        return self.data
        
