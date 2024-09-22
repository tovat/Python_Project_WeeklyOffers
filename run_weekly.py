from scraper import Scraper
from loader import DataLoader
from cleaner import DataCleaner
from funcs import set_up_logging, save_to_sql
import logging

set_up_logging()
logger = logging.getLogger(__name__)
    
if __name__ == "__main__":
    url = ['https://ereklamblad.se/ICA-Maxi-Stormarknad/erbjudanden', 
           'https://ereklamblad.se/Hemkop/erbjudanden',
           'https://ereklamblad.se/ICA-Supermarket/erbjudanden']
    
    # Step 1: Initialize and Run the Scraper
    scraper = Scraper(url)
    scraped_data = scraper.scrape()  # Scrape data

    if scraped_data:
        logger.info("Scraping successful. Proceeding to data cleaning.")
        
        # Step 2: Initialize DataCleaner with Scraped Data
        data_cleaner = DataCleaner(scraped_data)
        
        # Convert scraped data to DataFrame and apply cleaning
        cleaned_data = data_cleaner.clean()
        
        if not cleaned_data.empty:
            logger.info("Data cleaning completed. Proceeding to save data to SQL database.")
            
            # Step 3: Save Cleaned Data to SQL
            save_to_sql(cleaned_data)
        else:
            logger.error("No data to save after cleaning. Exiting.")
    else:
        logger.error("No data scraped, cannot proceed with cleaning or saving.")