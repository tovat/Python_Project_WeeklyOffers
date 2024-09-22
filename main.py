from scraper import Scraper
from loader import DataLoader
from cleaner import DataCleaner
from funcs import set_up_logging, save_to_sql
import logging
import pandas as pd

set_up_logging()
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    
    urls = ['https://ereklamblad.se/ICA-Maxi-Stormarknad/erbjudanden',
        'https://ereklamblad.se/Hemkop/erbjudanden',
        'https://ereklamblad.se/ICA-Supermarket/erbjudanden']
    
    all_scraped_data = []

    for url in urls:
        logger.info("Processing URL: %s", url)
        
        # Step 1: Initialize and Run the Scraper
        scraper = Scraper(url)  # Pass a single URL as a list
        scraped_data = scraper.scrape()  # Scrape data from the URL

        if scraped_data:
            logger.info("Scraping successful for URL: %s. Proceeding to data cleaning.", url)
            
            # Step 2: Initialize DataCleaner with Scraped Data
            data_cleaner = DataCleaner(scraped_data)
            
            # Convert scraped data to DataFrame and apply cleaning
            cleaned_data = data_cleaner.clean()
            
            if not cleaned_data.empty:
                logger.info("Data cleaning completed for URL: %s. Proceeding to save data to SQL database.", url)
                
                # Step 3: Save Cleaned Data to SQL
                all_scraped_data.append(cleaned_data)
            else:
                logger.error("No data to save after cleaning for URL: %s. Skipping.", url)
        else:
            logger.error("No data scraped for URL: %s. Skipping.", url)
    
    if all_scraped_data:
        # Combine all cleaned data into a single DataFrame
        combined_data = pd.concat(all_scraped_data, ignore_index=True)
        
        # Save combined data to SQL
        logger.info("Saving combined data to SQL database.")
        save_to_sql(combined_data)
    else:
        logger.error("No data to save after processing all URLs.")