"""Main script that will run automatically every Sunday kl. 10:00 through Windows Task Scheduler."""

from fetch_weekly_offers.utils.scraper import Scraper
from fetch_weekly_offers.utils.cleaner import DataCleaner
from fetch_weekly_offers.utils.funcs import set_up_logging, logger, save_to_sql
import pandas as pd

set_up_logging()

if __name__ == '__main__':
    
    urls = ['https://ereklamblad.se/ICA-Maxi-Stormarknad/erbjudanden',
        'https://ereklamblad.se/Hemkop/erbjudanden',
        'https://ereklamblad.se/ICA-Supermarket/erbjudanden']
    
    all_scraped_data = []

    for url in urls:
        logger.info('Processing URL: %s', url)
        
        # Instantiate Scraper and scrape data for one URL at a time
        scraper = Scraper(url)
        scraped_data = scraper.scrape()

        if scraped_data:
            logger.info('Scraping was successful for URL: %s. Proceeding to data cleaning.', url)
            
            # Instantiate DataCleaner with the newly scraped data
            data_cleaner = DataCleaner(scraped_data)
            
            # Convert the scraped data to DataFrame and proceed with data cleaning
            cleaned_data = data_cleaner.clean()
            
            if not cleaned_data.empty:
                logger.info('Data cleaning completed for URL: %s. Proceeding to save data to SQL database.', url)
                
                # Save the cleaned data to SQL
                all_scraped_data.append(cleaned_data)
            else:
                logger.error('An error occured. No data to save after cleaning for URL: %s.', url)
        else:
            logger.error('An error occured. No data scraped for URL: %s.', url)
    
    if all_scraped_data:
        # Combine all cleaned data 
        combined_data = pd.concat(all_scraped_data, ignore_index=True)
        
        # Save data to SQL db
        logger.info('Saving combined data to SQL database.')
        save_to_sql(combined_data)
    else:
        logger.error('No data to save after processing all URLs.')
    

    
        
  
    