"""Module providing a DataCleaner with functionality to clean and structure data gathered by the scraper."""

import pandas as pd
import re
from loader import DataLoader
import logging
from funcs import set_up_logging

# Set up logging 
set_up_logging()
logger = logging.getLogger(__name__)

class DataCleaner:
    """This class provides functionality to clean and structure data gathered 
    by the scraper, preparing it for data analysis and storage in a database.
    """
    
    def __init__(self, data: list):
        self.data = data
        if not data:
            logger.error("No data passed to DataCleaner.")
        else:
            logger.info(f"DataCleaner initialized with {len(data)} rows of data.")
        
    def convert_to_df(self) -> pd.DataFrame:
        """This method converts the scraped data to a pandas DataFrame."""
        if self.data:
            try: 
                self.df = pd.DataFrame(self.data)
                logger.info("Data successfully converted to a pandas DataFrame.")
                return self.df
            except Exception as e:
                logger.error("%s: Failed to convert data to DataFrame.", e)
                return pd.DataFrame() 
        else:
            logger.error("No data to convert into a DataFrame.")   
            return pd.DataFrame()        
        
    def remove_duplicates(self) -> pd.DataFrame:
        """This method removes duplicate rows."""
        if self.df is not None:
            try:
                initial_row_count = len(self.df)
                self.df = self.df.drop_duplicates()
                new_row_count = len(self.df)
                del_rows_count = initial_row_count - new_row_count
                logger.info("Duplicates removed, %d rows were dropped.", del_rows_count)
                return self.df
            except Exception as e:
                logger.error("%s: Failed to remove duplicates.", e)
                return self.df
        else:
            logger.error("No data loaded to remove duplicates from.")
            return pd.DataFrame()      
                       
    def clean_prices(self) -> pd.DataFrame:
        """This method cleans the price column and converts it to numeric."""
        if self.df is not None:
            try:
                self.df['Price'] = self.df['Price'].astype(str)
                self.df['Price'] = self.df['Price'].str.replace('kr', '')
                self.df['Price'] = self.df['Price'].str.replace(',', '.')
                self.df['Price'] = self.df['Price'].astype(float)
                logger.info("Price column cleaned and converted to numeric successfully.")
                return self.df
            except Exception as e:
                logger.error("%s: Failed to clean/convert price column.", e)
                return self.df
        else:
            logger.error("No data loaded to clean.")
            return pd.DataFrame()
            
    def clean_details(self) -> pd.DataFrame:
        """This method cleans and splits the details column into two new columns."""
        if self.df is not None:
            try: 
                self.df['Details'] = self.df['Details'].astype(str)
                self.df[['Quantity', 'ComparisonPrice']] = self.df['Details'].str.split('•', expand=True)
                self.df= self.df.drop('Details', axis=1)
                logger.info("Details column modified and cleaned successfully.")
                return self.df
            except Exception as e:
                logger.error("%s: Failed to clean/modify details column.", e)
                return self.df
        else:
            print("No data loaded to clean.")
            return pd.DataFrame()
            
    def clean_datetime(self) -> pd.DataFrame:
        """This method converts the date columns to date objects."""
        if self.df is not None:
            # Any invalid date values will be set to NaT (errors='coerce')
            try:
                self.df['ValidFrom'] = pd.to_datetime(self.df['ValidFrom'], errors='coerce').dt.date
                self.df['ValidThrough'] = pd.to_datetime(self.df['ValidThrough'], errors='coerce').dt.date
                self.df['ValidUntil'] = pd.to_datetime(self.df['ValidUntil'], errors='coerce').dt.date
                logger.info("Date columns converted and cleaned successfully.")
                return self.df
            except Exception as e:
                logger.error("%s: Failed to convert and clean date columns.", e)
                return self.df
        else:
            logger.error("No data to clean.")
            return pd.DataFrame()
            
    def clean(self) -> pd.DataFrame:
        """Run all cleaning steps."""
        if self.data:
            try: 
                self.convert_to_df()
                self.remove_duplicates()
                self.clean_prices()
                self.clean_details()
                self.clean_datetime()
                logger.info("Data cleaned successfully.")
                return self.df
            except Exception as e:
                logger.error("%s: Failed to clean data.", e)
                return pd.DataFrame()
        else:
            logger.error("No data to clean.")
            return pd.DataFrame()
    
    def save_clean_data(self, filename='cleaned_offers.csv') -> None:
        """This method saves the cleaned data to a CSV file. Optional for manual handling of data."""
        if self.df is not None:
            self.df.to_csv(filename, index=False)
            print(f"Cleaned data saved to {filename}")
        else:
            print("No data to save. Please load and clean the data first.")
            
        
#if __name__ == "__main__":
    # Define the folder where CSV files are stored (use '.' for the current directory)
    #folder_path = '.'
    #data_loader = DataLoader(folder_path)
    #data = data_loader.load_data_to_df()
    
    #data_cleaner = DataCleaner(data)
    #data_cleaner.clean()
    #data_cleaner.save_clean_data()

    
    
    

        


