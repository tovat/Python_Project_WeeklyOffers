import logging
import pandas as pd
import glob
import os
from funcs import set_up_logging

set_up_logging()
logger = logging.getLogger(__name__)

class DataLoader:
    """This class provides functionality to load CSV data gathered by the webscraper
    and concatenate them into a DataFrame, preparing for data cleaning."""
    
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.data = None
        
    def gather_csv_files(self):
        """This method gathers all CSV files saved by the scraper."""
        try:
            # Search for all CSV files starting with 'offers_' 
            csv_files = glob.glob(os.path.join(self.folder_path, 'offers_*.csv'))
            logger.info("A total of %d CSV files were gathered successfully.", len(csv_files))
            return csv_files
        except Exception as e:
            logger.error("No CSV files were found.")
    
    def load_data_to_df(self) -> pd.DataFrame:
        """This method loads data from all CSV files and concatenates them into a DataFrame."""
        csv_files = self.gather_csv_files()
        
        if csv_files:
            df_list = []
            for file in csv_files:
                print(f"Loading file: {file}")
                df = pd.read_csv(file)
                if not df.empty:
                    df_list.append(df)
                else:
                    logger.warning("%s is empty", file)
                    
            if df_list:
                self.data = pd.concat(df_list, ignore_index=True)
                logger.info("Data loaded and concatenated successfully.")
                self.df = self.data
                return self.df 
            else:
                logger.error("No data to load.")
                return pd.DataFrame()       
        else:
            logger.error("No CSV files to load.")
            return pd.DataFrame()
            
if __name__ == "__main__":
    # Define the folder where CSV files are stored (use '.' for the current directory)
    folder_path = '.'
    
    data = DataLoader(folder_path)
    data.load_data_to_df()