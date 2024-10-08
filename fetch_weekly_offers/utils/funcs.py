"""This module provides functions for logging and saving data to SQL."""

import logging
import pandas as pd
from sqlalchemy import create_engine

# Function to configure logging 
def set_up_logging():
    logging.basicConfig(filename='logging.log', 
                        level=logging.INFO, 
                        filemode='a', 
                        format='[%(asctime)s][%(name)s] - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    
logger = logging.getLogger()

# Function to save scraped and cleaned data to SQL database "WeeklyOffers"
def save_to_sql(df: pd.DataFrame, db_name='WeeklyOffers', table_name='offers', server='MSI') -> None:
    """Saves the cleaned DataFrame to a MSSQL database."""
    try:
        engine = create_engine('mssql+pyodbc://MSI/WeeklyOffers?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')
        logger.info('Connection to database was successful.')
    except Exception as e: 
        logger.critical('Critical: %s. Could not connect to database.', e)
        return
    
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logger.info('Data saved to %s in %s successfully.', db_name, table_name)
        print('Data was successfully saved to WeeklyOffers db.')
    except Exception as e: 
        logger.error('%s: Failed to save data to database.', e)
        
        
    



        