"""Test the functions of the DataCleaner class.
The tests are made both with data generated by the Scraper (real data) and manipulated data (invalid data).
"""

import pandas as pd
from cleaner import DataCleaner
from datetime import date
import pytest

# General class containing test methods for DataCleaner
class GeneralDataCleanerTests:
           
    def test_convert_to_df(self, data_cleaner):
        """Test the conversion of raw data to a pandas DataFrame."""
        df = data_cleaner.convert_to_df()
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
    
    def test_remove_duplicates(self, data_cleaner):
        """Test the removal of duplicate rows."""
        df = data_cleaner.convert_to_df()
        try: 
            new_df = data_cleaner.remove_duplicates()
            assert len(new_df) != len(df)
        except Exception as e:
            pytest.fail(f'Failed to remove duplicate rows: {e}')
        
    def test_clean_prices(self, data_cleaner):
        """Test the cleaning and conversion of price data."""
        data_cleaner.convert_to_df()
        try: 
            df = data_cleaner.clean_prices()
            assert df['Price'].dtype == 'float64'
        except Exception as e:
            pytest.fail(f'Failed to clean price data: {e}')
        
    def test_clean_details(self, data_cleaner):
        data_cleaner.convert_to_df()
        try: 
            df = data_cleaner.clean_details()
            assert 'Quantity' in df.columns
            assert 'ComparisonPrice' in df.columns
        except Exception as e:
            pytest.fail(f'Failed to split details column: {e}')
        
    def test_clean_datetime(self, data_cleaner):
        data_cleaner.convert_to_df()
        try:
            df = data_cleaner.clean_datetime()
            assert isinstance(df['ValidFrom'].iloc[0], (date, type(None)))
            assert isinstance(df['ValidThrough'].iloc[0], (date, type(None)))
            assert isinstance(df['ValidUntil'].iloc[0], (date, type(None)))
        except Exception as e:
            pytest.fail(f'Failed to convert to date objects: {e}')

# Class for testing with real data
class TestRealData(GeneralDataCleanerTests):
    
    def setup_method(self):
        """Instantiating DataCleaner with real data generated by the Scraper."""
        raw_data = pd.read_csv('tests/offers_2024-09-24.csv')
        self.test_data = DataCleaner(raw_data.to_dict(orient='records'))
        
    def test_convert_to_df(self):
        super().test_convert_to_df(self.test_data)

    def test_remove_duplicates(self):
        super().test_remove_duplicates(self.test_data)

    def test_clean_prices(self):
        super().test_clean_prices(self.test_data)

    def test_clean_details(self):
        super().test_clean_details(self.test_data)

    def test_clean_datetime(self):
        super().test_clean_datetime(self.test_data)
        

# Class for testing with invalid data
class TestInvalidData(GeneralDataCleanerTests):
    
    def setup_method(self):
        """Instantiating DataCleaner with invalid data."""
        invalid_data = [
            {'Name': 'K-orv', 'Price': '25SEK', 'Details': '1.•100', 'Store': 'ICA', 'ValidFrom': '20', 'ValidThrough': '2024-09-22', 'ValidUntil': '2024-09-25'},
            {'Name': 'K-orv', 'Price': '25SEK', 'Details': '11•00', 'Store': 'ICA', 'ValidFrom': '20', 'ValidThrough': '2024-09-22', 'ValidUntil': '2024-09-25'},
            {'Name': 123, 'Price': '-', 'Details': '2•200', 'Store': '+', 'ValidFrom': '2024-09-22 18:00:00', 'ValidThrough': '20', 'ValidUntil': '2024-X-26'},
            {'Name': 'Tändvätska', 'Price': '123,45', 'Details': '3•300', 'Store': 'Coop', 'ValidFrom': '2024-09-23', 'ValidThrough': '2024-09-24', 'ValidUntil': ''}
        ]
        self.test_data = DataCleaner(invalid_data)
    
    def test_convert_to_df(self):
        super().test_convert_to_df(self.test_data)

    def test_remove_duplicates(self):
        super().test_remove_duplicates(self.test_data)

    def test_clean_prices(self):
        super().test_clean_prices(self.test_data)

    def test_clean_details(self):
        super().test_clean_details(self.test_data)

    def test_clean_datetime(self):
        super().test_clean_datetime(self.test_data)