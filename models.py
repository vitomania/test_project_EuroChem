import re
import sys
import os
import datetime as dt
import pandas as pd
from typing import Dict

import requests
import yfinance as yf
from bs4 import BeautifulSoup


def check_input_params(start_date: str, end_date: str, loading_path: str, by_day: bool = False) -> None:
    """
    Check whether the input params have appropriate format or not.
    """
    try:
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d')
        end_date = dt.datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        raise Exception('Parameter start_date, end_date must be an ISO 8601 date (YYYY-MM-DD)')

    if start_date > end_date:
        raise Exception('start_date must be <= end_date')

    if re.search(r'\.csv$', loading_path) is None:
        raise Exception('Parameter loading_path must end with \'.csv\' (Example: your_path.csv)')

    if not isinstance(by_day, bool):
        raise TypeError('Parameter by_day must be boolean type')


class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


class CurrencyRate(object):
    """
    This class is used to upload currency rate by day/week.
    The class works according to ETL pipeline.

    Currency data was taken from: https://finance.yahoo.com/
    """
    def __init__(self, start_date: str, end_date: str, loading_path: str, by_day: bool = False):
        """

        :param start_date: given start date (format: ISO 8601 date (YYYY-MM-DD))
        :param end_date: given end date (format: ISO 8601 date (YYYY-MM-DD))
        :param loading_path: path to where data should be loaded (format: your_path.csv)
        :param by_day: if True then data will be loaded by day otherwise by week
        """

        check_input_params(start_date, end_date, loading_path, by_day)

        self.start_date = dt.datetime.strptime(start_date, '%Y-%m-%d') + dt.timedelta(days=1)
        self.end_date = dt.datetime.strptime(end_date, '%Y-%m-%d') + dt.timedelta(days=1)
        self.loading_path = loading_path
        self.by_day = by_day

        self.currency = ['TRY', 'MAD', 'INR', 'IDR', 'RUB', 'SAR', 'VES']

    def extract(self) -> pd.DataFrame:
        tickers = ','.join(cur + '=X' for cur in self.currency)

        with HiddenPrints():
            frame = yf.download(tickers, start=self.start_date, end=self.end_date, interval='1d', group_by='Ticker')

        frame = frame.stack(level=0).rename_axis(['Date', 'Symbol']).reset_index()

        return frame

    def transform(self, frame: pd.DataFrame) -> pd.DataFrame:
        frame.drop(columns=['Adj Close', 'Volume'], inplace=True)

        if frame.shape[0] == 0:
            if self.by_day:
                return frame
            else:
                return pd.DataFrame([], columns=['Symbol', 'Start Week', 'End Week', 'Open', 'Low', 'High', 'Close'])

        frame['Symbol'] = 'USD/' + frame['Symbol'].str.replace('=X', '')

        if self.by_day:
            return frame

        # transform to weekly data format
        frame['Week'] = frame['Date'].dt.isocalendar().week
        frame = frame.groupby(['Symbol', 'Week'], as_index=False).agg(
            Start_Week=pd.NamedAgg(column='Date', aggfunc='min'),
            End_Week=pd.NamedAgg(column='Date', aggfunc='max'),
            Open=pd.NamedAgg(column='Open', aggfunc='mean'),
            Low=pd.NamedAgg(column='Low', aggfunc='min'),
            High=pd.NamedAgg(column='High', aggfunc='max'),
            Close=pd.NamedAgg(column='Close', aggfunc='mean')
        )
        frame.drop(columns='Week', inplace=True)

        return frame

    def load(self, frame: pd.DataFrame) -> None:
        frame.to_csv(self.loading_path, index=False, date_format="%Y-%m-%d", float_format='%.2f')

    def run(self) -> None:
        frames = self.extract()
        frame = self.transform(frames)
        self.load(frame)


class AvgTemp(object):
    """
    This class is used to upload average temperature by day/week.
    The class works according to ETL pipeline and uses API.
    The temperature is shown in Celsius.

    API: https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation
    Database: https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database
    Support info: https://www.ncei.noaa.gov/data/global-summary-of-the-day/doc/
    Description of the result csv file: https://www.ncei.noaa.gov/data/global-summary-of-the-day/doc/readme.txt
    Countries abbreviation: https://www.ncei.noaa.gov/data/global-summary-of-the-day/doc/country-list.txt
    Airport information: * https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt
                         * https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv
    """
    def __init__(self, start_date: str, end_date: str, loading_path: str, by_day: bool = False):
        """

        :param start_date: given start date (format: ISO 8601 date (YYYY-MM-DD))
        :param end_date: given end date (format: ISO 8601 date (YYYY-MM-DD))
        :param loading_path: path to where data should be loaded (format: your_path.csv)
        :param by_day: if True then data will be loaded by day otherwise by week
        """

        check_input_params(start_date, end_date, loading_path, by_day)

        self.start_date = start_date
        self.end_date = end_date
        self.loading_path = loading_path
        self.by_day = by_day

        self.code_airport = {  # taken from Airport information
            '07149099999': 'LFPO',
            '17128099999': 'LTAC',
            '60135099999': 'GMME',
            '43003099999': 'VABB',
            '98429099999': 'RPLL'
        }

    def extract(self) -> pd.DataFrame:
        url = [
            'https://www.ncei.noaa.gov/access/services/data/v1?',
            'dataset=global-summary-of-the-day',
            'dataTypes=TEMP',
            'stations=' + ','.join(self.code_airport.keys()),
            'options=includeStationName:true',
            'startDate=' + self.start_date,
            'endDate=' + self.end_date
        ]

        url = '&'.join(url)
        frame = pd.read_csv(url,
                            parse_dates=['DATE'],
                            converters={'STATION': lambda x: self.code_airport.get(x, 'UNKNOWN')})

        return frame

    def transform(self, frame: pd.DataFrame) -> pd.DataFrame:
        frame.columns = frame.columns.str.title()

        if frame.shape[0] == 0:
            if self.by_day:
                return frame
            else:
                return pd.DataFrame([], columns=['Station', 'Name', 'Start_Week', 'End_Week', 'Temp'])

        frame['Temp'] = (frame['Temp'] - 32) * 5 / 9  # convert Fahrenheit to Celsius

        if self.by_day:
            return frame

        # transform to weekly data format
        frame['Week'] = frame['Date'].dt.isocalendar().week
        frame = frame.groupby(['Station', 'Name', 'Week'], as_index=False).agg(
            Start_Week=pd.NamedAgg(column='Date', aggfunc='min'),
            End_Week=pd.NamedAgg(column='Date', aggfunc='max'),
            Temp=pd.NamedAgg(column='Temp', aggfunc='mean')
        )
        frame.drop(columns='Week', inplace=True)

        return frame

    def load(self, frame: pd.DataFrame) -> None:
        frame.to_csv(self.loading_path, index=False, date_format="%Y-%m-%d", float_format='%.2f')

    def run(self) -> None:
        frames = self.extract()
        frame = self.transform(frames)
        self.load(frame)


class Balance(object):
    """
    This class is used to upload balance of payments by day/week.
    The class works according to ETL pipeline.

    Balance data was taken from: * http://www.cbr.ru/statistics/macro_itm/svs/
                                 * http://www.cbr.ru/vfs/statistics/credit_statistics/bop/
    """
    def __init__(self, start_year: int, end_year: int, loading_path: str):
        """

        :param start_year: given start year (must be integer)
        :param end_year: given end year (must be integer)
        :param loading_path: path to where data should be loaded (format: your_path.csv)
        """

        self.check_input_params(start_year, end_year, loading_path)

        self.start_year = start_year
        self.end_year = end_year
        self.loading_path = loading_path

    @staticmethod
    def check_input_params(start_year: int, end_year: int, loading_path: str) -> None:
        """
        Check whether the input params have appropriate format or not.
        """
        if not isinstance(start_year, int):
            raise TypeError('Parameter start_year must be integer type')

        if not isinstance(end_year, int):
            raise TypeError('Parameter end_year must be integer type')

        if start_year > end_year:
            raise Exception('start_year must be <= end_year')

        if re.search(r'\.csv$', loading_path) is None:
            raise Exception('Parameter loading_path must end with \'.csv\' (Example: your_path.csv)')

    def extract(self) -> Dict[str, pd.DataFrame]:
        response = requests.get('http://www.cbr.ru/vfs/statistics/credit_statistics/bop/')
        soup = BeautifulSoup(response.text, features="lxml")
        filenames = []
        for link in soup.findAll('a'):
            match = re.search(r'57-bop_.*\.xlsx$', r'{}'.format(link.get('href')))
            if match:
                filenames.append(match[0])

        frames = {}
        for year in range(self.start_year, self.end_year + 1):
            if year in [1992, 1993]:
                filename = f'57-bop_92-93.xlsx'
            else:
                filename = f'57-bop_{str(year)[-2:]}.xlsx'

            if filename not in filenames:
                continue

            url = f'http://www.cbr.ru/vfs/statistics/credit_statistics/bop/{filename}'
            frame = pd.read_excel(url, skiprows=3)
            frame.rename(columns={'Unnamed: 0': 'Parameter'}, inplace=True)
            frames[str(year)] = frame

        return frames

    @staticmethod
    def transform(frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        empty_frame = pd.DataFrame([], columns=['Parameter', 'Amount ($M)', 'Start_Quarter', 'End_Quarter'])
        if len(frames) == 0:
            return empty_frame

        for year, frame in frames.items():
            if frame.shape[0] == 0:
                continue

            mapping = {}
            for i, q in enumerate(['I квартал', 'II квартал', 'III квартал', 'IV квартал'], 1):
                mapping[q + f' {year} г.'] = i

            frame = frame.loc[:, frame.columns.isin(['Parameter'] + list(mapping.keys()))]

            if frame.shape[1] < 2:
                frames[year] = empty_frame
                continue

            mask = frame.isna().all(axis=1)
            if mask.any():
                frame = frame.iloc[:mask.argmax(), :]
            frame = frame.dropna(how='all', subset=frame.columns.drop('Parameter'))
            frame = frame.melt(id_vars=['Parameter'],
                               value_vars=frame.columns.drop('Parameter'),
                               var_name='Quarter',
                               value_name='Amount ($M)')

            frame['Quarter'] = frame['Quarter'].apply(lambda x: mapping[x])
            frames[year] = frame

        return pd.concat(frames.values())

    def load(self, frame: pd.DataFrame) -> None:
        frame.to_csv(self.loading_path, index=False, date_format="%Y-%m-%d", float_format='%.2f')

    def run(self) -> None:
        frames = self.extract()
        frame = self.transform(frames)
        self.load(frame)
