import requests
import numpy as np
import os
import pandas as pd
import re
import time
import json
import lxml
import dateutil

from FinanceAndMl_libs import finance_ml as fm
from pprint import pprint
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime, timedelta

# Settings
pd.set_option('max_rows', 1_000)
pd.set_option('max_columns', 100)
pd.set_option('display.width', 1_200)


def path_iterator(tickers: list, path: str, df: pd.DataFrame, cut_date: str) -> pd.DataFrame:
    count = 0
    for ticker in tqdm(tickers):
        print(ticker)
        cur_df = pd.read_csv(f"{path}\{ticker}.csv", parse_dates=['date'])
        cur_df = cur_df[cur_df['date'] >= cut_date]
        cur_df['div_ratio'] = cur_df['divCash'] / cur_df['close'].shift(1)
        cur_df = cur_df[(cur_df['div_ratio'] >= 0.02) & (cur_df['volume'] >= 10_000)]
        cur_df['ticker'] = ticker

        df = pd.concat([df, cur_df], ignore_index=True)
        if count == 100:
            df.to_csv(f"SpecDiv_onlyTiingo_{cut_date}.csv", index=False)
            count = 0
        count += 1

    return df


def make_file(cut_date: str):
    # Соберём все дивы из Tiingo, которые более 2%.

    path_main = r'E:\Биржа\Stocks. BigData\Цены\Дейли\tiingo'
    path_usa = r'E:\Биржа\Stocks. BigData\Цены\Дейли\tiingo\usa'

    ticker_main = [ticker.replace('.csv', '') for ticker in os.listdir(path_main) if '.csv' in ticker]
    ticker_usa = [ticker.replace('.csv', '') for ticker in os.listdir(path_usa) if '.csv' in ticker]
    ticker_usa = list(set(ticker_usa) - set(ticker_main))

    df_main = pd.DataFrame({})
    df_main = path_iterator(ticker_main, path_main, df_main, cut_date)
    df_main = path_iterator(ticker_usa, path_usa, df_main, cut_date)

    df_main.to_csv(f"SpecDiv_onlyTiingo_{cut_date}.csv", index=False)


if __name__ == "__main__":
    # dividend, Per Share, Distribution, Spin-Off
    make_file('2022-04-01')

    # tickers_error = []
    # path_usa = r'E:\Биржа\Stocks. BigData\Цены\Дейли\tiingo\usa'
    # ticker_usa = [ticker.replace('.csv', '') for ticker in os.listdir(path_usa) if '.csv' in ticker]
    # for ticker in tqdm(ticker_usa):
    #     try:
    #         cur_df = pd.read_csv(f"{path_usa}\{ticker}.csv", parse_dates=['date'])
    #     except UnicodeDecodeError:
    #         tickers_error.append(ticker)
    #
    # fm.download_tickers(tickers_error, threads=20)
