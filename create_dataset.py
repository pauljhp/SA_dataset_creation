import sqlite3
import pandas as pd
import numpy as np
import datetime as dt
from StockSentimentAnalysis import news_sentiment
from StockSentimentAnalysis.FinancialModelingPrep.indices import Index
from typing import Union, Optional
from argparse import ArgumentParser

sql_conn = sqlite3.connect('./data/spx_news_sentiment_price.db')
cursor = sql_conn.cursor()

START_DATE = dt.date(2020, 1, 1)

def main(index: str='SPX', 
    start_date: Union[dt.date, str]=START_DATE,
    print_every: int=10):
    ticker_list = Index.get_members(index)
    tickers = ticker_list.index.to_list()
    total = len(tickers)
    if isinstance(start_date, str):
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d').date()
    elif isinstance(start_date, dt.date):
        pass
    else:
        raise TypeError("start_date must be a datetime.date or a string")
    for i, ticker in enumerate(tickers):
        if i % print_every == 0:
            print(f"{i / total * 100:.2f}% done")
        price_news = news_sentiment.get_daily_sentiment_series(
            tickers[0], start_date=start_date)
        price_news.T.fillna(-1.).to_sql(ticker, sql_conn, if_exists='replace', index=True)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-i", "--index", type=str, default="SPX")
    parser.add_argument("-s", "--start_date", type=str, 
        default=START_DATE.strftime('%Y-%m-%d'))
    parser.add_argument("-p", "--print_every", type=int, default=10)
    
    args = parser.parse_args()

    main(**vars(args))