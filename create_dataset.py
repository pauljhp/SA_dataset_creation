import sqlite3
import pandas as pd
import numpy as np
import datetime as dt
from StockSentimentAnalysis import news_sentiment
from StockSentimentAnalysis.FinancialModelingPrep.indices import Index
from StockSentimentAnalysis.FinancialModelingPrep.tickers import Ticker
from typing import Union, Optional
from argparse import ArgumentParser
import logging

LOGPATH = './logs/create_dataset.log'
logging.basicConfig(filename=LOGPATH, level=logging.ERROR)

sql_conn_sent = sqlite3.connect('./data/spx_news_sentiment_price.db')
sql_conn_fund = sqlite3.connect('./data/spx_news_sentiment_fundamental.db')
sent_cur = sql_conn_sent.cursor()
fund_cur = sql_conn_fund.cursor()

START_DATE = dt.date(2017, 1, 1)

def main(index: str='SPX', 
    start_date: Union[dt.date, str]=START_DATE,
    print_every: int=10,
    verbose: bool=True,
    output: str='sql',
    ignore_error: bool=True,
    max_token_length: int=64,
    batch_size: int=256):
    """
    :param index: str
    :param start_date: Union[dt.date, str]
    :param print_every: int
    :param verbose: bool
    :param output: str, takes 'sql', 'csv'
    """
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
        try:
            if verbose: print(ticker)
            if i % print_every == 0:
                print(f"{i / total * 100:.2f}% done")
            price_news = news_sentiment.get_daily_sentiment_series(
                ticker, start_date=start_date, 
                max_token_len=max_token_length, 
                batch_size=batch_size)
            fundamentals = Ticker.download_financial_ratios(ticker).reset_index()
            fundamentals = fundamentals.merge(
                Ticker.list_financial_growth(ticker), on=['date', "period", "symbol"]
            )
            fundamentals = fundamentals.drop(["symbol", "period"], axis=1).set_index("date").astype(float)
            if output == 'sql':
                price_news.T.to_sql(ticker, sql_conn_sent, if_exists='replace', index=True)
                fundamentals.T.to_sql(ticker, sql_conn_fund, if_exists='replace', index=True)
            elif output == 'csv':
                price_news.T.to_csv(f'./data/news_price/{ticker}.csv')
                fundamentals.T.to_csv(f'./data/fundamentals/{ticker}.csv')
        except Exception as e:
            if ignore_error:
                logging.error(f"Error with {ticker}: {e}")
                pass
            else:
                raise e

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-i", "--index", type=str, default="SPX")
    parser.add_argument("-s", "--start_date", type=str, 
        default=START_DATE.strftime('%Y-%m-%d'))
    parser.add_argument("-p", "--print_every", type=int, default=10)
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-o", "--output", type=str, default="sql")
    parser.add_argument("-e", "--ignore_error", action="store_true")
    parser.add_argument("-l", "--max_token_length", type=int, default=64)
    parser.add_argument("-b", "--batch_size", type=int, default=256)
    
    args = parser.parse_args()

    main(**vars(args))