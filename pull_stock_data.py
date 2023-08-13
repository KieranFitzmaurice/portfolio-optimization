import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
import financialmodels as fm
import os

pwd = os.getcwd()
date_str = pd.to_datetime('today').strftime('%Y-%m-%d')

# Create folders for output
fm.create_folders()

# Initialize proxy pool
proxy_list_path = os.path.join(pwd,'proxies','proxy_list.txt')
proxypool = fm.ProxyPool(proxy_list_path)
proxypool.remove_bad_proxies()

# Get list of U.S. stocks with good liquidity
ticker_symbols = fm.get_liquid_us_stocks(proxypool)

# Pull latest stock price data from Yahoo finance
fm.update_stock_data(proxypool)
