import numpy as np
import pandas as pd
import scipy.stats as stats
from scipy.interpolate import interp1d
from datetime import datetime
import yfinance as yf
import requests
import time
import os

# *** Proxy pool class *** #

class ProxyPool:

    def __init__(self,proxy_list_path):
        """
        param: proxy_list_path: path to list of proxies downloaded form webshare.io
        """
        proxy_list = list(np.loadtxt(proxy_list_path,dtype=str))
        proxy_list = ['http://' + ':'.join(x.split(':')[2:]) + '@' + ':'.join(x.split(':')[:2]) for x in proxy_list]
        proxy_list = [{'http':x,'https':x} for x in proxy_list]

        self.proxy_list = proxy_list
        self.num_proxies = len(self.proxy_list)
        self.random_index = stats.randint(0,self.num_proxies).rvs

    def verify_ip_addresses(self,sleep_seconds=0.1,nmax=10):

        """
        Function to verify that IP address appears as those of proxies
        """
        url = 'https://api.ipify.org/'
        n = np.min([nmax,len(self.proxy_list)])

        for i in range(n):

            res = requests.get(url,proxies=self.proxy_list[i])
            print(res.text,flush=True)
            time.sleep(sleep_seconds)

        return(None)

    def remove_bad_proxies(self,sleep_seconds=0.1):
        """
        Function to remove non-working proxies from list
        """

        url = 'https://api.ipify.org/'
        working_proxies = []

        n_start = self.num_proxies

        for proxy in self.proxy_list:
            try:
                res = requests.get(url,proxies=proxy)
                working_proxies.append(proxy)
            except:
                pass
            time.sleep(sleep_seconds)

        self.proxy_list = working_proxies
        self.num_proxies = len(self.proxy_list)
        self.random_index = stats.randint(0,self.num_proxies).rvs

        n_remove  = n_start - self.num_proxies

        print(f'Removed {n_remove} / {n_start} proxies.',flush=True)

        return(None)


    def random_proxy(self):
        """
        Function to return a randomly-selected proxy from self.proxy_list
        """

        proxy = self.proxy_list[self.random_index()]

        return(proxy)

# *** Initial setup *** #

def create_folders():
    """
    Function to create directory structure for data scraped from company websites

    param: companies: list of company names
    """

    pwd = os.getcwd()

    folders_to_create = []

    folders_to_create.append(f'data/tickers/raw')
    folders_to_create.append(f'data/tickers/clean')

    for folder in folders_to_create:
        folderpath = os.path.join(pwd,folder)
        if not os.path.exists(folderpath):
            os.makedirs(folderpath,exist_ok=True)

    return(None)

# Helpter function to save list as .txt file
def save_list_as_txt(filepath,x):
    """
    param: filepath: output path of .txt file
    param: x: list to be saved
    """
    with open(filepath,'w') as f:
        for i in range(len(x)):
            f.write(f'{x[i]}\n')
        f.close()
    return(None)

# Helper function to interpolate missing data
def interpolate_missing(x,y):
    """
    Function to fill in NaN values in pandas series using linear interpolation

    param: x: pandas series of independent variable (must be complete)
    param: y: pandas series of dependent variable (can have missing values)
    returns: y_filled: pandas series of dependent variable with missing values filled in
    """
    x = pd.to_numeric(x,errors='coerce')
    y = pd.to_numeric(y,errors='coerce')

    if x.isna().sum() > 0:
        raise ValueError('Independent variable must be numeric and cannot include missing values. ')

    m = ~y.isna()
    f = interp1d(x[m],y[m])
    y_filled = f(x)
    return(y_filled)

# Functions to scrape data from FRED database

def get_risk_free_rate(api_key):
    """
    This function pulls data on the yield of 1-year U.S. treasury securities from the FRED
    economic database (url: https://fred.stlouisfed.org/series/DGS1). This yield can be used
    as a proxy for the risk-free rate of return on a 1-year investment.

    param: api_key: Alphanumeric key enabling access to FRED API
    """
    series_id = 'DGS1'
    url = f'https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json'
    res = requests.get(url)
    df = pd.DataFrame(res.json()['observations'])[['date','value']]
    df['date'] = pd.to_datetime(df['date'])
    df['value'] = interpolate_missing(df['date'],df['value'])/100
    df.rename(columns={'date':'Date','value':'Risk Free Rate'}).set_index('Date',drop=True)
    return(df)


# Functions to scrape data from Yahoo finance

def get_liquid_us_stocks(proxypool,sleep_seconds=0.25,count=250,failure_limit=5):

    """
    This function returns a list of U.S. stocks listed on NYSE or NASDAQ with
    an average trading volume of >1M and a share price of >$10.

    param: proxypool: pool of proxies to route requests through
    param: sleep_seconds: number of seconds to wait in between api queries
    param: count: number of tickers to display per page (won't work if too high)
    param: failure_limit: number of failed queries in a row required to cease further attempts
    """

    pwd = os.getcwd()
    date_str = pd.to_datetime('today').strftime('%Y-%m-%d')

    offset = 0
    numfailures = 0

    df_list = []

    while numfailures < failure_limit:

        url = f'https://finance.yahoo.com/screener/e571efd8-2e40-41be-8401-0aef2dcd52b3?offset={offset}&count={count}'

        headers = {'Accept':'*/*',
                   'Accept-Encoding':'gzip, deflate, br',
                   'Accept-Language':'en-US,en;q=0.9',
                   'Cookie':'tbla_id=2c5e5f61-f1a3-4a53-868c-57c8e1743841-tuct9f6b7ad; gpp=DBAA; gpp_sid=-1; PRF=t%3DAAPL%26newChartbetateaser%3D1; cmp=t=1691869618&j=0&u=1YNN; OTH=v=2&s=2&d=eyJraWQiOiIwMTY0MGY5MDNhMjRlMWMxZjA5N2ViZGEyZDA5YjE5NmM5ZGUzZWQ5IiwiYWxnIjoiUlMyNTYifQ.eyJjdSI6eyJndWlkIjoiQ0NRR1o3S0xNUUREWkFHRUwyTjRZTlZIREkiLCJwZXJzaXN0ZW50Ijp0cnVlLCJzaWQiOiJtQkp0MTN3S0RYb1QifX0.Lf3GtgtuZDrK9qmgYggXKVcl_5rfBO261dG_D6YrW4xn4Gc3MCqFf1kkE07kfkl7l8Cvk07xbynPwiR8N_AQpsgsbykQANvdDN_wf90rr6McRPiufrW6yj_o50-x2EXGVPqF4CtmHr2S6r2ZuCsqL3Bxt-xGULeSNNJl_NchU4o; T=af=JnRzPTE2OTE4Njk3NjQmcHM9YjRmVDltNUM5dmdrTm1BWVV5QzZXUS0t&d=bnMBeWFob28BZwFDQ1FHWjdLTE1RRERaQUdFTDJONFlOVkhESQFhYwFBQ1pWX1pncAFhbAFrZml0em1hdXJpY2U5OEBnbWFpbC5jb20Bc2MBbWJyX3JlZ2lzdHJhdGlvbgFmcwFvNFRPNi5CazEuSkUBenoBRUouMWtCQTdFAWEBUUFFAWxhdAFFSi4xa0IBbnUBMA--&kt=EAAbEObqjwDfI7_sUSBbUDO5w--~I&ku=FAAdKbYpKc3zzaiUddQaerX9Th3r.8nCAOXzF7aBh2NDMnsxJH2sWBIvf3i54y3mGjlh6oIYvDfmHMc1ps5hj1T.W9LDdSXey.ds0ldWxM0914dlCNuZYWUpNyVI_hxQd4cj42tVqlW6KhjwXMS6Ksdpx28xxzkGqr0D.FanYnjgF8-~E; F=d=kpKVk109vITHEQgm9x94ItPi0LxzJRWt10QIvXO2VwvHgv9BS42tuYBEQq8-; PH=l=en-US; Y=v=1&n=42n74e1mjl39f&l=enkfg2bd7gpe4m5dpukav9u5v6ex48cnbenlvdb5/o&p=n32vvvv00000000&r=1c6&intl=us; GUCS=AX6Rr5a_; GUC=AQEACAJk2SxlC0Ii_QTy&s=AQAAAKkI31bo&g=ZNfiUQ; A1=d=AQABBFMa_GICEO97UbGAtOHKcnv7xQ443sUFEgEACAIs2WQLZdxI0iMA_eMBAAcIUxr8Yg443sUID7qsVuLBXOCuEspSPevwugkBBwoB8Q&S=AQAAAhFJEqAJLFuOeTqmaZCSb9o; A3=d=AQABBFMa_GICEO97UbGAtOHKcnv7xQ443sUFEgEACAIs2WQLZdxI0iMA_eMBAAcIUxr8Yg443sUID7qsVuLBXOCuEspSPevwugkBBwoB8Q&S=AQAAAhFJEqAJLFuOeTqmaZCSb9o; A1S=d=AQABBFMa_GICEO97UbGAtOHKcnv7xQ443sUFEgEACAIs2WQLZdxI0iMA_eMBAAcIUxr8Yg443sUID7qsVuLBXOCuEspSPevwugkBBwoB8Q&S=AQAAAhFJEqAJLFuOeTqmaZCSb9o&j=US; gam_id=y-gRZrwmlG2uKRYsH9VXQtLugDfCrHZc2hbIE7nViKwjbQiM7h_w---A; check=true; mbox=PC#4b72921d-446d-3e95-884e-6c2059ad1f36.34_0#1755115090; mboxEdgeCluster=34; AMCVS_6B25357E519160E40A490D44%40AdobeOrg=1; AMCV_6B25357E519160E40A490D44%40AdobeOrg=-1124106680%7CMCIDTS%7C19582%7CMCMID%7C70387708431593500443497709643102152209%7CMCAAMLH-1692475089%7C7%7CMCAAMB-1692475089%7Cj8Odv6LonN4r3an7LhD3WZrU1bUpAkFkkiY1ncBR96t2PTI%7CMCOPTOUT-1691877489s%7CNONE%7CvVersion%7C5.2.0',
                   'Origin':'https://finance.yahoo.com',
                   'Referer':'https://finance.yahoo.com/screener/e571efd8-2e40-41be-8401-0aef2dcd52b3',
                   'Sec-Ch-Ua':"\"Not/A)Brand\";v=\"99\", \"Google Chrome\";v=\"115\", \"Chromium\";v=\"115\"",
                   'Sec-Ch-Ua-Mobile':'?0',
                   'Sec-Ch-Ua-Platform':"Windows",
                   'Sec-Fetch-Dest':'empty',
                   'Sec-Fetch-Mode':'cors',
                   'Sec-Fetch-Site':'same-site',
                   'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'}

        try:
            res = requests.get(url,headers=headers,proxies=proxypool.random_proxy())
        except:
            numfailures += 1

        if res.status_code == 200:

            page_data = pd.read_html(res.content)
            page_df = page_data[0]

            if len(page_df) == 0:
                numfailures += 1
            else:
                df_list.append(page_df)
                offset+=count
                numfailures = 0
        else:
            numfailures += 1

        time.sleep(sleep_seconds)

    df = pd.concat(df_list)

    ticker_symbols = list(np.sort(df['Symbol'].unique()))

    ticker_filepath = os.path.join(pwd,f'data/tickers/{date_str}_liquid_us_stocks.txt')
    save_list_as_txt(ticker_filepath,ticker_symbols)

    return(ticker_symbols)

def download_stock_data(ticker_symbols,proxypool,sleep_seconds=0.1,failure_limit=5):
    """
    Function to download stock price data from Yahoo finance.

    param: ticker_symbols: list of stock tickers to pull data for
    param: proxypool: pool of proxies to route api requests through
    param: sleep_seconds: number of seconds to wait in between api queries
    param: failure_limit: number of failed queries in a row required to cease further attempts
    """

    pwd = os.getcwd()
    n = len(ticker_symbols)

    for i,symbol in enumerate(ticker_symbols):

        print(f'{i+1} / {n} - {symbol}',flush=True)

        numfailures = 0
        keepgoing = True
        while (keepgoing==True) and (numfailures < failure_limit):
            try:
                df = yf.Ticker(symbol).history(period='max',proxy=proxypool.random_proxy())
                outname = os.path.join(pwd,f'data/tickers/raw/{symbol}.csv')
                df.to_csv(outname)
                keepgoing = False
            except:
                numfailures += 1

            time.sleep(sleep_seconds)

    return(None)

def update_stock_data(proxypool,max_days_since_update=7):
    """
    param: proxypool: pool of proxies to route api requests through
    param: max_days_since_update: update stock data if it's more than n days out of date
    """

    pwd = os.getcwd()
    date_today = pd.to_datetime('today')

    # Get most recent list of liquid U.S. stocks
    ticker_lists = [x for x in os.listdir(os.path.join(pwd,'data/tickers')) if 'liquid_us_stocks.txt' in x]
    ticker_lists.sort()
    latest_list = ticker_lists[-1]
    ticker_filepath = os.path.join(pwd,f'data/tickers/{latest_list}')
    ticker_symbols = np.genfromtxt(ticker_filepath,dtype=str)

    raw_dir = os.path.join(pwd,'data/tickers/raw')
    tickers_to_download = []

    # Check for stocks that have missing or old data
    for ticker in ticker_symbols:

        filepath = os.path.join(raw_dir,f'{ticker}.csv')

        if os.path.exists(filepath):
            days_since_modified = (date_today - pd.to_datetime(datetime.fromtimestamp(os.path.getmtime(filepath)))).days
            if days_since_modified > max_days_since_update:
                tickers_to_download.append(ticker)
        else:
            tickers_to_download.append(ticker)

    # Download most recent data
    download_stock_data(tickers_to_download,proxypool)

    return(None)

def clean_stock_data():
    """
    Function to consolidate raw stock data into single dataframe
    """

    pwd = os.getcwd()
    date_str = pd.to_datetime('today').strftime('%Y-%m-%d')

    # Get most recent list of liquid U.S. stocks
    ticker_lists = [x for x in os.listdir(os.path.join(pwd,'data/tickers')) if 'liquid_us_stocks.txt' in x]
    ticker_lists.sort()
    latest_list = ticker_lists[-1]
    ticker_filepath = os.path.join(pwd,f'data/tickers/{latest_list}')
    ticker_symbols = np.genfromtxt(ticker_filepath,dtype=str)

    raw_dir = os.path.join(pwd,'data/tickers/raw')

    df_list = []
    n = len(ticker_symbols)

    for i,ticker in enumerate(ticker_symbols):
        print(f'{i+1} / {n} - {ticker}',flush=True)
        
        ticker_filepath = os.path.join(raw_dir,f'{ticker}.csv')
        if os.path.exists(ticker_filepath):
            df = pd.read_csv(ticker_filepath)
            df['Symbol'] = ticker
            df['Date'] = pd.to_datetime(df['Date'].apply(lambda x: x.split(' ')[0]))
            df['Price'] = df['Close']
            df = df.resample('M',on='Date').agg({'Symbol':'last','Price':'last','Volume':'last'}).iloc[:-1,]
            df = df.reset_index()
            df['Period'] = df['Date'].dt.to_period('M')
            df['Monthly Log Return'] = np.concatenate(([np.nan],np.log(df['Price'].values[1:]/df['Price'].values[:-1])))
            df = df[['Symbol','Period','Date','Price','Volume','Monthly Log Return']]

            df_list.append(df)

    df = pd.concat(df_list).reset_index(drop=True)
    outname = os.path.join(pwd,f'data/tickers/clean/{date_str}_stock_data_clean.csv')
    df.to_csv(outname,index=False)

    return(df)
