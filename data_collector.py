import pandas as pd
import polygon
from tqdm import tqdm
from sec import stock, constants, processor
import asyncio
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
constants.set_polygon_key("4cR_irLDgivxae1WO4y0Wb30VYxXRkQj")
stocks_client = polygon.StocksClient('4cR_irLDgivxae1WO4y0Wb30VYxXRkQj')
reference_client = polygon.ReferenceClient('4cR_irLDgivxae1WO4y0Wb30VYxXRkQj')
# processor.download_sec_data()

async def download_fundamental_data():
    # Get all the stocks
    companies = pd.read_csv("data/companies.csv", header=0)
    dataset = pd.DataFrame(columns=['Symbol', 'Date', 'EBITDA', 'Revenue', 'Operating Margin', 'ROA', 'Debt-to-Equity', 'FCF', 'P/E Ratio', 'IPO Price', '1D Price', '1W Price', '1M Price', '3M Price', '1Y Price'])

    for index, row in tqdm(companies.iterrows(), total=companies.shape[0]):
        symbol = row['Symbol']
        date = row['Date']
        query_date = (pd.to_datetime(date) + pd.DateOffset(days=120)).strftime('%Y-%m-%d')
        try: 
            processor.process_sec_json(symbol)
            s = stock.Stock(symbol)
            
            try:
                open_price = stocks_client.get_daily_open_close(symbol, date)['open']
            except:
                open_price = None

            try:
                one_day_price = await s.get_price(query_date=(pd.to_datetime(date) + pd.DateOffset(days=1)).strftime('%Y-%m-%d'))
            except:
                one_day_price = None

            try:
                one_week_price = await s.get_price(query_date=(pd.to_datetime(date) + pd.DateOffset(weeks=1)).strftime('%Y-%m-%d'))
            except:
                one_week_price = None

            try:
                one_month_price = await s.get_price(query_date=(pd.to_datetime(date) + pd.DateOffset(months=1)).strftime('%Y-%m-%d'))
            except:
                one_month_price = None

            try:
                three_month_price = await s.get_price(query_date=(pd.to_datetime(date) + pd.DateOffset(months=3)).strftime('%Y-%m-%d'))
            except:
                three_month_price = None

            try:
                one_year_price = await s.get_price(query_date=(pd.to_datetime(date) + pd.DateOffset(years=1)).strftime('%Y-%m-%d'))
            except:
                one_year_price = None

            try:
                ebidta = s.get_ebitda(query_date=query_date, quarterly=True)
            except:
                ebidta = None

            try: 
                revenue = s.get_revenue(query_date=query_date, quarterly=True)
            except:
                revenue = None

            try:
                ebit = s.get_ebit(query_date=query_date, quarterly=True)
            except:
                ebit = None

            try:
                net_income = s.get_net_income(query_date=query_date, quarterly=True)
            except:
                net_income = None

            try:
                total_assets = s.get_total_assets(query_date=query_date, quarterly=True)
            except:
                total_assets = None

            try:
                total_debt = s.get_total_debt(query_date=query_date, quarterly=True)
            except:
                total_debt = None

            try:
                stockholders_equity = s.get_stockholders_equity(query_date=query_date, quarterly=True)
            except:
                stockholders_equity = None
                
            try:
                operating_cash_flow = s.get_operating_cash_flow(query_date=query_date, quarterly=True)
            except:
                operating_cash_flow = None

            try:
                capex = s.get_capex(query_date=query_date, quarterly=True)
            except:
                capex = None

            try:
                pe_ratio = await s.get_pe_ratio(query_date=query_date, quarterly=True)
            except:
                pe_ratio = None

            try:
                operating_margin = ebit / revenue
            except:
                operating_margin = None

            try:
                roa = net_income / total_assets
            except:
                roa = None

            try:
                debt_to_equity = total_debt / stockholders_equity
            except:
                debt_to_equity = None

            try:
                fcf = operating_cash_flow - capex
            except:
                fcf = None

            dataset = dataset.append({
                'Symbol': symbol,
                'Date': date,
                'EBITDA': ebidta,
                'Revenue': revenue,
                'Operating Margin': operating_margin,
                'ROA': roa,
                'Debt-to-Equity': debt_to_equity,
                'FCF': fcf,
                'P/E Ratio': pe_ratio,
                'IPO Price': open_price,
                '1D Price': one_day_price,
                '1W Price': one_week_price,
                '1M Price': one_month_price,
                '3M Price': three_month_price,
                '1Y Price': one_year_price
            }, ignore_index=True)

        except:
            try:
                open_price = stocks_client.get_daily_open_close(symbol, date)['open']
            except:
                open_price = None

            try:
                for i in range(0, 3):
                    result = stocks_client.get_daily_open_close(symbol, (pd.to_datetime(date) + pd.DateOffset(days=(1 + i))).strftime('%Y-%m-%d'))
                    if result['status'] == 'OK':
                        one_day_price = result['close']
                        break
                    elif i == 2:
                        one_day_price = None
            except:
                one_day_price = None

            try:
                for i in range(0, 3):
                    result = stocks_client.get_daily_open_close(symbol, (pd.to_datetime(date) + pd.DateOffset(weeks=1) + pd.DateOffset(days=i)).strftime('%Y-%m-%d'))
                    if result['status'] == 'OK':
                        one_week_price = result['close']
                        break
                    elif i == 2:
                        one_week_price = None
            except:
                one_week_price = None
            
            try:
                for i in range(0, 3):
                    result = stocks_client.get_daily_open_close(symbol, (pd.to_datetime(date) + pd.DateOffset(months=1) + pd.DateOffset(days=i)).strftime('%Y-%m-%d'))
                    if result['status'] == 'OK':
                        one_month_price = result['close']
                        break
                    elif i == 2:
                        one_month_price = None

            except:
                one_month_price = None

            try:
                for i in range(0, 3):
                    result = stocks_client.get_daily_open_close(symbol, (pd.to_datetime(date) + pd.DateOffset(months=3) + pd.DateOffset(days=i)).strftime('%Y-%m-%d'))
                    if result['status'] == 'OK':
                        three_month_price = result['close']
                        break
                    elif i == 2:
                        three_month_price = None
            except:
                three_month_price = None
            
            try:
                for i in range(0, 3):
                    result = stocks_client.get_daily_open_close(symbol, (pd.to_datetime(date) + pd.DateOffset(years=1) + pd.DateOffset(days=i)).strftime('%Y-%m-%d'))
                    if result['status'] == 'OK':
                        one_year_price = result['close']
                        break
                    elif i == 2:
                        one_year_price = None
            except:
                one_year_price = None
            
            dataset = dataset.append({
                'Symbol': symbol,
                'Date': date,
                'EBITDA': None,
                'Revenue': None,
                'Operating Margin': None,
                'ROA': None,
                'Debt-to-Equity': None,
                'FCF': None,
                'P/E Ratio': None,
                'IPO Price': open_price,
                '1D Price': one_day_price,
                '1W Price': one_week_price,
                '1M Price': one_month_price,
                '3M Price': three_month_price,
                '1Y Price': one_year_price
            }, ignore_index=True)
            
            # print(f"Failed to download data for {symbol}")
            
    dataset.to_csv("data/fundamental_data.csv", index=False)

def download_sentiment_data():
    # Get all the stocks
    companies = pd.read_csv("data/companies.csv", header=0)
    sentiment_data = pd.DataFrame(columns=['Symbol', 'Sentiment Score'])
    for index, row in tqdm(companies.iterrows(), total=companies.shape[0]):
        symbol = row['Symbol']
        date = row['Date']
        end_date = (pd.to_datetime(date) + pd.DateOffset(months=3)).strftime('%Y-%m-%d')
        start_date = (pd.to_datetime(date) - pd.DateOffset(months=6)).strftime('%Y-%m-%d')
        sentiment_score = 0
        try:
            news = reference_client.get_ticker_news(symbol, published_utc_lte=end_date, published_utc_gte=start_date)['results']
            if len(news) == 0:
                sentiment_score = None
            for n in news:
                try:
                    insights = n['insights']
                    for i in insights:
                        if i['ticker'] == symbol:
                            if i['sentiment'] == 'positive' or i['sentiment'] == 'bullish':
                                sentiment_score += 1
                            elif i['sentiment'] == 'negative' or i['sentiment'] == 'bearish':
                                sentiment_score -= 1
                except:
                    continue

            sentiment_data = sentiment_data.append({
                'Symbol': symbol,
                'Sentiment Score': sentiment_score
            }, ignore_index=True)
        except:
            #print(f"Failed to download sentiment for {symbol}")
            sentiment_data = sentiment_data.append({
                'Symbol': symbol,
                'Sentiment Score': None
            }, ignore_index=True)

    sentiment_data.to_csv(f"data/sentiment_data.csv", index=False)

asyncio.run(download_fundamental_data())

download_sentiment_data()
