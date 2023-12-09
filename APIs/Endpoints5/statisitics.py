from datetime import date
import requests
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta

base_url = "https://financialmodelingprep.com/api/v3/historical-price-full"
API_KEY = "96051dba5181978c2f0ce23c1ef4014b"
from dateutil.relativedelta import relativedelta


# Read Data From API => To be converted to read from our database. The data is 10 years long for each date.

# For the initialization, consider storing the date in a variable because it is going to be used to avaoid multiple reads from the DB
def get_stock_data(Symbol, current_date):
    # current date is the date at which the analysis is made
    start_date = "1970-01-01"
    url = f"{base_url}/{Symbol}?from={start_date}&to={current_date}&apikey={API_KEY}"
    response = requests.get(url)
    quotes = response.json()
    df = pd.DataFrame.from_dict(quotes["historical"])
    columns_to_extract = ["date", "adjClose"]
    df = df[columns_to_extract]
    print(df)
    return df


DEFAULT_PERIODS = [0.5] + np.arange(1, 10, 1).tolist()



def construct_statistics(Symbol, current_date, analysis_periods=DEFAULT_PERIODS):
    df = get_stock_data(Symbol, current_date)

    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date", inplace=False)
    df = df.sort_values(by="date")
    last_date = df.index[-1]

    statitcs = {}


    metrics = ["maxPrice", "minPrice", "averagePrice", "emAveragePrice" ,"return", "maxDrowDown", "drawUp", "daysNoChangePercentage",
               "daysUpPercentage","daysDownPercentage","dailyVol", "weeklyVol",
               "monthlyVol", "dailyEmaVol", "weeklyEmaVol", "monthlyEmaVol"]

    for metric in metrics:
        statitcs[metric] = {}

    periods_name = [f"{period}y" for period in analysis_periods]
    periods_name.append("all")

    analysis_dates = []

    for period in analysis_periods:
        end_date = last_date - pd.DateOffset(months=period * 12)
        nearest_date = df.index[df.index <= end_date].max()
        analysis_dates.append(nearest_date)

    analysis_dates.append(df.index[0])

    for i, nearest_date in enumerate(analysis_dates):
        period_name = periods_name[i]

        if pd.notna(nearest_date):
            selected_prices = df.loc[nearest_date:last_date, 'adjClose']
            percentage_change = selected_prices.pct_change()
            cum_returns = (1 + percentage_change).cumprod()
            positive_returns = percentage_change.apply(lambda x: max(0, x))
            selected_prices_weekly = selected_prices.resample("W").last()
            selected_prices_monthly = selected_prices.resample("M").last()
            daily_log_returns = np.log(selected_prices / selected_prices.shift(1))
            weekly_log_returns = np.log(
                selected_prices_weekly / selected_prices_weekly.shift(1))
            monthly_log_returns = np.log(
                selected_prices_monthly / selected_prices_monthly.shift(1))

            daily_ema_volatility = daily_log_returns.ewm(span=10, min_periods=0, adjust=False).std().iloc[-1]
            weekly_ema_volatility = weekly_log_returns.ewm(span=10, min_periods=0, adjust=False).std().iloc[-1]
            monthly_ema_volatility = monthly_log_returns.ewm(span=10, min_periods=0, adjust=False).std().iloc[-1]
            statitcs["averagePrice"][period_name] = selected_prices.mean()
            statitcs["emAveragePrice"][period_name]=selected_prices.ewm(span=10, min_periods=0, adjust=False).mean().iloc[-1]
            statitcs["maxPrice"][period_name] = selected_prices.max()

            statitcs["minPrice"][period_name] = selected_prices.min()
            statitcs["return"][period_name] = selected_prices[-1] / selected_prices[0] - 1
            statitcs["maxDrowDown"][period_name] = (cum_returns / cum_returns.cummax() - 1).min()

            statitcs["drawUp"][period_name] = (1 + positive_returns).cumprod().iloc[-1] - 1
            statitcs["daysNoChangePercentage"][period_name] = (percentage_change == 0).sum() / len(percentage_change)
            statitcs["daysUpPercentage"][period_name] = (percentage_change > 0).sum() / len(percentage_change)
           
            statitcs["daysDownPercentage"][period_name] = (percentage_change < 0).sum() / len(percentage_change)
            statitcs["dailyVol"][period_name] = np.std(daily_log_returns) * np.sqrt(252)
            statitcs["weeklyVol"][period_name] = np.std(weekly_log_returns) * np.sqrt(52)
           
            statitcs["monthlyVol"][period_name] = np.std(monthly_log_returns) * np.sqrt(12)
            statitcs["dailyEmaVol"][period_name] = daily_ema_volatility * np.sqrt(252)
            statitcs["weeklyEmaVol"][period_name] = weekly_ema_volatility *  np.sqrt(52)
            statitcs["monthlyEmaVol"][period_name] = monthly_ema_volatility * np.sqrt(12)

        else:

            for metric in metrics:
                statitcs[metric][period_name] = np.nan

    return statitcs


statitcs = construct_statistics("^OVX", current_date="2023-11-08")

print(statitcs)
