

from fastapi import HTTPException, FastAPI, Response, status, APIRouter
from fastapi.responses import JSONResponse
from datetime import date

import pandas as pd

from schemas.Sector import serializeList2

from schemas.Sector import serializeDict2


from pymongo import MongoClient, UpdateOne

from APIs.Endpoints2.Quotes import QuotesCollection

from config.db import get_database
from APIs.Endpoints1.companies_APIs import get_company_symbols

from APIs.Endpoints3.FOREX import FOREX_IndexesCollection
from APIs.Endpoints4.stock_indexes_Quotes import STOCKIndexes_QuotesCollection


from typing import List


import os
import asyncio
import aiohttp
import time
import datetime

Quotes_update = APIRouter()
api_key = os.getenv("API_KEY")

import httpx
import pymongo
import pymongo.errors as errors
import numpy as np







def get_date_for_symbol(dataframe, symbol):
    # print("working with this dataframe")
    # print(dataframe)
    print("Treating the symbol  ", symbol)
    filtered_df = dataframe[dataframe['symbol'] == symbol]

    if not filtered_df.empty:
        return filtered_df['date'].iloc[0]
    else:
        return None




def update_csv_with_symbol_and_date(csv_url, symbol, date):
    try:
        df = pd.read_csv(csv_url)
    except Exception as e:
        print(f"Error loading the .csv file: {e}")
        return

    if symbol in df['symbol'].values:
        df.loc[df['symbol'] == symbol, 'date'] = date
    else:
        new_entry = {'symbol': symbol, 'date': date}
        new_df = pd.DataFrame([new_entry]) 
        df = pd.concat([df, new_df], ignore_index=True)  

    try:
        df.to_csv(csv_url, index=False)
        print("CSV file updated successfully.")
    except Exception as e:
        print(f"Error saving the .csv file: {e}")





def find_nearest_date(date, dates):
    return min(dates, key=lambda d: abs(datetime.datetime.strptime(d, "%Y-%m-%d") - datetime.datetime.strptime(date, "%Y-%m-%d")))




def compare_dates(csv_date_str, date_to_compare_str):
    csv_date_format = "%Y-%m-%d"
    date_format = csv_date_format

    csv_date = csv_date_str
    date_to_compare = date_to_compare_str

    if csv_date is None or date_to_compare is None:
        print("One or both dates are missing.")
        return 1  

    if csv_date < date_to_compare:
        print(csv_date, " is in the past.")
        return -1
    elif csv_date == date_to_compare:
        print(csv_date, " is today.")
        return 1
    else:
        print(csv_date, " is in the future.")
        return 1







### Statistics Functions
def get_stock_data(symbol, target_date):
    # Calculate the start date as 10 years before the target date
    start_date = (pd.to_datetime(target_date) - pd.DateOffset(years=10)).strftime('%Y-%m-%d')

    # Query the data for the date range
    query = {"symbol": symbol, "date": {"$gte": start_date, "$lte": target_date}}
    results = STOCKIndexes_QuotesCollection.find(query)

    if results:
        data_list = list(results)

        df = pd.DataFrame(data_list)

        columns_to_extract = ["date", "adjClose"]
        df = df[columns_to_extract]
        print(df)
        return df
    else:
        print(f"No data found for {symbol} in the date range from {start_date} to {target_date}")
        return None




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








@Quotes_update.get("/v1/testerFonctionStatistiques")
async def testerFonctionsStatistiques():
    # statitcs = construct_statistics("ENGIY", current_date="2023-11-10")
    statitcs = construct_statistics("LYFT", current_date="2023-11-10")

    print(statitcs)
    return serializeDict2(statitcs)











#### My endpoint for updating quotes with marketCap and EV values
@Quotes_update.get("/v1/update_quotes_marketCap_EV")
async def update_market_cap_ev():
    total_updates = 0  
    successful_updates = 0
    error_updates=0
    no_change_updates=0


    # Getting today's date
    current_date = datetime.datetime.now()
    formatted_todayDate = current_date.strftime("%Y-%m-%d")



    csv_file_path = 'Quotes_CSV_file/Quotes_marketCap_EV_update.csv'
    SymbolDateQuotesDF = pd.read_csv(csv_file_path)



    pipeline = [
        {"$group": {"_id": "$symbol"}},
        {"$project": {"_id": 0, "symbol": "$_id"}}
    ]
    quotes_symbols_list = list(QuotesCollection.aggregate(pipeline))
    symbols_list = [item['symbol'] for item in quotes_symbols_list]

    print("--- List of all the symbols ")
    print(symbols_list)

    # Storing the data of marketCap and EV of all the symbols
    marketCap_results = []
    EV_results = []

    async with httpx.AsyncClient() as client:
        for symbol in symbols_list:
            marketCapUrl = f"https://financialmodelingprep.com/api/v3/historical-market-capitalization/{symbol}?apikey={api_key}"
            EV_resultsUrl = f"https://financialmodelingprep.com/api/v3/enterprise-values/{symbol}/?period=quarter&apikey=96051dba5181978c2f0ce23c1ef4014b"

            try:
                # Fetch marketCap data
                response_marketCap = await client.get(marketCapUrl)
                response_marketCap.raise_for_status()
                marketCap_result = response_marketCap.json()
                marketCap_results.append({ "symbol":symbol,"data": marketCap_result})

                # Fetch EV data
                response_EV = await client.get(EV_resultsUrl)
                response_EV.raise_for_status()
                EV_result = response_EV.json()
                EV_results.append({ "symbol":symbol,"data": EV_result})


            except httpx.HTTPError as e:
                marketCap_results.append({"symbol": symbol, "error_marketCap": str(e)})
                EV_results.append({"symbol": symbol, "error_EV": str(e)})


        all__Quotes=serializeList2(QuotesCollection.find({}))
        # return(all__Quotes)


        ########making the update by adding the marketcap
        for marketCap_element in marketCap_results:
            for marketCap_element_date in marketCap_element["data"]:
                date = marketCap_element_date["date"]
                if compare_dates(get_date_for_symbol(SymbolDateQuotesDF,marketCap_element["symbol"]),date)==1:
                    symbol = marketCap_element_date["symbol"]
                    marketCapitalization = marketCap_element_date["marketCap"]

                    filter_query = {"symbol": symbol, "date": date}
                    update_query = {"$set": {"marketCap": marketCapitalization}}

                    try:
                        result = QuotesCollection.update_one(filter_query, update_query)
                        total_updates += 1
                        if result.modified_count > 0:
                            successful_updates += 1
                            status = "Success"
                            message = f"MarketCap Update successful for {symbol} on {date}"
                        else:
                            no_change_updates += 1
                            status = "NoChange"
                            message = f"MarketCap No changes for {symbol} on {date}"
                    except errors.PyMongoError as e:
                        error_updates += 1
                        status = "Error"
                        message = f"MarketCap Error updating document: {e}"

                    ## Print progress
                    print(f"MarketCap updating : symbol {symbol} date{date}  Processed {total_updates} updates - {successful_updates} successful, {no_change_updates} no change, {error_updates} errors")




##Making the update with the EV
    for element in all__Quotes:
        symbol = element["symbol"]
        date = element["date"]

        found = False

        for symbol_data in EV_results:
            if symbol_data["symbol"] == symbol:
                for data_element in symbol_data["data"]:
                    if (data_element["date"] == date) and compare_dates(get_date_for_symbol(SymbolDateQuotesDF,symbol),date)==1:
                        found = True
                        print(symbol, " and the date is ",date)

                        numberOfShares = data_element.get("numberOfShares", None)
                        CashAndCashEquivalents = data_element.get("minusCashAndCashEquivalents", None)
                        TotalDebt = data_element.get("addTotalDebt", None)
                        enterpriseValue = data_element.get("enterpriseValue", None)

                        statistics = construct_statistics(symbol, current_date=date)
                        JsonValues=serializeDict2(statistics)



                        filter_query = {"symbol": symbol, "date": date}
                        update_query = {"$set": {
                            "numberOfShares": numberOfShares,
                            "CashAndCashEquivalents": CashAndCashEquivalents,
                            "TotalDebt": TotalDebt,
                            "enterpriseValue": enterpriseValue,

                            "max_price": JsonValues['maxPrice'],
                            "minPrice": JsonValues['minPrice'],

                            "averagePrice": JsonValues['averagePrice'],
                            "emAveragePrice": JsonValues['emAveragePrice'],

                            "return": JsonValues['return'],
                            "maxDrowDown": JsonValues['maxDrowDown'],

                            "drawUp": JsonValues['drawUp'],
                            "daysNoChangePercentage": JsonValues['daysNoChangePercentage'],

                            "daysUpPercentage": JsonValues['daysUpPercentage'],
                            "daysDownPercentage": JsonValues['daysDownPercentage'],
                


                            "dailyVol": JsonValues['dailyVol'],
                            "weeklyVol": JsonValues['weeklyVol'],

                            "monthlyVol": JsonValues['monthlyVol'],
                            "dailyEmaVol": JsonValues['dailyEmaVol'],

                            "weeklyEmaVol": JsonValues['weeklyEmaVol'],
                            "monthlyEmaVol": JsonValues['monthlyEmaVol']
                
                                                }}

                        try:
                            result = QuotesCollection.update_one(filter_query, update_query)
                            total_updates += 1
                            if result.modified_count > 0:
                                successful_updates += 1
                                status = "Success"
                                message = f"EV Update successful for {symbol} on {date}"
                            else:
                                no_change_updates += 1
                                status = "NoChange"
                                message = f"EV No changes for {symbol} on {date}"
                        except errors.PyMongoError as e:
                            error_updates += 1
                            status = "Error"
                            message = f"EV Error updating document: {e}"

                         ## Print progress
                        print(f"EV updating : symbol {symbol} date{date}  Processed {total_updates} updates - {successful_updates} successful, {no_change_updates} no change, {error_updates} errors")
                        update_csv_with_symbol_and_date("Quotes_CSV_file/Quotes_marketCap_EV_update.csv",symbol,formatted_todayDate)
                        break



                if not found:
                    # No exact match, find nearest date
                    nearest_date = find_nearest_date(date, [data["date"] for data in symbol_data["data"]])
                    for data_element in symbol_data["data"]:
                        if data_element["date"] == nearest_date:
                            # Perform update in the database
                            print(f"Updating database for symbol {symbol} on date {date} with data from symbol {symbol} on date {nearest_date}")


                            print(symbol, " and the date is ",date)

                            numberOfShares = data_element.get("numberOfShares", None)
                            CashAndCashEquivalents = data_element.get("minusCashAndCashEquivalents", None)
                            TotalDebt = data_element.get("addTotalDebt", None)
                            enterpriseValue = data_element.get("enterpriseValue", None)


         
                            statistics = construct_statistics(symbol, current_date=date)
                            JsonValues=serializeDict2(statistics)


                            filter_query = {"symbol": symbol, "date": date}
                            update_query = {"$set": {
                                "numberOfShares": numberOfShares,
                                "CashAndCashEquivalents": CashAndCashEquivalents,
                                "TotalDebt": TotalDebt,
                                "enterpriseValue": enterpriseValue,

                                "max_price": JsonValues['maxPrice'],
                                "minPrice": JsonValues['minPrice'],

                                "averagePrice": JsonValues['averagePrice'],
                                "emAveragePrice": JsonValues['emAveragePrice'],

                                "return": JsonValues['return'],
                                "maxDrowDown": JsonValues['maxDrowDown'],

                                "drawUp": JsonValues['drawUp'],
                                "daysNoChangePercentage": JsonValues['daysNoChangePercentage'],

                                "daysUpPercentage": JsonValues['daysUpPercentage'],
                                "daysDownPercentage": JsonValues['daysDownPercentage'],
                    


                                "dailyVol": JsonValues['dailyVol'],
                                "weeklyVol": JsonValues['weeklyVol'],

                                "monthlyVol": JsonValues['monthlyVol'],
                                "dailyEmaVol": JsonValues['dailyEmaVol'],

                                "weeklyEmaVol": JsonValues['weeklyEmaVol'],
                                "monthlyEmaVol": JsonValues['monthlyEmaVol']
                    
                                                    }}


                            try:
                                result = QuotesCollection.update_one(filter_query, update_query)
                                total_updates += 1
                                if result.modified_count > 0:
                                    successful_updates += 1
                                    status = "Success"
                                    message = f"EV Update successful for {symbol} on {date}"
                                else:
                                    no_change_updates += 1
                                    status = "NoChange"
                                    message = f"EV No changes for {symbol} on {date}"
                            except errors.PyMongoError as e:
                                error_updates += 1
                                status = "Error"
                                message = f"EV Error updating document: {e}"

                            ## Print progress
                            print(f"EV updating : symbol {symbol} date{date}  Processed {total_updates} updates - {successful_updates} successful, {no_change_updates} no change, {error_updates} errors")
                            update_csv_with_symbol_and_date("Quotes_CSV_file/Quotes_marketCap_EV_update.csv",symbol,formatted_todayDate)
                            break

    if not found:
        print(f"No matching data found for symbol {symbol} and date {date}")


    return("Updating the marketCap and EV process is finished")





































## #####  The magic solution that updates quotes with marketCap values and EV with a way more performant logic that uses batches
# from typing import List

# @Quotes_update.get("/v1/Magic_update_quotes_marketCap_EV")
# async def update_market_cap_ev():
#     total_updates = 0
#     successful_updates = 0
#     error_updates = 0
#     no_change_updates = 0

#    ### Getting today's date
#     current_date = datetime.datetime.now()
#     formatted_todayDate = current_date.strftime("%Y-%m-%d")

#     csv_file_path = 'Quotes_CSV_file/Quotes_marketCap_EV_update.csv'
#     SymbolDateQuotesDF = pd.read_csv(csv_file_path)

#     pipeline = [
#         {"$group": {"_id": "$symbol"}},
#         {"$project": {"_id": 0, "symbol": "$_id"}}
#     ]
#     quotes_symbols_list = list(QuotesCollection.aggregate(pipeline))
#     symbols_list = [item['symbol'] for item in quotes_symbols_list]

#     print("--- List of all the symbols ")
#     print(symbols_list)

#    ### Storing the data of marketCap and EV of all the symbols
#     marketCap_results = []
#     EV_results = []

#     async with httpx.AsyncClient() as client:
#         tasks = []
#         for symbol in symbols_list:
#             marketCapUrl = f"https://financialmodelingprep.com/api/v3/historical-market-capitalization/{symbol}?apikey={api_key}"
#             EV_resultsUrl = f"https://financialmodelingprep.com/api/v3/enterprise-values/{symbol}/?period=quarter&apikey=96051dba5181978c2f0ce23c1ef4014b"

#             tasks.append(client.get(marketCapUrl))
#             tasks.append(client.get(EV_resultsUrl))

#         responses = await asyncio.gather(*tasks)

#         for i in range(0, len(responses), 2):
#             response_marketCap = responses[i]
#             response_EV = responses[i + 1]

#             symbol = symbols_list[i // 2]

#             try:
#                 ##Fetch marketCap data
#                 response_marketCap.raise_for_status()
#                 marketCap_result = response_marketCap.json()
#                 marketCap_results.append({"symbol": symbol, "data": marketCap_result})

#                 ##Fetch EV data
#                 response_EV.raise_for_status()
#                 EV_result = response_EV.json()
#                 EV_results.append({"symbol": symbol, "data": EV_result})

#             except httpx.HTTPError as e:
#                 marketCap_results.append({"symbol": symbol, "error_marketCap": str(e)})
#                 EV_results.append({"symbol": symbol, "error_EV": str(e)})

#     all_quotes = serializeList2(QuotesCollection.find({}))

#     ###Batch processing for marketCap updates
#     marketCap_updates = []
#     for marketCap_element in marketCap_results:
#         for marketCap_element_date in marketCap_element["data"]:
#             date = marketCap_element_date["date"]
#             if compare_dates(get_date_for_symbol(SymbolDateQuotesDF, marketCap_element["symbol"]), date) == 1:
#                 symbol = marketCap_element_date["symbol"]
#                 marketCapitalization = marketCap_element_date["marketCap"]

#                 filter_query = {"symbol": symbol, "date": date}
#                 update_query = {"$set": {"marketCap": marketCapitalization}}

#                 marketCap_updates.append((filter_query, update_query))

#    #### Batch processing for EV updates
#     ev_updates = []
#     for element in all_quotes:
#         symbol = element["symbol"]
#         date = element["date"]

#         found = False

#         for symbol_data in EV_results:
#             if symbol_data["symbol"] == symbol:
#                 for data_element in symbol_data["data"]:
#                     if (data_element["date"] == date) and compare_dates(
#                         get_date_for_symbol(SymbolDateQuotesDF, symbol), date
#                     ) == 1:
#                         found = True
#                         print(symbol, " and the date is ", date)
                        

#                         numberOfShares = data_element.get("numberOfShares", None)
#                         CashAndCashEquivalents = data_element.get("minusCashAndCashEquivalents", None)
#                         TotalDebt = data_element.get("addTotalDebt", None)
#                         enterpriseValue = data_element.get("enterpriseValue", None)

#                         filter_query = {"symbol": symbol, "date": date}
#                         update_query = {
#                             "$set": {
#                                 "numberOfShares": numberOfShares,
#                                 "CashAndCashEquivalents": CashAndCashEquivalents,
#                                 "TotalDebt": TotalDebt,
#                                 "enterpriseValue": enterpriseValue
#                             }
#                         }

#                         ev_updates.append((filter_query, update_query))

#                         ###Print progress
#                         print(
#                             f"EV updating : symbol {symbol} date{date}  Processed {total_updates} updates - {successful_updates} successful, {no_change_updates} no change, {error_updates} errors"
#                         )
#                         update_csv_with_symbol_and_date("Quotes_CSV_file/Quotes_marketCap_EV_update.csv", symbol, formatted_todayDate)
#                         break

#                 if not found:
#                     ###No exact match, find nearest date
#                     nearest_date = find_nearest_date(date, [data["date"] for data in symbol_data["data"]])
#                     for data_element in symbol_data["data"]:
#                         if data_element["date"] == nearest_date:
#                             ###Perform update in the database
#                             print(
#                                 f"Updating database for symbol {symbol} on date {date} with data from symbol {symbol} on date {nearest_date}"
#                             )

#                             print(symbol, " and the date is ", date)

                                        
                           

#                             numberOfShares = data_element.get("numberOfShares", None)
#                             CashAndCashEquivalents = data_element.get("minusCashAndCashEquivalents", None)
#                             TotalDebt = data_element.get("addTotalDebt", None)
#                             enterpriseValue = data_element.get("enterpriseValue", None)

#                             filter_query = {"symbol": symbol, "date": date}
#                             update_query = {
#                                 "$set": {
#                                     "numberOfShares": numberOfShares,
#                                     "CashAndCashEquivalents": CashAndCashEquivalents,
#                                     "TotalDebt": TotalDebt,
#                                     "enterpriseValue": enterpriseValue,
#                                 }
#                             }

#                             ev_updates.append((filter_query, update_query))

#                             ##Print progress
#                             print(
#                                 f"EV updating : symbol {symbol} date{date}  Processed {total_updates} updates - {successful_updates} successful, {no_change_updates} no change, {error_updates} errors"
#                             )
#                             update_csv_with_symbol_and_date("Quotes_CSV_file/Quotes_marketCap_EV_update.csv", symbol, formatted_todayDate)
#                             break

#     ##Execute batch updates for marketCap and EV
#     try:
#         marketCap_bulk_updates = [UpdateOne(update[0], update[1]) for update in marketCap_updates]
#         ev_bulk_updates = [UpdateOne(update[0], update[1]) for update in ev_updates]

#         if marketCap_bulk_updates:
#             result_marketCap = QuotesCollection.bulk_write(marketCap_bulk_updates)
#             total_updates += len(marketCap_bulk_updates)
#             successful_updates += result_marketCap.modified_count
#             no_change_updates += len(marketCap_bulk_updates) - result_marketCap.modified_count

#         if ev_bulk_updates:
#             result_ev = QuotesCollection.bulk_write(ev_bulk_updates)
#             total_updates += len(ev_bulk_updates)
#             successful_updates += result_ev.modified_count
#             no_change_updates += len(ev_bulk_updates) - result_ev.modified_count

#         print("Batch updates executed successfully.")

#     except errors.PyMongoError as e:
#         error_updates += 1
#         print(f"Error during batch updates: {e}")

#     return "Updating the marketCap and EV process is finished"





















