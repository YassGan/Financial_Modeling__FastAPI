

from fastapi import HTTPException, FastAPI, Response, status, APIRouter
from fastapi.responses import JSONResponse

import pandas as pd

from schemas.Sector import serializeList2

from pymongo import MongoClient, UpdateOne

from APIs.Endpoints2.Quotes import QuotesCollection

from config.db import get_database
from APIs.Endpoints1.companies_APIs import get_company_symbols

from APIs.Endpoints3.FOREX import FOREX_IndexesCollection

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


        #making the update by adding the marketcap
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


                        filter_query = {"symbol": symbol, "date": date}
                        update_query = {"$set": {
                            "numberOfShares": numberOfShares,
                            "CashAndCashEquivalents": CashAndCashEquivalents,
                            "TotalDebt": TotalDebt,
                            "enterpriseValue": enterpriseValue,
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


                            filter_query = {"symbol": symbol, "date": date}
                            update_query = {"$set": {
                                "numberOfShares": numberOfShares,
                                "CashAndCashEquivalents": CashAndCashEquivalents,
                                "TotalDebt": TotalDebt,
                                "enterpriseValue": enterpriseValue,
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





































#### The magic solution that updates quotes with marketCap values and EV with a way more performant logic that uses batches
# from typing import List

# @Quotes_update.get("/v1/update_quotes_marketCap_EV")
# async def update_market_cap_ev():
#     total_updates = 0
#     successful_updates = 0
#     error_updates = 0
#     no_change_updates = 0

#     # Getting today's date
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

#     # Storing the data of marketCap and EV of all the symbols
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
#                 # Fetch marketCap data
#                 response_marketCap.raise_for_status()
#                 marketCap_result = response_marketCap.json()
#                 marketCap_results.append({"symbol": symbol, "data": marketCap_result})

#                 # Fetch EV data
#                 response_EV.raise_for_status()
#                 EV_result = response_EV.json()
#                 EV_results.append({"symbol": symbol, "data": EV_result})

#             except httpx.HTTPError as e:
#                 marketCap_results.append({"symbol": symbol, "error_marketCap": str(e)})
#                 EV_results.append({"symbol": symbol, "error_EV": str(e)})

#     all_quotes = serializeList2(QuotesCollection.find({}))

#     # Batch processing for marketCap updates
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

#     # Batch processing for EV updates
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
#                                 "enterpriseValue": enterpriseValue,
#                             }
#                         }

#                         ev_updates.append((filter_query, update_query))

#                         # Print progress
#                         print(
#                             f"EV updating : symbol {symbol} date{date}  Processed {total_updates} updates - {successful_updates} successful, {no_change_updates} no change, {error_updates} errors"
#                         )
#                         update_csv_with_symbol_and_date("Quotes_CSV_file/Quotes_marketCap_EV_update.csv", symbol, formatted_todayDate)
#                         break

#                 if not found:
#                     # No exact match, find nearest date
#                     nearest_date = find_nearest_date(date, [data["date"] for data in symbol_data["data"]])
#                     for data_element in symbol_data["data"]:
#                         if data_element["date"] == nearest_date:
#                             # Perform update in the database
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

#                             # Print progress
#                             print(
#                                 f"EV updating : symbol {symbol} date{date}  Processed {total_updates} updates - {successful_updates} successful, {no_change_updates} no change, {error_updates} errors"
#                             )
#                             update_csv_with_symbol_and_date("Quotes_CSV_file/Quotes_marketCap_EV_update.csv", symbol, formatted_todayDate)
#                             break

#     # Execute batch updates for marketCap and EV
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





















