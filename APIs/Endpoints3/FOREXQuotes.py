

from fastapi import HTTPException, FastAPI, Response, status, APIRouter
from fastapi.responses import JSONResponse

import pandas as pd

from config.db import get_database 
from APIs.Endpoints1.companies_APIs import get_company_symbols 

from APIs.Endpoints3.FOREX import FOREX_IndexesCollection 


import os
import asyncio
import aiohttp  
import time
import datetime

FOREX_Quotes = APIRouter()
api_key = os.getenv("API_KEY")




def get_FOREX_Quotes_collection():
    db = get_database()
    FOREX_Quotes=db["FOREX_Quotes"]
    FOREX_Quotes.create_index([("_id", 1)])
    return FOREX_Quotes

FOREX_QuotesCollection=get_FOREX_Quotes_collection()








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



def get_FOREXQUOTES_symbols():
    try:
        all_FOREXQuotes = FOREX_IndexesCollection.find({}, { "symbol": 1 })
        
        if not all_FOREXQuotes:
            return set()  
        
        symbols = {str(company.get("symbol")) for company in all_FOREXQuotes}  
        
        return symbols
    except Exception as e:
        raise HTTPException(status_code=400, detail="Une erreur s'est produite lors de la récupération des données.")







async def FOREX_Quotes_Creation(symbol, dataframe):
    print("ForexIndex with symbol '", symbol, "' made FOREX Quotes API call ")
    creation_Order=False
    start_date = "1950-01-01"


    # Getting today's date
    current_date = datetime.datetime.now()
    formatted_todayDate = current_date.strftime("%Y-%m-%d")

    symbol_to_check = symbol

    # Check if we have already treated the company before in the quotes or not
    symbolDate_if_Exists_in_the_DataFrame = get_date_for_symbol(dataframe, symbol_to_check)
    print("The extracted date with the symbol ", symbolDate_if_Exists_in_the_DataFrame)

    if symbolDate_if_Exists_in_the_DataFrame != formatted_todayDate:
        if(symbolDate_if_Exists_in_the_DataFrame==None):
            print(f"-->>{symbol_to_check} does not exist in the csv and we are going to add FOREXquote for the first time  ")
            creation_Order=True

        else:
            print(f"-->>{symbol_to_check} exists in the csv and we are going to add new FOREX quote and update the last date in the csv ")
            start_date = symbolDate_if_Exists_in_the_DataFrame
            creation_Order=True
    else:
        print(f"-->>{symbol_to_check}  exist in the csv and it's up to date ")

    # The end_date is always today's date
    end_date = formatted_todayDate

    # print("start_date ", start_date)
    # print("end_date ", end_date)

    api_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?from={start_date}&to={end_date}&apikey={api_key}"
    #print("The api url ", api_url)
    
    if creation_Order==True:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as response:
                data = await response.json()
                # Check if data is not empty before accessing its elements
                if data and len(data.get("historical", [])) > 0:
                    for obj in data.get("historical", []):
                        obj["symbol"] = symbol
                        
                    FOREX_QuotesCollection.insert_many(data["historical"])

                    Symbol_Date_FOREXQuotes_CSV_FileName = "FOREXQuotes_CSV_fle/FOREXQuotes_CSV_fle.csv"
                    update_csv_with_symbol_and_date(Symbol_Date_FOREXQuotes_CSV_FileName, symbol, formatted_todayDate)

                    print(f"The compnay ' {symbol}' has Quotes data inserted into the database and updating the CSV quotes file ")
                else:
                    print(f"No data returned for symbol {symbol}")




def get_FOREX_Quotes_symbols():
    print("The get FOREX quotes symbols ")




@FOREX_Quotes.get('/FOREX_Quotes_Creation_API')
async def Insert_FOREX_Quotes_Creation_API():

    all_FOREX_Quotes_Symobls = get_FOREX_Quotes_symbols()
    #all_FOREX_Quotes_SymoblsList = list(all_FOREX_Quotes_Symobls)

    all_FOREX_Quotes_SymoblsList=["EURUSD","EURTND"]

    print("Quotes indexes symbols ")
    print(get_FOREXQUOTES_symbols())

    if all_FOREX_Quotes_Symobls is not None:
        print(len(all_FOREX_Quotes_Symobls))
    else:
        print("all_FOREX_Quotes_Symobls is None, check your initialization logic.")

        

    #Reading the quotes csv file that contains the symbol and the date information of the companies 
    csv_file_path = 'FOREXQuotes_CSV_fle/FOREXQuotes_CSV_fle.csv'

    SymbolDateQuotesDF = pd.read_csv(csv_file_path)

    batch_size = 10  
    
    results = []
    
    for i in range(0, 30, batch_size):
        symbols_batch = all_FOREX_Quotes_SymoblsList[i:i + batch_size]
        awaitable_tasks = [FOREX_Quotes_Creation(symbol, SymbolDateQuotesDF) for symbol in symbols_batch]
        batch_results = await asyncio.gather(*awaitable_tasks)
        results.extend(batch_results)
    
    return {"message": "FOREX Quotes creation process is complete"}



from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio

async def run_forex_quotes_creation():
    print("The scheduled task call")
    result = await Insert_FOREX_Quotes_Creation_API()
    print(result)

scheduler = AsyncIOScheduler()

scheduler.add_job(run_forex_quotes_creation, trigger='interval', seconds=10, max_instances=1)

scheduler.start()

