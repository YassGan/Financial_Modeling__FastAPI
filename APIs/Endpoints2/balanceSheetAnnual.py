from fastapi import HTTPException, FastAPI, Response, status, APIRouter
from fastapi.responses import JSONResponse

import pandas as pd


import os
import asyncio
import aiohttp  
import time
import datetime




from APIs.Endpoints1.companiesFiltering import CompaniesCollection 
from config.db import get_database 

from APIs.Endpoints1.companies_APIs import get_company_symbols 



BS_Annual = APIRouter()
api_key = os.getenv("API_KEY")






def get_companies_Annual_BalanceSheet():
    db = get_database()
    AnnualBalanceSheet=db["Annual_Balance_Sheet"]

    return AnnualBalanceSheet


companies_Annual_BalanceSheetCollection=get_companies_Annual_BalanceSheet()



#Function that gets the csv path of the file that contains companies symbols and the latest data of its balancesheet
def return_CompanyLatestBalanceSheet_Date_Symbol_Set(csv_file_path):
    try:
        df = pd.read_csv(csv_file_path)
        symbol_date_set = set()  
        for  row in df.iterrows():
            symbol_date_set.add((row['symbol'], row['date']))
        return symbol_date_set
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return set() 
    



def find_date_by_symbol(symbol, symbol_date_set):
    for s, date in symbol_date_set:
        if s == symbol:
            return date
        else:
            print("Pas de date existante dans le csv pour le symbol {symbol}")
            return None  





# the second approach for creating balance sheet 
async def BalanceSheetCreation(symbol,balanceSheet_URL,Tous_les_Symbols_DBlist_set,Symbol_Date_BalanceSheetDF_Set,DataBaseName):
    print("Company with symbol", symbol, "made balance sheet API call and is comparing the API data with the data in the database")
    
    # api_url = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}?apikey={api_key}"
    # https://financialmodelingprep.com/api/v3/balance-sheet-statement

    api_url = f"{balanceSheet_URL}/{symbol}?apikey={api_key}"
    # Tous_les_Symbols_list=get_company_symbols()
    # Tous_les_Symbols_list_set = set(Tous_les_Symbols_list)
    # travailler avec le set car c'est beaucoup plus pratique et rapide dans les tests de check d'existence d'un élément dans un set
    
    symbol_to_check = symbol
    if symbol_to_check in Tous_les_Symbols_DBlist_set:
        companyDataExistsinBalanceSheetDB=True
        print(f"{symbol_to_check} exists in the set of symbols.")
    else:
        print(f"{symbol_to_check} does not exist in the set of symbols.")
        companyDataExistsinBalanceSheetDB=False


    if companyDataExistsinBalanceSheetDB:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as response:
                    data = await response.json()
                    print(data[0]['symbol'])
                    api_date = datetime.datetime.strptime(data[0]['date'], '%Y-%m-%d')
                    company_csv_date_str=find_date_by_symbol(symbol,Symbol_Date_BalanceSheetDF_Set)
                    
                    company_csv_date = datetime.datetime.strptime(company_csv_date_str, '%Y-%m-%d')


                    if api_date > company_csv_date:
                        DataBaseName.replace_one({"symbol": data[0]['symbol']}, data[0])
                        
                        data[0]['symbol'] = data[0]['symbol']+"_"+company_csv_date.strftime("%Y-%m-%d")
                        data[0]['date'] = company_csv_date.strftime("%Y-%m-%d")

                        DataBaseName.insert_one(data[0])



                        print(f"Updated {symbol} balance sheet data in the database.")
                    else:
                        print(f"{symbol} balance sheet data in the database is up to date.")
    # else:
    #     async with aiohttp.ClientSession() as session:
    #         async with session.get(api_url) as response:
    #             data = await response.json()
    #             data[0]['symbol'] = data[0]['symbol']
    #             DataBaseName.insert_one(data[0])
    #             print(f"Inserted {symbol} balance sheet data into the database.")
                
                if '_id' in data[0]:
                    data[0]['_id'] = str(data[0]['_id'])

    return JSONResponse(content=data[0])  




@BS_Annual.get('/BalanceSheetCreationAPI')
async def Insert_BS_Annual_information():

    dataframe = ['AAPL', 'LMNR']

    companiesSymbolsSet=get_company_symbols()

    Symbol_Date_BalanceSheetDF_Set=return_CompanyLatestBalanceSheet_Date_Symbol_Set("company_symbol_date.csv")

    awaitable_tasks = [BalanceSheetCreation(symbol,'https://financialmodelingprep.com/api/v3/balance-sheet-statement',companiesSymbolsSet,Symbol_Date_BalanceSheetDF_Set,companies_Annual_BalanceSheetCollection) for symbol in dataframe]

    results = await asyncio.gather(*awaitable_tasks)

    return results










# Function that runs the online api that returns the balance sheet information
async def CompanyBalanceSheetInfo(symbol):
    print("Company with symbol", symbol, "made balance sheet API call")
    api_url = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}?apikey={api_key}"

    async with aiohttp.ClientSession() as session:
        async with session.get(api_url) as response:
            data = await response.json()
    return data




#API that launches the function CompanyBalanceSheetInfo
@BS_Annual.get('/getBalanceSheet_company/{Symbol}')
async def create_BS_Annual_api(Symbol: str):
    result = await CompanyBalanceSheetInfo(Symbol)
    return result
























# The first approch  of creating balancesheet
# Function that gets the information of the balance sheet of the company and it compares the data with 
# the data in the database and the date data also if it exists and the date is older that the new one
# it adds the new information in the database otherwise it does nothing, if there is no data it creates a new
# element
async def CompanyAnnualBalanceSheetInfoComparisonInsertion(symbol):
    print("Company with symbol", symbol, "made balance sheet API call and is comparing the API data with the data in the database")
    
    api_url = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}?apikey={api_key}"

    async with aiohttp.ClientSession() as session:
        async with session.get(api_url) as response:
            data = await response.json()
            print(data[0]['symbol'])

            existing_data = companies_Annual_BalanceSheetCollection.find_one({"symbol": data[0]['symbol']})
            
            if existing_data:
                api_date = datetime.datetime.strptime(data[0]['date'], '%Y-%m-%d')
                db_date = datetime.datetime.strptime(existing_data['date'], '%Y-%m-%d')
                
                if api_date > db_date:
                    data[0]['symbol'] = data[0]['symbol']
                    companies_Annual_BalanceSheetCollection.replace_one({"symbol": data[0]['symbol']}, data[0])
                    
                    data[0]['symbol'] = data[0]['symbol']+"_"+db_date.strftime("%Y-%m-%d")
                    data[0]['date'] = db_date.strftime("%Y-%m-%d")


                    companies_Annual_BalanceSheetCollection.insert_one(data[0])

                    print(f"Updated {symbol} balance sheet data in the database.")
                else:
                    print(f"{symbol} balance sheet data in the database is up to date.")
            else:
                data[0]['symbol'] = data[0]['symbol']
                companies_Annual_BalanceSheetCollection.insert_one(data[0])
                print(f"Inserted {symbol} balance sheet data into the database.")
            
            if '_id' in data[0]:
                data[0]['_id'] = str(data[0]['_id'])

    return JSONResponse(content=data[0])  






# API that launches the function CompanyBalanceSheetInfoComparisonInsertion
@BS_Annual.get('/getBalanceSheet_company_comparison_insertion/{Symbol}')
async def create_BS_Annual_api(Symbol: str):
    result = await CompanyAnnualBalanceSheetInfoComparisonInsertion(Symbol)
    return result


# API that launches the function CompanyBalanceSheetInfoComparisonInsertion and use it with many symbols
@BS_Annual.get('/Insert_Annual_BalanceSheet_information_Comparison')
async def Insert_BS_Annual_information():

    dataframe = ['AAPL', 'LMNR']

    awaitable_tasks = [CompanyAnnualBalanceSheetInfoComparisonInsertion(symbol) for symbol in dataframe]

    results = await asyncio.gather(*awaitable_tasks)

    return results
























###Another logic to insert the balance sheet annual data
# async def fetch_balance_sheet_data(symbol):
#     api_url = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}?apikey={api_key}"

#     async with aiohttp.ClientSession() as session:
#         async with session.get(api_url) as response:
#             if response.status != 200:
#                 raise HTTPException(status_code=response.status, detail="Failed to fetch data from API")

#             data = await response.json()
#             return data[0]

# async def update_or_insert_balance_sheet_data(data):
#     existing_data = companies_Annual_BalanceSheetCollection.find_one({"symbol": data['symbol']})

#     if existing_data:
#         api_date = datetime.datetime.strptime(data['date'], '%Y-%m-%d')
#         db_date = datetime.datetime.strptime(existing_data['date'], '%Y-%m-%d')

#         if api_date > db_date:
#             data['symbol'] = data['symbol']
#             companies_Annual_BalanceSheetCollection.replace_one({"symbol": data['symbol']}, data)
#             return f"Updated {data['symbol']} balance sheet data in the database."
#         else:
#             return f"{data['symbol']} balance sheet data in the database is up to date."
#     else:
#         data['symbol'] = data['symbol']
#         companies_Annual_BalanceSheetCollection.insert_one(data)
#         return f"Inserted {data['symbol']} balance sheet data into the database."

# @BS_Annual.get('/getBalanceSheet_company_comparison_insertion/{Symbol}')
# async def create_BS_Annual_api(Symbol: str):
#     try:
#         data = await fetch_balance_sheet_data(Symbol)
#         result = await update_or_insert_balance_sheet_data(data)
#         return JSONResponse(content=data)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @BS_Annual.get('/Insertion_BalanceSheet_information_Comparison')
# async def Insert_BS_Annual_information():
#     dataframe = ['AAPL', 'LMNR', 'SU', 'LRCX', 'PBTS', 'AMAT', 'PLUG', 'AX', 'MET', 'HIBB', 'BLCO', 'F', 'BUSE']

#     try:
#         awaitable_tasks = [fetch_balance_sheet_data(symbol) for symbol in dataframe]
#         results = await asyncio.gather(*awaitable_tasks)
#         response_messages = [await update_or_insert_balance_sheet_data(data) for data in results]
#         return JSONResponse(content={"results": response_messages})
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))