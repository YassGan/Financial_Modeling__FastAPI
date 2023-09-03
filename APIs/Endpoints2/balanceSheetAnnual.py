from fastapi import HTTPException, FastAPI, Response, status, APIRouter
from fastapi.responses import JSONResponse



import os
import asyncio
import aiohttp  
import time
import datetime




from APIs.Endpoints1.companiesFiltering import CompaniesCollection 
from config.db import get_database 



BS_Annual = APIRouter()
api_key = os.getenv("API_KEY")






def get_companies_Annual_BalanceSheet():
    db = get_database()
    AnnualBalanceSheet=db["Annual_Balance_Sheet"]
    AnnualBalanceSheet.create_index([("symbol", 1)], unique=True)

    return AnnualBalanceSheet


companies_Annual_BalanceSheetCollection=get_companies_Annual_BalanceSheet()



















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










# Function that gets the information of the balance sheet of the company and it compares the data with 
# the data in the database and the date data also if it exists and the date is older that the new one
# it updates the information of the database otherwise it does nothing, if there is no data it creates a new
# element

async def CompanyBalanceSheetInfoComparisonInsertion(symbol):
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
                    print(f"Updated {symbol} balance sheet data in the database.")
                else:
                    print(f"{symbol} balance sheet data in the database is up to date.")
            else:
                data[0]['symbol'] = data[0]['symbol']
                companies_Annual_BalanceSheetCollection.insert_one(data[0])
                print(f"Inserted {symbol} balance sheet data into the database.")
            
            # Check if '_id' field exists in data dictionary before converting it to a string
            if '_id' in data[0]:
                data[0]['_id'] = str(data[0]['_id'])

    return JSONResponse(content=data[0])  





# API that launches the function CompanyBalanceSheetInfoComparisonInsertion
@BS_Annual.get('/getBalanceSheet_company_comparison_insertion/{Symbol}')
async def create_BS_Annual_api(Symbol: str):
    result = await CompanyBalanceSheetInfoComparisonInsertion(Symbol)
    return result


# API that launches the function CompanyBalanceSheetInfoComparisonInsertion and use it with many symbols
@BS_Annual.get('/Insert_BalanceSheet_information_Comparison')
async def Insert_BS_Annual_information():

    dataframe = ['AAPL', 'LMNR', 'SU', 'LRCX', 'PBTS','AMAT','PLUG','AX','MET','HIBB','BLCO','F','BUSE']

    awaitable_tasks = [CompanyBalanceSheetInfoComparisonInsertion(symbol) for symbol in dataframe]

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