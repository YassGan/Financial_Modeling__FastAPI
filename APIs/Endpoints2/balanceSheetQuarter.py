from fastapi import HTTPException, FastAPI, Response, status, APIRouter
from fastapi.responses import JSONResponse



import os
import asyncio
import aiohttp  
import time
import datetime




from APIs.Endpoints1.companiesFiltering import CompaniesCollection 
from config.db import get_database 



BS_Quarter = APIRouter()
api_key = os.getenv("API_KEY")






def get_companies_Quarter_BalanceSheet():
    db = get_database()
    QuarterBalanceSheet=db["Quarter_Balance_Sheet"]

    return QuarterBalanceSheet


companies_Quarter_BalanceSheetCollection=get_companies_Quarter_BalanceSheet()



















# Function that runs the online api that returns the balance sheet information
async def CompanyQuarterBalanceSheetInfo(symbol):
    print("Company with symbol", symbol, "made balance sheet API call pf quarter information")
    api_url = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}?period=quarter&apikey={api_key}"

    async with aiohttp.ClientSession() as session:
        async with session.get(api_url) as response:
            data = await response.json()
    return data




#API that launches the function CompanyBalanceSheetInfo
@BS_Quarter.get('/getQuarterBalanceSheet_company/{Symbol}')
async def create_BS_Quarter_api(Symbol: str):
    result = await CompanyQuarterBalanceSheetInfo(Symbol)
    return result










# Function that gets the information of the balance sheet of the company and it compares the data with 
# the data in the database and the date data also if it exists and the date is older that the new one
# it adds the new information in the database otherwise it does nothing, if there is no data it creates a new
# element
async def CompanyQuarterBalanceSheetInfoComparisonInsertion(symbol):
    print("Company with symbol", symbol, "made balance sheet API quarter call and is comparing the API data with the data in the database")
    
    api_url = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}?period=quarter&apikey={api_key}"

    async with aiohttp.ClientSession() as session:
        async with session.get(api_url) as response:
            data = await response.json()
            print(data[0]['symbol'])

            existing_data = companies_Quarter_BalanceSheetCollection.find_one({"symbol": data[0]['symbol']})
            
            if existing_data:
                api_date = datetime.datetime.strptime(data[0]['date'], '%Y-%m-%d')
                db_date = datetime.datetime.strptime(existing_data['date'], '%Y-%m-%d')
                
                if api_date > db_date:
                    data[0]['symbol'] = data[0]['symbol']
                    companies_Quarter_BalanceSheetCollection.replace_one({"symbol": data[0]['symbol']}, data[0])
                    
                    data[0]['symbol'] = data[0]['symbol']+"_"+db_date.strftime("%Y-%m-%d")
                    data[0]['date'] = db_date.strftime("%Y-%m-%d")


                    companies_Quarter_BalanceSheetCollection.insert_one(data[0])

                    
                    
                    
                    print(f"Updated {symbol} balance sheet data in the database.")
                else:
                    print(f"{symbol} balance sheet data in the database is up to date.")
            else:
                data[0]['symbol'] = data[0]['symbol']
                companies_Quarter_BalanceSheetCollection.insert_one(data[0])
                print(f"Inserted {symbol} balance sheet data into the database.")
            
            # Check if '_id' field exists in data dictionary before converting it to a string
            if '_id' in data[0]:
                data[0]['_id'] = str(data[0]['_id'])

    return JSONResponse(content=data[0])  






# API that launches the function CompanyBalanceSheetInfoComparisonInsertion
@BS_Quarter.get('/getQuarterBalanceSheet_company_comparison_insertion/{Symbol}')
async def create_BS_Annual_api(Symbol: str):
    result = await CompanyQuarterBalanceSheetInfoComparisonInsertion(Symbol)
    return result


# API that launches the function CompanyBalanceSheetInfoComparisonInsertion and use it with many symbols
@BS_Quarter.get('/Insert_Quarter_BalanceSheet_information_Comparison')
async def Insert_BS_Annual_information():

    dataframe = ['AAPL', 'LMNR','SU','LRCX']

    awaitable_tasks = [CompanyQuarterBalanceSheetInfoComparisonInsertion(symbol) for symbol in dataframe]

    results = await asyncio.gather(*awaitable_tasks)

    return results



