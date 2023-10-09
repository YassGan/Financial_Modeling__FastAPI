from fastapi import HTTPException, FastAPI, Response, status, APIRouter
from fastapi.responses import JSONResponse

import pandas as pd


import os
import asyncio
import aiohttp  
import time
import datetime


from schemas.Sector import serializeList2




from APIs.Endpoints1.companiesFiltering import CompaniesCollection 
from config.db import get_database 

from APIs.Endpoints1.companies_APIs import get_company_symbols 

import requests


Financial_Info = APIRouter()
api_key = os.getenv("API_KEY")



def get_AvailableCurrencies_collection():
    db = get_database()
    Available_Currencies=db["Available_Currencies"]
    Available_Currencies.create_index([("_id", 1)])

    return Available_Currencies
AvailableCurrenciesCollection=get_AvailableCurrencies_collection()


def return_currencies_pairs():
    print("Making an api call to get the currencies pairs")
    api_url = f"https://financialmodelingprep.com/api/v3/symbol/available-forex-currency-pairs?apikey={api_key}"
    response = requests.get(api_url)
    return response.json()

# print("Returning the currencies pairs from the online api ")
# print (return_currencies_pairs())



FOREX=APIRouter()



@FOREX.get("/return_currencies_pairs_API")
async def return_currencies_pairs_API():
    currenciesPairs = return_currencies_pairs()
    
    if not currenciesPairs:
        raise HTTPException(status_code=404, detail="No companies found in the database")
    
    return currenciesPairs


def All_currencies_list(array_of_currencies_pairs):
    currency_names = [item["currency"] for item in array_of_currencies_pairs]
    return currency_names


@FOREX.get("/return_all_currencies_list_API")
async def return_all_currencies_list_API():
    return All_currencies_list(return_currencies_pairs())




@FOREX.get("/createAvailableCurrencies_API")
async def createAvailableCurrencies_API():
    currencies_CSV_FileName = "Currency_CSV_file/Currency_CSV_file.csv"
    
    try:
        df = pd.read_csv(currencies_CSV_FileName)
    except Exception as e:
        print(f"Error loading the .csv file: {e}")
        raise HTTPException(status_code=500, detail="Error loading .csv file")

    currencies_list = All_currencies_list(return_currencies_pairs())

    currency_objects = []

    # Iterate through the symbols list and match with DataFrame

    for Currency_Code in currencies_list:
        match = df[df['Currency_Code'] == Currency_Code]
        if not match.empty:
            Symbol = match.iloc[0]['Symbol']
            Currency_Full_Name = match.iloc[0]['Currency_Full_Name']

            
            # Check if the currency already exists in the database
            existing_currency = AvailableCurrenciesCollection.find_one({"Currency_Code": Currency_Code})
            
            if not existing_currency:
                currency_objects.append({
                    "Currency_Code": Currency_Code,
                    "Full_Name": Currency_Full_Name,
                    "Symbol": Symbol
                })

    # Insert only the currencies that don't already exist
    if currency_objects:
        AvailableCurrenciesCollection.insert_many(serializeList2(currency_objects))
        
    print(currency_objects)
    return currency_objects
    









