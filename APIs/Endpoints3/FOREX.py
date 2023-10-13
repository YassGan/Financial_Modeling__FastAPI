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


def get_FOREXIndexes_collection():
    db = get_database()
    FOREX_Indexes=db["FOREX_Indexes"]
    FOREX_Indexes.create_index([("_id", 1)])
    return FOREX_Indexes

FOREX_IndexesCollection=get_FOREXIndexes_collection()





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
    unique_list = list(set(currency_names))
    return unique_list


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


    print(">>> Currencies list from the dataframe ",len(df))
    print(df)
    print("")
    print("")
    print(">>> Currencies list from the API with length ",len(currencies_list))
    print(currencies_list)

    currency_objects = []

    for Currency_Code in currencies_list:
        match = df[(df['Currency_Code'] == Currency_Code) & (df['Status'] != 'inserted')]
        if not match.empty:
            Symbol = match.iloc[0]['Symbol']
            Currency_Full_Name = match.iloc[0]['Currency_Full_Name']

            currency_objects.append({
                    "Currency_Code": Currency_Code,
                    "Full_Name": Currency_Full_Name,
                    "Symbol": Symbol
                })
            
    if currency_objects:
        print("Currencies to be created ")
        AvailableCurrenciesCollection.insert_many(serializeList2(currency_objects))
        df.loc[df['Currency_Code'].isin(currencies_list), 'Status'] = 'inserted'
        df.to_csv(currencies_CSV_FileName, index=False)

    return currency_objects
    






@FOREX.get("/createFOREX_Indexes__API")
async def createAvailableCurrencies_API():
    DB_Available_Currencies=serializeList2(AvailableCurrenciesCollection.find({}))
    currencies_objects_list_from_API = return_currencies_pairs()
    print(">>>>> currencies_objects_list_from_API")
    print(currencies_objects_list_from_API)

    print(">>>>> DB_Available_Currencies")
    print(DB_Available_Currencies)

    print("DB_FOREX_Indexes")
    print(serializeList2(FOREX_IndexesCollection.find({})))

    FOREXsymbolsList = [obj["symbol"] for obj in serializeList2(FOREX_IndexesCollection.find({}))]

    print(">>>>>>>>>> FOREXsymbolsList")

    print(FOREXsymbolsList)

    print(">>>>>>>>>>>>>>>>>")

    FOREX_Indexes=[]

    for currency_object in currencies_objects_list_from_API:
        # adding a condition to verify if the forex index exists already in the database or not 
        if currency_object["symbol"] not in FOREXsymbolsList:

            print(">>> Trating the index  ",currency_object['name'])
            currency_parts = currency_object['name'].split('/')
            first_currency=currency_parts[0]
            second_currency=currency_parts[1]

            for DB_available_currency_object in DB_Available_Currencies:
                if first_currency == DB_available_currency_object['Currency_Code']:
                    print("The first_currency ",first_currency ,"_id ",DB_available_currency_object['_id'])
                    first_currency_id=DB_available_currency_object['_id']

            for DB_available_currency_object in DB_Available_Currencies:
                if second_currency == DB_available_currency_object['Currency_Code']:
                    print("The second_currency ",second_currency ,"_id ",DB_available_currency_object['_id'])
                    final_currency_id=DB_available_currency_object['_id']

            FOREX_Indexes.append({
                        "name": currency_object["name"],
                        "symbol": currency_object["symbol"],
                        "intial_currency": first_currency,
                        "intial_currency_id": first_currency_id,
                        "final_currency": second_currency,
                        "final_currency_id": final_currency_id
                    })      
    if len(serializeList2(FOREX_Indexes))>0:   
        FOREX_IndexesCollection.insert_many(serializeList2(FOREX_Indexes))
    return("createFOREX_Indexes__API")








@FOREX.get("/AvailableCurrenciesCollection_API")
async def createAvailableCurrencies_API():
    return(serializeList2(AvailableCurrenciesCollection.find({})))














