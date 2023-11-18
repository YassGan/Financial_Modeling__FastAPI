from fastapi import HTTPException, FastAPI, Response, status, APIRouter
from fastapi.responses import JSONResponse

import pandas as pd
import requests
from config.db import get_database 
import os
from schemas.Sector import serializeList2
from schemas.Sector import serializeDict2



stock_indexes = APIRouter()
api_key = os.getenv("API_KEY")




def get_stockIndexes_collection():
    db = get_database()
    Stock_Indexes=db["Stock_Indexes"]
    Stock_Indexes.create_index([("_id", 1)])
    return Stock_Indexes
StockIndexesCollection=get_stockIndexes_collection()


#API to get all the stock indexes from the database
@stock_indexes.get('/v1/AllStock_Indexes')
async def find_all_stock_indexes():
    return serializeList2(StockIndexesCollection.find())


def return_stockIndexes_data_online_api():
    print("Making an API call to get the stock indexes")
    api_url = f"https://financialmodelingprep.com/api/v3/symbol/available-indexes?apikey={api_key}"
    response = requests.get(api_url)
    return serializeList2(response.json())


@stock_indexes.get("/v1/return_stock_Indexes_online_API")
async def return_currencies_pairs_API():
    stockIndexesData = return_stockIndexes_data_online_api()
    if not stockIndexesData:
        raise HTTPException(status_code=404, detail="No data returned from the online API")
    return stockIndexesData



def return_stockIndexes_symbols_db():
    stock_indexes_cursor = StockIndexesCollection.find()
    stock_indexes_List=serializeList2(stock_indexes_cursor)
    symbols = [entry["symbol"] for entry in stock_indexes_List]
    return symbols


@stock_indexes.get("/v1/return_stock_Indexes_symbols_DB")
async def return_stock_Indexes_symbols_DB():
    return return_stockIndexes_symbols_db()







def create_stockIndexes():
    stockIndexesSymbolsDB=return_stockIndexes_symbols_db()
    stock_indexes_data_api = return_stockIndexes_data_online_api()


    print("--Stock indexes data from the api ")
    print(stock_indexes_data_api)

    print("--Stock indexes symbols from the DB ")
    print(stockIndexesSymbolsDB)

    new_StockIndexes_to_create = [obj for obj in stock_indexes_data_api if obj['symbol'] not in stockIndexesSymbolsDB]


    if new_StockIndexes_to_create:
        StockIndexesCollection.insert_many(new_StockIndexes_to_create)
        return f"Inserting {len(new_StockIndexes_to_create)} new stockIndexes"
    else:
        return "No data to insert"


@stock_indexes.get("/v1/CreateStockIndexes")
async def return_currencies_pairs_API():
    result = create_stockIndexes()
    return result

