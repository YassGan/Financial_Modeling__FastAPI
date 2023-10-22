from config.db import get_database 
from fastapi import APIRouter,Response
from schemas.Industry import serializeList

import pandas as pd

import os

Exchange=APIRouter()



def get_exchange_collection():
    db = get_database()
    return db["exchange"]










exchangeCollection=get_exchange_collection()



# Function that returns all the exchanges from the database
def gettingAllExchangess():
    return serializeList(exchangeCollection.find())


#API to get all the Exchange from the database
@Exchange.get('/AllExchanges')
async def AllExchanges_API():
    return gettingAllExchangess()




#find_Country_id_by_name
def find_Exchange_id_by_name(CPHCode):
    result = exchangeCollection.find_one({
        'exchangeShortName': CPHCode
    })
    
    if result:
        return str(result['_id'])
    else:
        return "Exchange not found "
    
# API that calls the find_Country_id_by_name
@Exchange.get("/exchangeMongoId/{CPHCode}")
def get_exchange(CPHCode: str):
    exchangeid = find_Exchange_id_by_name(CPHCode)
    return exchangeid







# Function that creates new exchanges in the collection of exchange 
def creatingExchanges(DataFrame):
    DataFrame_Exchange = DataFrame[['exchange','exchangeShortName']]  

    #print(DataFrame_Exchange)

    # Cleaning the dataframe of exchanges, removing redundant elements and removing also null and empty values
    UniqueExchanges = DataFrame_Exchange.drop_duplicates()
    UniqueExchanges.dropna(inplace=True)


    # print('The exchange elements')
    # print(UniqueExchanges)

    if not UniqueExchanges.empty:
        new_exchanges = UniqueExchanges.to_dict(orient='records')
        
        existing_exhanges = set(exchangeCollection.distinct("exchange"))
        new_exchanges_to_create = [exchange for exchange in new_exchanges if exchange["exchange"] not in existing_exhanges]

        if new_exchanges_to_create:
            exchangeCollection.insert_many(new_exchanges_to_create)
            print( f"----------------> {len(new_exchanges_to_create)} new exchanges created successfully.")
        else:
            print( "- No new exchanges to create.")
            print( "------------> No new exchanges to create.")
            print( "------------> No new exchanges to create.")

    else:
        print( "------------> No new exchanges to create.")
        print( "------------> No new exchanges to create.")
        print( "------------> No new exchanges to create.")



# API that launches the function creatingExchanges
@Exchange.get('/creatingExchanges')
async def CountriesListAPI(): 
    DataFrame = pd.read_csv(os.getenv("CSV_FILE"), encoding='utf-8')
   
    creatingExchanges(DataFrame)
    return("creatingExchangesAPI")
