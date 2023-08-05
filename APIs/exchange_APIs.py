from config.db import get_database 
from fastapi import APIRouter,Response

import pandas as pd



Exchange=APIRouter()



def get_exchange_collection():
    db = get_database()
    return db["exchange"]




# Function that creates new exchanges in the collection of exchange 
def creatingExchanges():
    DataFrame = pd.read_csv("data.csv", encoding='utf-8')


    DataFrame_Exchange = DataFrame[['exchange','exchangeShortName']]  

    print(DataFrame_Exchange)

    # Cleaning the dataframe of exchanges, removing redundant elements and removing also null and empty values
    UniqueExchanges = DataFrame_Exchange.drop_duplicates()
    UniqueExchanges.dropna(inplace=True)


    print('The exchange elements')
    print(UniqueExchanges)

    if not UniqueExchanges.empty:
        new_exchanges = UniqueExchanges.to_dict(orient='records')
        
        existing_exhanges = set(get_exchange_collection().distinct("exchange"))
        new_exchanges_to_create = [exchange for exchange in new_exchanges if exchange["exchange"] not in existing_exhanges]

        if new_exchanges_to_create:
            get_exchange_collection().insert_many(new_exchanges_to_create)
            print( f"- {len(new_exchanges_to_create)} new exchanges created successfully.")
        else:
            print( "- No new exchanges to create.")
    else:
        print( "- No new exchanges to create.")


# API that launches the function creatingExchanges
@Exchange.get('/creatingExchanges')
async def CountriesListAPI():    
    creatingExchanges()
    return("creatingExchangesAPI")
