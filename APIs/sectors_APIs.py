
from fastapi import APIRouter,Response
from models.Sector import Sector 
from schemas.Sector import serializeList2
from config.db import get_database 

import os


import pandas as pd



import time


Sector=APIRouter()






def get_sector_collection():
    db = get_database()
    sectors=db["sectors"]  
    sectors.create_index([("name", 1)], unique=True)
    return sectors




sectorsCollection=get_sector_collection()


#find_Country_id_by_name
def find_sector_id_by_name(sectorName):

    result = sectorsCollection.find_one({
        'name': sectorName
    })
    
    if result:
        return str(result['_id'])
    else:
        return "sector not found "
    
# API that calls the find_Country_id_by_name
@Sector.get("/sectorMongoId/{sectorName}")
def get_sector(sectorName: str):
    sectorid = find_sector_id_by_name(sectorName)
    return sectorid















# Function that creates new sectors in the database from a dataFrame
def create_sectors_from_dataframe():

    DataF = pd.read_csv(os.getenv("CSV_FILE"), encoding='utf-8')

    start_time = time.time()
    DataF.dropna(subset=['sector'], inplace=True)

    # Get the unique values of the "Sector" column
    Sector_variables = DataF["sector"].unique()
    # print("Time taken for creating DataFrame:", time.time() - start_time, "seconds")

    # Convert the NumPy array to a DataFrame
    sector_df = pd.DataFrame(Sector_variables, columns=["name"])
    # print('The type of the sector_df variable is', type(sector_df))
    # print(sector_df)

    if not DataF.empty:
        # Convert the DataFrame to a dictionary with "records" orientation
        sectors_to_dict = sector_df.to_dict(orient='records')

        existing_sectors = set(get_sector_collection().distinct("name"))
        new_sectors_to_create = [sector for sector in sectors_to_dict if sector["name"] not in existing_sectors]

        if new_sectors_to_create:
            get_sector_collection().insert_many(new_sectors_to_create)
            print (f"--------------------- > {len(new_sectors_to_create)} new sectors created successfully.")
        else:
            print ("- No new sectors to create.")
            print( "----------------------> No new sectors to create.")
            print( "----------------------> No new sectors to create.")
    else:
        print( "----------------------> No new sectors to create.")
        print( "----------------------> No new sectors to create.")
        print( "----------------------> No new sectors to create.")






# API that creates new sectors from the DataFrame 
@Sector.get('/CreateSectorsFromDataFrame')
async def create_sectors_api():
    DataF = pd.read_csv(os.getenv("CSV_FILE"), encoding='utf-8')
    result = create_sectors_from_dataframe(DataF)
    if "new sectors created successfully" in result:
        return Response(status_code=201, content=result)
    else:
        return Response(status_code=200, content=result)



#API to get all the sectors from the database
@Sector.get('/AllSectors')
async def find_all_sectors():
    return serializeList2(get_sector_collection().find())





