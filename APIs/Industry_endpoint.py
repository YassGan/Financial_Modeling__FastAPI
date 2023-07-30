from fastapi import APIRouter,Response
from models.Industry import Industry 
from config.db import get_database 
from schemas.Industry import serializeDict, serializeList
from bson import ObjectId

import requests
import pandas as pd

industry = APIRouter() 



def get_industry_collection():
    db = get_database()
    return db["industries"]


#API to get all the industries from the database





#API to create an industry and add it to the database(it can be used to add an industry manually)
@industry.post('/AddIndustry')
async def create_industry(industry: Industry):
    get_industry_collection().industry.insert_one(dict(industry))
    return serializeList(get_industry_collection().find())



@industry.get("/fetch_data")
def fetch_data():
    url = "https://financialmodelingprep.com/api/v4/profile/all?apikey=96051dba5181978c2f0ce23c1ef4014b"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        return {"error": "Failed to fetch data from the API."}





df = pd.read_csv("profile_bulkyyyy.csv", encoding='utf-8')


print("-----------------------------")


print(df.size)

num_rows, num_cols = df.shape

print("Number of rows in the DataFrame:", num_rows)
print("Number of columns in the DataFrame:", num_cols)

print("-----------------------------")





# # API to create new industries from the DataFrame efficiently
# @industry.post('/CreateIndustriesFromDataFrame')
# async def create_industries_from_dataframe():
#     existing_industry_names = set(get_industry_collection().distinct("name"))
#     new_industries = Industry_Collection_Necessary_Data[~Industry_Collection_Necessary_Data["industry"].isin(existing_industry_names)]
    
#     if not new_industries.empty:
#         new_industries_dict = new_industries.to_dict(orient="records")
#         get_industry_collection().insert_many(new_industries_dict)
#         return Response(status_code=201, content=f"{len(new_industries)} new industries created successfully.")
#     else:
#         return Response(status_code=200, content="No new industries to create.")


# print(Industry_Collection_Necessary_Data)