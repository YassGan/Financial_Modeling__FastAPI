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





df = pd.read_csv("data.csv", encoding='utf-8')


print("-----------------------------")


print(df.size)


filtered_df = df[df['sector'].notna()]





num_rows, num_cols = filtered_df.shape



selected_columns = ['companyName', 'sector','address']
selected_df = filtered_df[selected_columns]





print("The dataframe filtered")
print(selected_df)

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