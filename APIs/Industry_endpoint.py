from fastapi import APIRouter,Response
from models.Industry import Industry 
from config.db import get_database 
from schemas.Industry import serializeDict, serializeList
from bson import ObjectId

import requests
import pandas as pd

import json
import io





End = APIRouter() 





# import schedule
# import time

# i=0

# def my_task():
#     global i  
#     i=i+1
#     print(i," This is a scheduled task.")

# schedule.every(3).seconds.do(my_task)

# while True:
#     schedule.run_pending()
#     time.sleep(1)







def get_industry_collection():
    db = get_database()
    return db["industries"]


#API to get all the industries from the database





#API to create an industry and add it to the database(it can be used to add an industry manually)
@End.post('/AddIndustry')
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




@End.get('/HelloIndusPoint')
async def HelloIndusPoint():
    return 'hello HelloIndusPoint'



@End.get('/DownloadFirst10AsJson')
async def download_first_10_as_json():
    first_10_df = filtered_df.head(10)  # Assuming you already have selected_df as a filtered DataFrame
    json_data = first_10_df.to_json(orient='records', lines=True)  # Convert DataFrame to JSON

    # Set the filename for the downloaded JSON file
    filename = "first_10_elements.json"

    # Save the JSON data to a file
    with open(filename, 'w', encoding='utf-8') as file:
        file.write(json_data)

    # Create a Response object to stream the file as a downloadable response
    response = Response(content=json_data, media_type='application/json')
    response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response

















# API to create new industries from the DataFrame efficiently
@End.get('/Existing')
async def create_industries_from_dataframe():
    # Get the set of existing industry names from the database
    existing_industry_names = set(get_industry_collection().distinct("industry"))
    print(existing_industry_names)
    # Filter the DataFrame to get new industries not present in the database
   






# API to create new industries from the DataFrame efficiently
@End.post('/CreateIndustriesFromDataFrame')
async def create_industries_from_dataframe():
    existing_industry_names = set(get_industry_collection().distinct("name"))
    new_industries = filtered_df[~filtered_df["industry"].isin(existing_industry_names)]
    
    if not new_industries.empty:
        new_industries_dict = new_industries.to_dict(orient="records")
        get_industry_collection().insert_many(new_industries_dict)
        return Response(status_code=201, content=f"{len(new_industries)} new industries created successfully.")
    else:
        return Response(status_code=200, content="No new industries to create.")


print(filtered_df)











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