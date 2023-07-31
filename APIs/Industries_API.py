from fastapi import APIRouter,Response
from models.Industry import Industry 
from config.db import get_database 
from schemas.Industry import serializeDict, serializeList
from bson import ObjectId

import requests
import pandas as pd










BASE_URL ='https://financialmodelingprep.com/api'	

API_KEY	='96051dba5181978c2f0ce23c1ef4014b'
Symbol	='VDADX'
industry = APIRouter() 

# https://financialmodelingprep.com/api/v3/profile/AAPL?apikey=****

	
def csv_to_json(csv_file_path):
    df = pd.read_csv(csv_file_path)
    json_data = df.to_json(orient='records')
    return json_data


JSON=csv_to_json('profile_bulkyyyy.csv')

#print(JSON)


@industry.get("/download_json")
def download_json():
    JSON = csv_to_json('profile_bulkyyyy.csv')
    response = Response(content=JSON, media_type="application/json")
    response.headers["Content-Disposition"] = "attachment; filename=data.json"
    return response



def APIRequest(BASE_URL, Symbol, API_KEY):
    return f"{BASE_URL}/v3/profile/{Symbol}?apikey={API_KEY}"


def get_industry_collection():
    db = get_database()
    return db["industries"]





# Function to fetch data from the API
def fetch_data_from_api(Symbol):
    api_url = f"https://financialmodelingprep.com/api/v3/profile/{Symbol}?apikey=96051dba5181978c2f0ce23c1ef4014b"
    response = requests.get(api_url)
    return response.json()


# Define the API route to fetch data from the API
@industry.get("/get_data/{Symbol}")
def get_data(Symbol: str):
    data = fetch_data_from_api(Symbol)
    return data






#API to get all the industries from the database
@industry.get('/AllIndustries')
async def find_all_industries():
    return serializeList(get_industry_collection().find())

# @user.get('/{id}')
# async def find_one_user(id):
#     return serializeDict(conn.local.user.find_one({"_id":ObjectId(id)}))



#API to create an industry and add it to the database(it can be used to add an industry manually)
@industry.post('/AddIndustry')
async def create_industry(industry: Industry):
    get_industry_collection().industry.insert_one(dict(industry))
    return serializeList(get_industry_collection().find())
