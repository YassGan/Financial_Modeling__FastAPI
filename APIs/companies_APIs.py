

from fastapi import APIRouter,Response
from models.Sector import Sector 

from config.db import get_database 



import pandas as pd


import requests

import time

from dotenv import load_dotenv
load_dotenv()

import os

api_key = os.getenv("API_KEY")



Company=APIRouter()





# Function to fetch comany's data from the API by its symbol
def fetch_data_from_api_bySymbol(Symbol):
    api_url = f"https://financialmodelingprep.com/api/v3/profile/{Symbol}?apikey={api_key}"
    response = requests.get(api_url)
    return response.json()


# Define the API route to fetch data from the API
@Company.get("/getCompanyBySymbol/{Symbol}")
def getCompanyBySymbol(Symbol: str):
    data = fetch_data_from_api_bySymbol(Symbol)
    return data




#Function to see more the dataframe of the csv file 
def creatingCompanies():
    DataFrame = pd.read_csv("data.csv", encoding='utf-8')


    # cleaning the dataframe
    DataFrameCompanies=DataFrame
    print("Longueur de la dataframe avant de lui associer la drop duplicates ")
    print(len(DataFrameCompanies))

    #removing duplicate elements of the dataframe
    print("Longueur de la dataframe après la drop duplicates ")
    DataFrameCleaned=DataFrameCompanies.drop_duplicates(subset='companyName')
    print(len(DataFrameCleaned))

    # removing nan elements
    print("Longueur de la dataframe après le remove des nan et des empty elements ")
    DataFrameCleaned=DataFrameCleaned.dropna(subset='companyName')
    print(len(DataFrameCleaned))

    #removing elements that have false isEtf
    DataFrameCleaned = DataFrameCleaned[(DataFrameCleaned['isEtf']==False) & (DataFrameCleaned['isAdr']==False) & (DataFrameCleaned['isFund']==False) ]
    print("Longueur de la dataframe après lui accorder les valeurs nécessaires ")
    print(len(DataFrameCleaned))


    






    return len(DataFrame)



# API that launches the function creatingExchanges
@Company.get('/creatingCompanies')
async def CompaniesCreation():    
    creatingCompanies()
    return("csv_file Len API ")

