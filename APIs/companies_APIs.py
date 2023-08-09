

from fastapi import APIRouter,Response,Query
from models.Sector import Sector 

from config.db import get_database 

from APIs.sectors_APIs import find_sector_id_by_name
from APIs.exchange_APIs import find_Exchange_id_by_name

from APIs.countries_APIs import find_Country_id_by_name  
from APIs.countries_APIs import find_subregion_id_by_Countryname  

from typing import List, Optional



import pandas as pd


import requests

import time

from dotenv import load_dotenv
load_dotenv()

import os

api_key = os.getenv("API_KEY")



Company=APIRouter()


def get_companies_collection():
    db = get_database()
    return db["companies"]



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

# Function that returns the peers of a company
def CompanyPeers(symbol):

    api_url = f"https://financialmodelingprep.com/api/v4/stock_peers?symbol={symbol}&apikey={api_key}"

    response = requests.get(api_url)
    
    # Check if response contains peers
    if response.json() and 'peersList' in response.json()[0]:
        return response.json()[0]['peersList']
    
    # Return custom message if no peers
    else:
        return 'No peers companies'
    
    
# API that launches the function companyPerrs
@Company.get("/getCompanyPeers/{Symbol}")
def getCompanyPeersBySymbol(Symbol: str):
    data = CompanyPeers(Symbol)
    return data





#Function to see more the dataframe of the csv file 
def creatingCompanies():
    DataFrame = pd.read_csv(os.getenv("CSV_FILE"), encoding='utf-8')


    # cleaning the dataframe
    DataFrameCompanies=DataFrame
    # print("Longueur de la dataframe avant de lui associer la drop duplicates ")
    # print(len(DataFrameCompanies))

    #removing duplicate elements of the dataframe
    # print("Longueur de la dataframe après la drop duplicates ")
    DataFrameCleaned=DataFrameCompanies.drop_duplicates(subset='companyName')
    # print(len(DataFrameCleaned))

    # removing nan elements
    # print("Longueur de la dataframe après le remove des nan et des empty elements ")
    DataFrameCleaned=DataFrameCleaned.dropna(subset='companyName')
    # print(len(DataFrameCleaned))

    #removing elements that have false isEtf
    DataFrameCleaned = DataFrameCleaned[(DataFrameCleaned['isEtf']==False) & (DataFrameCleaned['isAdr']==False) & (DataFrameCleaned['isFund']==False) ]
    # print("Longueur de la dataframe après lui accorder les valeurs nécessaires ")
    # print(len(DataFrameCleaned))
    CompaniesSymbols=DataFrameCleaned['Symbol']

    CompaniesSymbols = CompaniesSymbols.to_frame()

    # print('CompaniesSymbols=pd.DataFrame()')
    # print(CompaniesSymbols)

    companiesSymbolsList=[] 

    for index, row in CompaniesSymbols.iterrows():
        companiesSymbolsList.append(row)


    

    for i in range(len(companiesSymbolsList)-1):
        symbol = companiesSymbolsList[i]['Symbol']
        existing_company = get_companies_collection().find_one({"symbol": symbol})
        
        if existing_company:
            print(i,"/",len(companiesSymbolsList)-1,"-x-x-x-x-x-- >  Company with symbol <<", symbol, " >> already exists ")
        else:
            CompanyInfo_fromAPI = fetch_data_from_api_bySymbol(symbol)[0]

            print(i,"/",len(companiesSymbolsList)-1,"->->->->->-> >Treating the company << ", symbol,' >>')
            get_companies_collection().insert_one({
                "sectorId": find_sector_id_by_name(CompanyInfo_fromAPI['sector']),
                "subregionId": find_subregion_id_by_Countryname(CompanyInfo_fromAPI['country']),
                "countryId": find_Country_id_by_name(CompanyInfo_fromAPI['country']),
                "exchangeId": find_Exchange_id_by_name(CompanyInfo_fromAPI['exchangeShortName']),

                "companyName": CompanyInfo_fromAPI['companyName'],
                "symbol": symbol,
                "ipoDate": CompanyInfo_fromAPI['ipoDate'],
                "isin": CompanyInfo_fromAPI['isin'],
                "exchange": CompanyInfo_fromAPI['exchange'],
                "exchangeShortName": CompanyInfo_fromAPI['exchangeShortName'],
                "industry": CompanyInfo_fromAPI['industry'],
                "sector": CompanyInfo_fromAPI['sector'],
                "website": CompanyInfo_fromAPI['website'],
                "description": CompanyInfo_fromAPI['description'],
                "country": CompanyInfo_fromAPI['country'],
                "image": CompanyInfo_fromAPI['image'],
                "peers": CompanyPeers(CompanyInfo_fromAPI['symbol'])
            })







# API that launches the function creatingExchanges
@Company.get('/creatingCompanies')
async def CompaniesCreation():    
    creatingCompanies()
    return("csv_file Len API ")




# API endpoint to filter companies based on name and sector
@Company.get("/filterCompanies")
async def filter_companies(name: str = Query(None, title="Company Name"),
                           sector: str = Query(None, title="Sector")):
    filters = {}
    
    if name:
        filters["companyName"] = name
    
    if sector:
        filters["sector"] = sector
    
    companies_collection = get_companies_collection()
    filtered_companies = list(companies_collection.find(filters))
    print(filtered_companies)
    
    return "filtered_companies"



