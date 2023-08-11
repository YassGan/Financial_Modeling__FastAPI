

from fastapi import APIRouter,Response,Query
from models.Sector import Sector 

from config.db import get_database 

from APIs.sectors_APIs import find_sector_id_by_name
from APIs.exchange_APIs import find_Exchange_id_by_name

from APIs.countries_APIs import find_Country_id_by_name  
from APIs.countries_APIs import find_subregion_id_by_Countryname  

from typing import List, Optional

from APIs.countries_APIs import find_subregion_id_by_name 


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
    companies=db["companies"]
    companies.create_index([("symbol", 1)], unique=True)

    return companies






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
    start_time = time.time()

    DataFrame = pd.read_csv(os.getenv("CSV_FILE"), encoding='utf-8')

    # cleaning the dataframe
    print("Reading CSV and initializing data took: %.2f seconds" % (time.time() - start_time))
    start_time = time.time()

    DataFrameCompanies = DataFrame
    DataFrameCleaned = DataFrameCompanies.drop_duplicates(subset='companyName')
    DataFrameCleaned = DataFrameCleaned.dropna(subset='companyName')
    DataFrameCleaned = DataFrameCleaned[(DataFrameCleaned['isEtf'] == False) & (DataFrameCleaned['isAdr'] == False) & (DataFrameCleaned['isFund'] == False)]
    CompaniesSymbols = DataFrameCleaned['Symbol']
    CompaniesSymbols = CompaniesSymbols.to_frame()
    companiesSymbolsList = []

    companiesDb=get_companies_collection()

    for index, row in CompaniesSymbols.iterrows():
        companiesSymbolsList.append(row)

    print("DataFrame cleaning and symbol extraction took: %.2f seconds" % (time.time() - start_time))
    start_time = time.time()

    for i in range(len(companiesSymbolsList) - 1):
        symbol = companiesSymbolsList[i]['Symbol']
        existing_company = companiesDb.find_one({"symbol": symbol})

        if existing_company:
            print(i, "/", len(companiesSymbolsList) - 1, "-x-x-x-x-x-- >  Company with symbol <<", symbol, " >> already exists ")
        else:
            treating_start_time = time.time()

            CompanyInfo_fromAPI = fetch_data_from_api_bySymbol(symbol)[0]

            print(i+1, "/", len(companiesSymbolsList) - 1, "->->->->->-> >Treating the company << ", symbol, ' >>')
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

            print("Treating the company took: %.2f seconds" % (time.time() - treating_start_time))

    print("Processing each symbol and inserting into MongoDB took: %.2f seconds" % (time.time() - start_time))




# API that launches the function creatingExchanges
@Company.get('/creatingCompanies')
async def CompaniesCreation():    
    creatingCompanies()
    return("csv_file Len API ")







from bson.json_util import dumps
# API endpoint to filter companies based on name and sector
@Company.get("/filterCompanies")
async def filter_companies(name: str = Query(None, title="Company Name"),
                           sector: str = Query(None, title="Sector"),
                           industry: str = Query(None, title="Industry"),
                           subregion: str = Query(None, title="Subregion"),
                           country: str = Query(None, title="Country"),                          
                           keywords: str = Query(None, title="Keywords")):
    filters = {}
    
    if name:
        filters["companyName"] = name
    
    if sector:
        filters["sector"] = sector
    
    if industry:
        filters["industry"] = industry
    
    if subregion:
        filters["subregion"] = subregion
    
    if country:
        filters["country"] = country

    if keywords:
        description_keyword_regex = f".*{keywords}.*"
        filters["description"] = {"$regex": description_keyword_regex, "$options": "i"}

        



    companies_collection = get_companies_collection()

    filtered_companies = list(companies_collection.find(filters))
    
    # Convert ObjectId to string and remove non-serializable fields
    for company in filtered_companies:
        company["_id"] = str(company["_id"])  # Convert ObjectId to string
        if "_cls" in company:
            del company["_cls"]  # Remove non-serializable field
    
    return filtered_companies






@Company.get("/autocompleteCompanyName")
async def autocomplete_company_name(query: str):
    if not query:
        return []

    regex_pattern = f".*{query}.*"
    filters = {"companyName": {"$regex": regex_pattern, "$options": "i"}}

    companies_collection = get_companies_collection()
    matching_companies = list(companies_collection.find(filters, {"companyName": 1}))

    autocomplete_results = [company["companyName"] for company in matching_companies]

    return autocomplete_results

