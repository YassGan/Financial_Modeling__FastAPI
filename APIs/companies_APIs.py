

from fastapi import APIRouter,Response
from models.Sector import Sector 

from config.db import get_database 

from APIs.sectors_APIs import find_sector_id_by_name

from APIs.countries_APIs import find_Country_id_by_name  
from APIs.countries_APIs import find_subregion_id_by_Countryname  



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
def CompanyPeers(Symbol):
    api_url = f"https://financialmodelingprep.com/api/v4/stock_peers?symbol={Symbol}&apikey={api_key}"
    print(api_url)
    response = requests.get(api_url)
    return response.json()

# API that launches the function companyPerrs
@Company.get("/getCompanyPeers/{Symbol}")
def getCompanyPeersBySymbol(Symbol: str):
    data = CompanyPeers(Symbol)
    return data





#Function to see more the dataframe of the csv file 
def creatingCompanies():
    DataFrame = pd.read_csv("data_with_17.csv", encoding='utf-8')


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
    CompaniesSymbols=DataFrameCleaned['Symbol']

    CompaniesSymbols = CompaniesSymbols.to_frame()

    # print('CompaniesSymbols=pd.DataFrame()')
    # print(CompaniesSymbols)

    companiesSymbolsList=[] 

    for index, row in CompaniesSymbols.iterrows():
        companiesSymbolsList.append(row)


    
    CompaniesToInsertInDatabase=[]

    for i in range(len(companiesSymbolsList)-1):
        CompanyInfo_fromAPI=fetch_data_from_api_bySymbol(companiesSymbolsList[i]['Symbol'])[0]

        print("Treating the company ",companiesSymbolsList[i]['Symbol'])
        CompaniesToInsertInDatabase.append({
                        "sectorId": find_sector_id_by_name(CompanyInfo_fromAPI['sector']),
                        "subregionId": find_subregion_id_by_Countryname(CompanyInfo_fromAPI['country']),
                        "countryId": find_Country_id_by_name(CompanyInfo_fromAPI['country']),
                        "companyName":CompanyInfo_fromAPI['companyName'],
                        "symbol":companiesSymbolsList[i]['Symbol'],
                        "ipoDate":CompanyInfo_fromAPI['ipoDate'],
                        "isin":CompanyInfo_fromAPI['isin'],
                        "exchange":CompanyInfo_fromAPI['exchange'],
                        "exchangeShortName":CompanyInfo_fromAPI['exchangeShortName'],
                        "industry":CompanyInfo_fromAPI['industry'],
                        "sector":CompanyInfo_fromAPI['sector'],
                        "website":CompanyInfo_fromAPI['website'],
                        "description":CompanyInfo_fromAPI['description'],
                        "image":CompanyInfo_fromAPI['image'],
                        # "peers":CompanyPeers( CompanyInfo_fromAPI['image'])[0]['peersList'])

                    })
        
    CompaniesToInsertInDatabaseDf = pd.DataFrame(CompaniesToInsertInDatabase)
    

        
     
    print("CompanyData List ")
    print(CompaniesToInsertInDatabase)

    print("CompanyData df ")
    print(CompaniesToInsertInDatabaseDf)


    if not CompaniesToInsertInDatabaseDf.empty:
            new_companies = CompaniesToInsertInDatabaseDf.to_dict(orient='records')
            
            existing_companies = set(get_companies_collection().distinct("symbol"))
            new_companies_to_create = [company for company in new_companies if company.get("symbol") not in existing_companies]

            if new_companies_to_create:
                get_companies_collection().insert_many(new_companies_to_create)
                print (f"- {len(new_companies_to_create)} new new_companies created successfully.")
            else:
                print( "- No new new_companies to create.")
    else:
            print( "- No new new_companies to create.")




    # print("Company sectorId ", find_sector_id_by_name(CompanyInfo_fromAPI['sector']))
    # print("Company subregionId ", find_subregion_id_by_Countryname(CompanyInfo_fromAPI['country']))
    # print("Company countryId ", find_Country_id_by_name(CompanyInfo_fromAPI['country']))

    # print("Company name ",CompanyInfo_fromAPI['companyName'])
    # print("Company symbol ",companiesSymbolsList[0]['Symbol'])
    # print("Company ipoDate ",CompanyInfo_fromAPI['ipoDate'])
    # print("Company isin ",CompanyInfo_fromAPI['isin'])
    # print("Company exchange ",CompanyInfo_fromAPI['exchange'])
    # print("Company exchangeShortName ",CompanyInfo_fromAPI['exchangeShortName'])
    # print("Company industry ",CompanyInfo_fromAPI['industry'])
    # print("Company sector ",CompanyInfo_fromAPI['sector'])
    # print("Company website ",CompanyInfo_fromAPI['website'])
    # print("Company description ",CompanyInfo_fromAPI['description'])
    # print("Company image ",CompanyInfo_fromAPI['image'])
    # print("Company peers ",CompanyPeers(companiesSymbolsList[0]['Symbol'])[0]['peersList'])


    # print("Company price ",CompanyInfo_fromAPI['price'])


    return len(DataFrame)



# API that launches the function creatingExchanges
@Company.get('/creatingCompanies')
async def CompaniesCreation():    
    creatingCompanies()
    return("csv_file Len API ")

