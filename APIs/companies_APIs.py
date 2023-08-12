

from fastapi import APIRouter,Response,Query
from models.Sector import Sector 

from config.db import get_database 

from APIs.sectors_APIs import find_sector_id_by_name
from APIs.exchange_APIs import find_Exchange_id_by_name

from APIs.countries_APIs import find_Country_id_by_name  
from APIs.countries_APIs import find_subregion_id_by_Countryname  

from typing import List, Optional

from APIs.countries_APIs import find_subregion_id_by_name 

from multiprocessing import Pool


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
    companies.create_index([("Symbol", 1)], unique=True)

    return companies


CompaniesCollection=get_companies_collection()








def gettingAllSymbols(DataFrame):
    DataFrameCompanies = DataFrame
    DataFrameCleaned = DataFrameCompanies.drop_duplicates(subset='Symbol')
    DataFrameCleaned = DataFrameCleaned.dropna(subset='Symbol')
    DataFrameCleaned = DataFrameCleaned[
        (~DataFrameCleaned['isEtf']) & (~DataFrameCleaned['isAdr']) & (~DataFrameCleaned['isFund'])
    ]
    CompaniesSymbols = DataFrameCleaned['Symbol']
    
    # Concatenate symbols with a comma
    symbols_concatenated = ','.join(CompaniesSymbols)
    
    return symbols_concatenated





@Company.get("/getAllSymbols")
def gettingAllSymbolsAPI():
    data = gettingAllSymbols()
    print(data)
    return (data)




# Function that returns the peers of a company
def CompanyPeers(symbol):
    print(" Company with symbol ",symbol, " made company Peers API call")

    api_url = f"https://financialmodelingprep.com/api/v4/stock_peers?symbol={symbol}&apikey={api_key}"

    response = requests.get(api_url)
    return response.json()
    #The code underneath returns only the list of company perrs, the code above retunrs the company peers and the sympbol of the company
    # # Check if response contains peers
    # if response.json() and 'peersList' in response.json()[0]:
    #     return response.json()[0]['peersList']
    
    # # Return custom message if no peers
    # else:
    #     return 'No peers companies'
    
    
# API that launches the function companyPerrs
@Company.get("/getCompanyPeers/{Symbol}")
def getCompanyPeersBySymbol(Symbol: str):
    data = CompanyPeers(Symbol)
    return data






DataFrame = pd.read_csv(os.getenv("CSV_FILE"), encoding='utf-8')

All_Symbols = gettingAllSymbols(DataFrame)


perrsListDatafromOneAPICall = CompanyPeers(All_Symbols)

# Create a dictionary that maps symbols to their peers lists
symbol_to_peers = {entry['symbol']: entry['peersList'] for entry in perrsListDatafromOneAPICall}

def find_peers_list_by_symbol_in_list(symbol_to_find):
    return symbol_to_peers.get(symbol_to_find, "No Peers List")



def creatingCompaniesInsertMany(DataFrame):

    start_time = time.time()



    DataFrameCleaned = DataFrame.drop_duplicates(subset='companyName').dropna(subset='companyName')
    DataFrameCleaned = DataFrameCleaned[
        (~DataFrameCleaned['isEtf']) & (~DataFrameCleaned['isAdr']) & (~DataFrameCleaned['isFund'])
    ]




    # perrsListDatafromOneAPICall=CompanyPeers(All_Symbols)
    # print('----------- perrsListDatafromOneAPICall ')
    # print("-<<-<-<-<-<-<-<-<-<-<-<-<--<-<-<-<--< type of perrsListDatafromOneAPICall ",type(perrsListDatafromOneAPICall))
    # print(perrsListDatafromOneAPICall)


    # DFperrsListDatafromOneAPICall = pd.DataFrame(perrsListDatafromOneAPICall)
    # print("-<<-<-<-<-<-<-<-<-<-<-<-<--<-<-<-<--< type of DFperrsListDatafromOneAPICall ",type(DFperrsListDatafromOneAPICall))
    # print(DFperrsListDatafromOneAPICall)


    # print("testing the function that returs the peers list from the symbol ")

    # print(find_peers_list_by_symbol_in_list("TRYG.CO"))


    companiesDb = get_companies_collection()


    # check_for_null_symbols(DataFrameCleaned)


    selected_columns_Companies = DataFrameCleaned[['companyName', 'Symbol', 'ipoDate','isin','exchange','exchangeShortName','industry','sector','website','description','country','image']]

    print(len(selected_columns_Companies))






    with Pool(processes=5) as pool:
        start_time_pool = time.time()

        selected_columns_Companies['subregionId'] = pool.map(find_subregion_id_by_Countryname, selected_columns_Companies['country'])
        selected_columns_Companies['sectorId'] = pool.map(find_sector_id_by_name, selected_columns_Companies['sector'])
        selected_columns_Companies['countryId'] = pool.map(find_Country_id_by_name, selected_columns_Companies['country'])
        selected_columns_Companies['exchangeId'] = pool.map(find_Exchange_id_by_name, selected_columns_Companies['exchangeShortName'])
        selected_columns_Companies['companyPeers'] = pool.map(find_peers_list_by_symbol_in_list, selected_columns_Companies['Symbol'])


        end_time_pool = time.time()
        elapsed_time_pool = end_time_pool - start_time_pool
        print(f"Time spent in Pool: {elapsed_time_pool:.2f} seconds")







    if not selected_columns_Companies.empty:
        companiesDb.insert_many(selected_columns_Companies.to_dict('records'))

    print("DataFrame cleaning and inserting took: %.2f seconds" % (time.time() - start_time))


    # # Create a list to store the documents to be inserted
    # documents_to_insert = []

    # for i, row in DataFrameCleaned.iterrows():
    #     symbol = row['Symbol']
    #     existing_company = companiesDb.find_one({"symbol": symbol})

    #     if existing_company:
    #         print(i, "/", len(DataFrameCleaned) - 1, "-x-x-x-x-x-- > Company with symbol <<", symbol, ">> already exists")
    #     else:
    #         treating_start_time = time.time()

    #         print(i + 1, "/", len(DataFrameCleaned) - 1, "->->->->->-> >Treating the company <<", symbol, '>>')

    #         documents_to_insert.append({
    #             "sectorId": find_sector_id_by_name(row['sector']),
    #             "subregionId": find_subregion_id_by_Countryname(row['country']),
    #             "countryId": find_Country_id_by_name(row['country']),
    #             "exchangeId": find_Exchange_id_by_name(row['exchangeShortName']),

    #             "companyName": row['companyName'],
    #             "symbol": symbol,
    #             "ipoDate": row['ipoDate'],
    #             "isin": row['isin'],
    #             "exchange": row['exchange'],
    #             "exchangeShortName": row['exchangeShortName'],
    #             "industry": row['industry'],
    #             "sector": row['sector'],
    #             "website": row['website'],
    #             "description": row['description'],
    #             "country": row['country'],
    #             "image": row['image'],
    #             # "peers": CompanyPeers(symbol)
    #         })

    #         print("Treating the company took: %.2f seconds" % (time.time() - treating_start_time))

    # # Convert the list of dictionaries to a DataFrame
    # df_to_insert = pd.DataFrame(documents_to_insert)

    # # print("Type of documents_to_insert ",type(documents_to_insert))
    # # print("Type of df_to_insert",type(df_to_insert))


    # # Insert the DataFrame into the database
    # if not df_to_insert.empty:
    #     companiesDb.insert_many(df_to_insert.to_dict('records'))

    # print("Processing each symbol and inserting into MongoDB took: %.2f seconds" % (time.time() - start_time))




# API that launches the function creatingExchanges
@Company.get('/creatingCompaniesWithInsertMany')
async def CompaniesCreationApiInsertMany(): 
    #creatingCompaniesInsertMany(DataFrame)

    chunk_size = 100
    Process = 0
    for i in range(0, len(DataFrame), chunk_size):
        Process=Process+1
        chunk = DataFrame.iloc[i:i+chunk_size]

        # Perform your treatments on the current chunk
        print("Processing chunk:",Process)
        print(chunk)
        creatingCompaniesInsertMany(chunk)

        # Add your treatment code here for the current chunk
        # For example, you can use 'chunk' to apply functions or calculations

        # Add a line break for clarity in the console output
        print("\n")   
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


def check_for_null_symbols(data_frame):
    if 'Symbol' not in data_frame.columns:
        raise ValueError("DataFrame doesn't have a 'Symbol' column.")

    null_symbols = data_frame[data_frame['Symbol'].isnull()]
    if null_symbols.empty:
        print("No null values found in the 'Symbol' column.")
    else:
        print("Null values found in the 'Symbol' column:")
        print(null_symbols)
























#Function that creates new companies and gets information from the dataframe itself
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

    companiesDb = get_companies_collection()

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

            # Get the company information from the DataFrame itself
            company_info = DataFrameCleaned[DataFrameCleaned['Symbol'] == symbol].iloc[0]

            print(i + 1, "/", len(companiesSymbolsList) - 1, "->->->->->-> >Treating the company << ", symbol, ' >>')
            companiesDb.insert_one({
                # "sectorId": find_sector_id_by_name(company_info['sector']),
                # "subregionId": find_subregion_id_by_Countryname(company_info['country']),
                # "countryId": find_Country_id_by_name(company_info['country']),
                # "exchangeId": find_Exchange_id_by_name(company_info['exchangeShortName']),

                "companyName": company_info['companyName'],
                "symbol": symbol,
                "ipoDate": company_info['ipoDate'],
                "isin": company_info['isin'],
                "exchange": company_info['exchange'],
                "exchangeShortName": company_info['exchangeShortName'],
                "industry": company_info['industry'],
                "sector": company_info['sector'],
                "website": company_info['website'],
                "description": company_info['description'],
                "country": company_info['country'],
                "image": company_info['image'],
                # "peers": CompanyPeers(symbol)
            })

            print("Treating the company took: %.2f seconds" % (time.time() - treating_start_time))

    print("Processing each symbol and inserting into MongoDB took: %.2f seconds" % (time.time() - start_time))



# API that launches the function creatingExchanges
@Company.get('/creatingCompanies')
async def CompaniesCreation():    
    creatingCompanies()
    return("csv_file Len API ")



#Function that creates companies and uses the information from the online API
def creatingCompaniesfromAPI():
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
            companiesDb.insert_one({
                # "sectorId": find_sector_id_by_name(CompanyInfo_fromAPI['sector']),
                # "subregionId": find_subregion_id_by_Countryname(CompanyInfo_fromAPI['country']),
                # "countryId": find_Country_id_by_name(CompanyInfo_fromAPI['country']),
                # "exchangeId": find_Exchange_id_by_name(CompanyInfo_fromAPI['exchangeShortName']),

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
@Company.get('/creatingCompaniesfromAPI')
async def CompaniesCreationApi():    
    creatingCompaniesfromAPI()
    return("csv_file Len API ")




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

