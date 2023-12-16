

from fastapi import APIRouter,Response,Query,HTTPException
from models.Sector import Sector 

from config.db import get_database 

from schemas.Sector import serializeList2


from APIs.Endpoints1.sectors_APIs import find_sector_id_by_name
from APIs.Endpoints1.exchange_APIs import find_Exchange_id_by_name
from APIs.Endpoints1.countries_APIs import find_Country_id_by_name  
from APIs.Endpoints1.countries_APIs import find_subregion_id_by_Countryname  
from APIs.Endpoints1.utilsFunctionalitites_APIs import download_csv_from_url


from APIs.Endpoints1.Industries_APIs import find_industry_id_by_name  





from typing import List, Optional
import os


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






def get_company_symbols():
    try:
        companies_collection = get_companies_collection()
        all_companies = companies_collection.find({}, { "Symbol": 1 })
        
        if not all_companies:
            return set()  
        
        symbols = {str(company.get("Symbol")) for company in all_companies}  
        
        return symbols
    except Exception as e:
        raise e

@Company.get("/getAllCompaniesFromDB")
async def get_all_company_symbols():
    symbols = get_company_symbols()
    
    if not symbols:
        raise HTTPException(status_code=404, detail="No companies found in the database")
    
    return symbols








@Company.get("/v1/infos/profile")
async def get_company_by_symbol(symbol: str = Query(..., title="Company Symbol")):
    try:
        company = CompaniesCollection.find_one({"Symbol": symbol})

        if company:
            # Sanitize the company data as needed
            sanitized_company = {
                "companyName": company.get("companyName", None),
                "sector": company.get("sector", None),
                "industry": company.get("industry", None),
                "country": company.get("country", None),
            }
            return sanitized_company
        else:
            raise HTTPException(status_code=404, detail="Company not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



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





@Company.get("/v1/getAllSymbols")
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
@Company.get("/v1/getCompanyPeers/{Symbol}")
def getCompanyPeersBySymbol(Symbol: str):
    data = CompanyPeers(Symbol)
    return data



from itertools import repeat







# Create a dictionary that maps symbols to their peers lists

def find_peers_list_by_symbol_in_list(symbol, symbol_list):
    for entry in symbol_list:
        if entry['symbol'] == symbol:
            return entry['peersList']
    return "No Peers List"





def creatingCompaniesInsertMany(DataFrame):

    start_time = time.time()

    All_Symbols = gettingAllSymbols(DataFrame)


    perrsListDatafromOneAPICall = CompanyPeers(All_Symbols)


    DataFrameCleaned = DataFrame.drop_duplicates(subset='companyName').dropna(subset='companyName')
    DataFrameCleaned = DataFrameCleaned[
        (~DataFrameCleaned['isEtf']) & (~DataFrameCleaned['isAdr']) & (~DataFrameCleaned['isFund'])
    ]

    companiesDb = get_companies_collection()


    selected_columns_Companies = DataFrameCleaned[['companyName', 'Symbol', 'ipoDate','isin','exchange','exchangeShortName','industry','sector','website','description','country','image']]

    print(len(selected_columns_Companies))


    with Pool(processes=6) as pool:
        start_time_pool = time.time()

        selected_columns_Companies['subregionId'] = pool.map(find_subregion_id_by_Countryname, selected_columns_Companies['country'])
        selected_columns_Companies['sectorId'] = pool.map(find_sector_id_by_name, selected_columns_Companies['sector'])
        selected_columns_Companies['countryId'] = pool.map(find_Country_id_by_name, selected_columns_Companies['country'])
        selected_columns_Companies['exchangeId'] = pool.map(find_Exchange_id_by_name, selected_columns_Companies['exchangeShortName'])
        selected_columns_Companies['industryId'] = pool.map(find_industry_id_by_name, selected_columns_Companies['industry'])    

        selected_columns_Companies['companyPeers'] = pool.starmap(find_peers_list_by_symbol_in_list, zip(selected_columns_Companies['Symbol'], repeat(perrsListDatafromOneAPICall)))



        end_time_pool = time.time()
        elapsed_time_pool = end_time_pool - start_time_pool
        print(f"Time spent in Pool: {elapsed_time_pool:.2f} seconds")


    if not selected_columns_Companies.empty:
        companiesDb.insert_many(selected_columns_Companies.to_dict('records'))

        # The old school way to verify if there are already elements in the database to not insert it a second time
        # new_compnaies = selected_columns_Companies.to_dict(orient='records')
        # existing_companies = set(companiesDb.distinct("Symbol"))
        # new_companies_to_create = [company for company in new_compnaies if company["Symbol"] not in existing_companies]

        # if new_companies_to_create:
        #     companiesDb.insert_many(new_companies_to_create)
        #     print(f"------------> {len(new_companies_to_create)} new companies created successfully.")
        # else:
        #     print("- No new companies to create.")

    print("DataFrame cleaning and inserting took: %.2f seconds" % (time.time() - start_time))





#This funciton returns the intersection between the old and the new csv file
def Function_Intersection_Old_New_CSV():

    # Download the new file
    url = f"https://financialmodelingprep.com/api/v4/profile/all?apikey={api_key}"
    print(url)
    file_path = "NewCSV.csv"

    success = download_csv_from_url(url, file_path)
    if success:
        print( Response(content="File downloaded successfully.", media_type="text/plain"))
    else:
        print( Response(content="Failed to download the file.", media_type="text/plain"))

    if success:
        NewDataFrame = pd.read_csv("NewCSV.csv", encoding='utf-8')
        OldDataFrame=pd.read_csv("OldCSV.csv", encoding='utf-8')

        start_Verification_New_Old_CSV_time = time.time()


        new_elements = NewDataFrame[~NewDataFrame['Symbol'].isin(OldDataFrame['Symbol'])]

        # Create the third dataframe containing only the new elements
        NewDataFrameValues = pd.DataFrame(new_elements)

        print(f"Time spent in verifying between the old csv and the new csv files is : {time.time()-start_Verification_New_Old_CSV_time:.2f} seconds")




        NewDataFrameValues = NewDataFrameValues.drop_duplicates(subset='companyName').dropna(subset='companyName')
        NewDataFrameValues = NewDataFrameValues[
            (~NewDataFrameValues['isEtf']) & (~NewDataFrameValues['isAdr']) & (~NewDataFrameValues['isFund'])
        ]



        # Reset the index of the third dataframe
        NewDataFrameValues.reset_index(drop=True, inplace=True)
        print("The new data frame with new elements")
        print(len(NewDataFrameValues))
        # Now we delete the old file and rename the new file to old file to make comparisons in the future with new other files 

        file_path_to_delete = "OldCSV.csv" 
        if os.path.exists(file_path_to_delete):
            os.remove(file_path_to_delete)
            print(f"File '{file_path_to_delete}' deleted successfully.")
            old_name = "NewCSV.csv"  # Replace with the current filename
            new_name = "OldCSV.csv"  # Replace with the new filename

            if os.path.exists(old_name):
                os.rename(old_name, new_name)
                print(f"File '{old_name}' renamed to '{new_name}' successfully.")
            else:
                print(f"File '{old_name}' does not exist.")
        else:
            print(f"File '{file_path_to_delete}' does not exist.")


        print("-<-<-<-<-<-<-<-<-<-<-<-<-<- The length of companies ")
        return(NewDataFrameValues)























def CompaniesCreationProcess(NewDataFrameValues):

        chunk_size = 100
        Process = 0
        for i in range(0, len(NewDataFrameValues), chunk_size):
            Process=Process+1
            chunk = NewDataFrameValues.iloc[i:i+chunk_size]

            print("Processing chunk:",Process)
            print(chunk)
            creatingCompaniesInsertMany(chunk)

            print("\n")   
        return("csv_file Len API ")











# API that launches the function creatingExchanges
@Company.get('/v1/creatingCompaniesWithInsertMany')
async def CompaniesCreationApiInsertMany(): 
    #Df=Function_Intersection_Old_New_CSV()
    DataFrame_1000 = pd.read_csv("CSV_1000_Element.csv", encoding='utf-8')

    CompaniesCreationProcess(DataFrame_1000)











from bson.json_util import dumps
# API endpoint to filter companies based on name and sector
@Company.get("/v1/filterCompanies_API")
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

    try:
        # Projection to fetch only necessary fields
        projection = {"companyName": 1, "sector": 1, "industry": 1, "subregion": 1, "country": 1}
        
        # Fetching data in batches using cursor
        filtered_companies_cursor = CompaniesCollection.find(filters, projection=projection)
        sanitized_companies = []

        for company in filtered_companies_cursor:
            sanitized_companies.append({
                "companyName": company["companyName"],
                "sector": company.get("sector", None),
                "industry": company.get("industry", None),
                "subregion": company.get("subregion", None),
                "country": company.get("country", None)
            })

        return sanitized_companies

    except Exception as e:
        return {"error": str(e)}


@Company.get("/v1/autocompleteCompanyName")
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














# Function to fetch comany's data from the API by its symbol
def fetch_data_from_api_bySymbol(Symbol):
    api_url = f"https://financialmodelingprep.com/api/v3/profile/{Symbol}?apikey={api_key}"
    response = requests.get(api_url)
    return response.json()


# Define the API route to fetch data from the API
@Company.get("/v1/getCompanyBySymbol/{Symbol}")
def getCompanyBySymbol(Symbol: str):
    data = fetch_data_from_api_bySymbol(Symbol)
    return data