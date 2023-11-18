
from fastapi import APIRouter,Response,Query

from config.db import get_database 
from schemas.Sector import serializeList2
from schemas.Sector import serializeDict2

import pymongo 
import math 
import re
import json
import pandas as pd

CompanyFiltering=APIRouter()

from APIs.Endpoints1.countries_APIs import get_Country_Name_by_id_from_db

from typing import List



def get_companies_collection():
    db = get_database()
    companies=db["companies"]
    companies.create_index([("Symbol", 1)], unique=True)

    return companies


CompaniesCollection=get_companies_collection()

# Create an index on multiple fields
CompaniesCollection.create_index([
    ("description", "text"),
    ("companyName", pymongo.ASCENDING),  
    ("sector", pymongo.ASCENDING),
    ("industry", pymongo.ASCENDING),
    ("subregion", pymongo.ASCENDING),
    ("country", pymongo.ASCENDING)
])


from bson.json_util import dumps
# API endpoint to filter companies based on name and sector
# Example on how to use this api 
# http://localhost:8000/filterCompanies_Filtering?name=CompanyName&sector=Technology&industry=Software&subregion=North&country=USA&keywords=AI"

#Curl of many fields for example countries http://localhost:1001/filterCompanies_Filtering?country=FR&country=DK&country=SE

import re


#API curl example on http://localhost:1001/Screener?sector=Technology&country=FR&country=DK&keywords=Research%20medical&keyword_mode=and
@CompanyFiltering.get("/v1/Screener")
async def filter_companies(
    name: str = Query(None, title="Company Name"),
    sector: List[str] = Query(None, title="Sector"),
    country: List[str] = Query(None, title="Country"),

    industry: List[str] = Query(None, title="Industry"),

    keywords: str = Query(None, title="Keywords"),
    keyword_mode: str = Query("and", title="Keyword Mode")
):
    try:
        query = {}

        if name:
            query["companyName"] = name

        if sector:
            query["sector"] = {"$in": sector}

        if industry:
            query["industry"] = {"$in": industry}


        if country:
            query["country"] = {"$in": country}

        projection = {"companyName": 1, "sector": 1, "industry": 1, "subregion": 1, "country": 1,"description": 1}

        if keywords:
            keywords_list = keywords.split()
            keyword_queries = []

            for keyword in keywords_list:
                description_keyword_regex = re.compile(f".*{keyword}.*", re.IGNORECASE)
                keyword_query = {"description": {"$regex": description_keyword_regex}}
                keyword_queries.append(keyword_query)

            if keyword_mode == "and":
                print("Using 'and' operator")
                print("Main Query:", query)
                print("Keyword Queries:", keyword_queries)
                query["$and"] = keyword_queries

            elif keyword_mode == "or":
                query["$or"] = keyword_queries

            elif keyword_mode == "not":
                query["$nor"] = keyword_queries
        filtered_companies_cursor = CompaniesCollection.find(query, projection=projection)

        # Convert MongoDB documents to a list of dictionaries
        sanitized_companies = []
        for company in filtered_companies_cursor:
            sanitized_company = {
                "companyName": company["companyName"],
                "sector": company.get("sector", None),
                "industry": company.get("industry", None),
                "country": company.get("country", None),
                "description": company.get("description", None),

            }

            # Handle any problematic float values by converting them to JSON-compliant values
            for key, value in sanitized_company.items():
                if isinstance(value, float) and (value == float('inf') or value == float('-inf') or math.isnan(value)):
                    sanitized_company[key] = None  # Replace problematic values with None

            sanitized_companies.append(sanitized_company)

        return sanitized_companies

    except Exception as e:
        return {"error": str(e)}




# Pas trop important
@CompanyFiltering.get("/v1/filterCompanies_HierarchicalLogic")
async def filter_companies(name: str = Query(None, title="Company Name"),
                           sector: str = Query(None, title="Sector"),
                           industry: str = Query(None, title="Industry"),
                           subregion: str = Query(None, title="Subregion"),
                           country: str = Query(None, title="Country"),
                           keywords: str = Query(None, title="Keywords")):

    try:
        # Start with an empty filter
        filters = {}

        if sector:
            filters["sector"] = sector

        projection = {"companyName": 1, "sector": 1, "industry": 1, "subregion": 1, "country": 1}

        cursor = CompaniesCollection.find(filters, projection=projection)

        df = pd.DataFrame(list(cursor))

        if industry:
            df = df[df["industry"] == industry]

        if country:
            df = df[df["country"] == country]

        if keywords:
            description_keyword_regex = f".*{keywords}.*"
            df = df[df["description"].str.contains(description_keyword_regex, case=False, na=False)]

        print(df)


        return "Hierchical Filtering"

    except Exception as e:
        return {"error": str(e)}





# http://localhost:1001/infos/autoCompletete?query=app
@CompanyFiltering.get("/v1/infos/autoCompletete")
async def autocomplete_company_name(query: str):
    if not query:
        return []

    regex_pattern = f".*{query}.*"
    filters = {"companyName": {"$regex": regex_pattern, "$options": "i"}}

    companies_collection = get_companies_collection()
    matching_companies = list(companies_collection.find(filters, {"companyName": 1,"Symbol": 1,"exchange":1,"country":1,"image":1,"_id":1}))


#With the query of the official country name that takes time of requesting the official name, element by element 
    # autocomplete_results = [{"companyName": company["companyName"],"logo": company["image"], "Symbol": company["Symbol"],"exchange": company["exchange"],"country": company["country"],"company_country_name":get_Country_Name_by_id_from_db(company['_id']),"drapeau":f"https://flagcdn.com/w320/{company['country'].lower()}.png" } for company in matching_companies]

#Without the query of the official country name 
    autocomplete_results = [{"companyName": company["companyName"],"logo": company["image"], "Symbol": company["Symbol"],"exchange": company["exchange"],"country": company["country"],"drapeau":f"https://flagcdn.com/w320/{company['country'].lower()}.png" } for company in matching_companies]




    return autocomplete_results






#find_company_by_symbol
def find_Comany_by_symbol(symbol):
    result = CompaniesCollection.find_one({
        'Symbol': symbol
    })
    if result:
        return serializeDict2(result)
    else:
        return "not found "




# Endpoint qui retourne les informations d'une société à partir d'un symbol à partir de notre base de données
@CompanyFiltering.get("/v1/symbol/{symbol}")
async def getCompanyInfoBySymbol(symbol:str):
    CompanyResult=find_Comany_by_symbol(symbol)
    return (CompanyResult)




def get_all_symbols():
    results = CompaniesCollection.find({}, {"Symbol": 1})
    symbols = [result["Symbol"] for result in results]
    return symbols




# Endpoint qui retourne tous les symbôles des sociétés
@CompanyFiltering.get("/v1/Allsymbols")
async def getAllSymbols():
    symbols = get_all_symbols()
    return symbols


