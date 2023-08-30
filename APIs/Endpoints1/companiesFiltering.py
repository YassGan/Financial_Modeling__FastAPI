
from fastapi import APIRouter,Response,Query

from config.db import get_database 

import pymongo 
CompanyFiltering=APIRouter()


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
# http://localhost:8000/filterCompanies?name=CompanyName&sector=Technology&industry=Software&subregion=North&country=USA&keywords=AI"

#Curl of many fields for example countries http://localhost:1001/filterCompanies_Filtering?country=FR&country=DK&country=SE

import re

@CompanyFiltering.get("/filterCompanies_Filtering")
async def filter_companies(name: str = Query(None, title="Company Name"),
                            sector: List[str] = Query(None, title="Sector"),
                            industry: List[str] = Query(None, title="Industry"),
                            subregion: List[str] = Query(None, title="Subregion"),
                            country: List[str] = Query(None, title="Country"),
                            keywords: List[str] = Query(None, title="Keywords")):

    filters = {}

    if name:
        filters["companyName"] = name

    if sector:
        filters["sector"] = {"$in": sector}

    if industry:
        filters["industry"] = {"$in": industry}

    if subregion:
        filters["subregion"] = {"$in": subregion}

    if country:
        filters["country"] = {"$in": country}

    if keywords:
        search_string = " ".join(keywords)
        filters["$text"] = {"$search": search_string}


    projection = {"_id": 0, "companyName": 1, "sector": 1, "industry": 1, "description": 1}

    try:
        filtered_companies = CompaniesCollection.find(filters, projection)
        filtered_companies = CompaniesCollection.find(filters, projection)
        explanation = filtered_companies.explain()
        print(">>>>>>>>> explanation")
        print(explanation)

        return list(filtered_companies)

    except Exception as e:
        return {"error": str(e)}
    






@CompanyFiltering.get("/autocompleteCompanyName")
async def autocomplete_company_name(query: str):
    if not query:
        return []

    regex_pattern = f".*{query}.*"
    filters = {"companyName": {"$regex": regex_pattern, "$options": "i"}}

    companies_collection = get_companies_collection()
    matching_companies = list(companies_collection.find(filters, {"companyName": 1}))

    autocomplete_results = [company["companyName"] for company in matching_companies]

    return autocomplete_results




