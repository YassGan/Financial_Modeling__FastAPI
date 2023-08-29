
from fastapi import APIRouter,Response,Query

from config.db import get_database 

 
CompanyFiltering=APIRouter()





def get_companies_collection():
    db = get_database()
    companies=db["companies"]
    companies.create_index([("Symbol", 1)], unique=True)

    return companies


CompaniesCollection=get_companies_collection()



from bson.json_util import dumps
# API endpoint to filter companies based on name and sector
# Example on how to use this api 
# http://localhost:8000/filterCompanies?name=CompanyName&sector=Technology&industry=Software&subregion=North&country=USA&keywords=AI"
@CompanyFiltering.get("/filterCompanies_Filtering")
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
        filtered_companies = list(CompaniesCollection.find(filters))

        # Sanitize data and remove non-serializable fields
        sanitized_companies = []
        for company in filtered_companies:
            sanitized_company = {}
            for key, value in company.items():
                if isinstance(value, (int, str, bool, dict, list)):
                    sanitized_company[key] = value
            sanitized_companies.append(sanitized_company)

        return sanitized_companies

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




