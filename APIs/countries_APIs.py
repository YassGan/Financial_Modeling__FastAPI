from config.db import get_database 
from fastapi import APIRouter,Response
from schemas.Industry import serializeList
import pandas as pd
import requests
import os


import asyncio
import aiohttp




Country=APIRouter()



@Country.get('/Hello_countryAPIs')
async def Hello_countryPage():
    return 'Hello country Page'




def get_countries_collection():
    db = get_database()

    countries=db["countries"]  
    countries.create_index([("country", 1)], unique=True)
    return countries

countriesCollection=get_countries_collection()






def get_subregion_collection():
    db = get_database()
    subregions=db["subregions"]  
    subregions.create_index([("subregion", 1)], unique=True)
    return subregions






#find_Country_id_by_name
def find_Country_id_by_name(isoCode):
    result = countriesCollection.find_one({
        'country': isoCode
    })
    
    if result:
        return str(result['_id'])
    else:
        return "not found "
    
# API that calls the find_Country_id_by_name
@Country.get("/countryMongoId/{isoCode}")
def get_country(isoCode: str):
    countryid = find_Country_id_by_name(isoCode)
    return countryid







# Function that creates subregions
def CreatingSubregion(DataFrame):

    # Replace 'UK' with 'GB' in the 'country' column
    DataFrame['country'] = DataFrame['country'].replace('UK', 'GB')
    DataFrame_Countries = DataFrame[['country']]  

    DataFrame_Countries=DataFrame_Countries.drop_duplicates()
    DataFrame_Countries.dropna(inplace=True)

    print(len(DataFrame_Countries))


    DataFrame_Countries['subregion']=DataFrame_Countries['country'].apply(get_country_Subregion)
    DataFrame_Subregion=pd.DataFrame()
    DataFrame_Subregion['subregion']=DataFrame_Countries['subregion'].drop_duplicates()
    


    if not DataFrame_Subregion.empty:
            new_subregions = DataFrame_Subregion.to_dict(orient='records')
            
            existing_subregions = set(get_subregion_collection().distinct("subregion"))
            new_subregions_to_create = [subregion for subregion in new_subregions if subregion.get("subregion") not in existing_subregions]

            if new_subregions_to_create:
                get_subregion_collection().insert_many(new_subregions_to_create)
                print (f"---------->  {len(new_subregions_to_create)} new subregions created successfully.")
            else:
                print( "---------> No new subregions to create.")
                print( "---------> No new subregions to create.")


    else:
            print( "---------> No new subregions to create.")
            print( "---------> No new subregions to create.")
            print( "---------> No new subregions to create.")

    






@Country.get("/CreatingSubregions")
def CreatingSubregion_API():
    DataFrame = pd.read_csv(os.getenv("CSV_FILE"), encoding='utf-8')

    CreatingSubregion(DataFrame)
    return 'CreatingSubregion_API'
    



# Function that returns all the subregions from the database
def gettingAllSubregions():
    return serializeList(get_subregion_collection().find())


#API to get all the industries from the database
@Country.get('/AllSubregions')
async def AllSubregions_API():
    return gettingAllSubregions()


# Function that returns all the subregions from the database
def gettingAllCountries():
    return serializeList(get_countries_collection().find())


#API to get all the AllSubregions from the database
@Country.get('/AllCountries')
async def AllCountries_API():
    return gettingAllCountries()




#find_Country_id_by_name
def find_subregion_id_by_name(subregionName):


    subregionCollection=get_subregion_collection()
    result = subregionCollection.find_one({
        'subregion': subregionName
    })
    
    if result:
        return str(result['_id'])
    else:
        return "subregion not found "
    
# API that calls the find_Country_id_by_name
@Country.get("/subregionMongoId/{subregion}")
def get_subregion(subregion: str):
    subregionid = find_subregion_id_by_name(subregion)
    return subregionid



#find_Country_id_by_name
def find_subregion_id_by_Countryname(countryName):
    if isinstance(countryName, str):  # Check if it's a valid string
        countryNameMajus = countryName.upper()
        result = countriesCollection.find_one({
            'country': countryNameMajus
        })
        
        if result:
            return str(result['_id'])
        else:
            return "subregion not found"
    else:
        return "invalid country name"

    
# API that calls the find_Country_id_by_name
@Country.get("/subregionMongoIdbyCountry/{countryName}")
def get_subregion(countryName: str):
    countryNameMajus=countryName.upper()

    subregionid = find_subregion_id_by_Countryname(countryNameMajus)
    return subregionid







# Function that takes the ISO code of the country and returns the flag URL
def CountryFlag(isoCode):

    if isinstance(isoCode, str):
        lowerIsoCode=isoCode.lower()
        return f'https://flagcdn.com/w320/{lowerIsoCode}.png'
    else:
        # Handle cases when isoCode is not a string (e.g., it's a float or NaN)
        return "No"  # Or return a default flag or handle the case accordingly





# Endpoint to get the flag URL of a country by its ISO code
@Country.get("/flag/{isoCode}")
def get_flag(isoCode: str):
    flag_url = CountryFlag(isoCode)
    return flag_url



# Function that returns the official name of a country by its isoCode
def get_country_name(iso_code):
    base_url = "https://restcountries.com/v3/alpha/"
    url = f"{base_url}{iso_code}"
    response = requests.get(url)
    
    if response.status_code == 200:
        country_data = response.json()
        return country_data[0]['name']['common']
    else:
        return "Country not found"

# Endpoint to get the name of a country by its ISO code
@Country.get("/countryName/{isoCode}")
def get_CountryOfficialName(isoCode: str):
    return(get_country_name(isoCode))





# Function that returns the currency name of a country by its isoCode
def get_country_currency(iso_code):
    url = f"https://restcountries.com/v2/alpha/{iso_code}"
    response = requests.get(url)
    
    if response.status_code == 200:
        country_data = response.json()
        return country_data['currencies'][0]['name']
    else:
        return "Country not found"

# Endpoint to get the name of a country by its ISO code
@Country.get("/countryCurrency/{isoCode}")
def get_CountryCurrency(isoCode: str):
    return(get_country_currency(isoCode))




# Function that returns the region name of a country by its isoCode
def get_country_region(iso_code):
    url = f"https://restcountries.com/v2/alpha/{iso_code}"
    response = requests.get(url)
    
    if response.status_code == 200:
        country_data = response.json()
        return country_data['region']
    else:
        return "Country not found"

# Endpoint to get the region of a country by its ISO code
@Country.get("/countryRegion/{isoCode}")
def get_CountryRegion(isoCode: str):
    print( get_country_region(isoCode))
    return(get_country_region(isoCode))






#function that returns the region name of a country by its isoCode
def get_country_Subregion(iso_code):

    url = f"https://restcountries.com/v2/alpha/{iso_code}"
    response = requests.get(url)
    
    if response.status_code == 200:
        country_data = response.json()
        return country_data['subregion']
    else:
        return "Country not found"
    




# Endpoint to get the region of a country by its ISO code
@Country.get("/countrySubRegion/{isoCode}")
def get_CountrySubRegion(isoCode: str):
    print( get_country_Subregion(isoCode))
    return(get_country_Subregion(isoCode))



from multiprocessing import Pool

import time

def CreatingCountries(DataFrame):
    start_time_total = time.time()

    DataFrame['country'] = DataFrame['country'].replace('UK', 'GB')

    # Cleaning the information of the countries (dropping duplicates, removing empty values) 
    Uniquecountries = DataFrame['country'].drop_duplicates()

    UniquecountriesDF = Uniquecountries.to_frame()
    UniquecountriesDF.dropna(inplace=True)

    # Replace 'UK' with 'GB' in the 'country' column

    # Working with Pool for parallelism to make things turn faster
    with Pool(processes=4) as pool:
        start_time_pool = time.time()

        UniquecountriesDF['official_name'] = pool.map(get_country_name, UniquecountriesDF['country'])
        UniquecountriesDF['flag'] = pool.map(get_flag, UniquecountriesDF['country'])
        UniquecountriesDF['currency'] = pool.map(get_country_currency, UniquecountriesDF['country'])
        UniquecountriesDF['region'] = pool.map(get_country_region, UniquecountriesDF['country'])
        UniquecountriesDF['subregion'] = pool.map(get_country_Subregion, UniquecountriesDF['country'])

        end_time_pool = time.time()
        elapsed_time_pool = end_time_pool - start_time_pool
        print(f"Time spent in Pool: {elapsed_time_pool:.2f} seconds")

    sectors = gettingAllSubregions()
    UniquecountriesList = []

    for index, row in UniquecountriesDF.iterrows():
        start_time_row = time.time()

        subregion_name = row['subregion']
        for sector_obj in sectors:
            if sector_obj['subregion'] == subregion_name:
                UniquecountriesList.append({
                    "official_name": row['official_name'],
                    "flag": row['flag'],
                    "country": row['country'],
                    "currency": row['currency'],
                    "region": row['region'],
                    "subregion": row['subregion'],
                    "subregionId": sector_obj['_id']
                })

        end_time_row = time.time()
        elapsed_time_row = end_time_row - start_time_row
        print(f"Time spent in processing rows: {elapsed_time_row:.2f} seconds")

    UniquecountriesDF = pd.DataFrame(UniquecountriesList)

    if not UniquecountriesDF.empty:
        start_time_insert = time.time()

        new_countries = UniquecountriesDF.to_dict(orient='records')
        existing_countries = set(countriesCollection.distinct("country"))
        new_countries_to_create = [country for country in new_countries if country["country"] not in existing_countries]

        if new_countries_to_create:
            countriesCollection.insert_many(new_countries_to_create)
            print(f"------------> {len(new_countries_to_create)} new countries created successfully.")
        else:
            print("- No new countries to create.")

        end_time_insert = time.time()
        elapsed_time_insert = end_time_insert - start_time_insert
        print(f"Time spent in inserting into MongoDB: {elapsed_time_insert:.2f} seconds")

    else:
        print("- No new countries to create.")

    end_time_total = time.time()
    elapsed_time_total = end_time_total - start_time_total
    print(f"Total time: {elapsed_time_total:.2f} seconds")


    



#API that launches the function CreatingCountries 
@Country.get('/CreatingCountries')
async def CountriesListAPI():   
    DataFrame = pd.read_csv(os.getenv("CSV_FILE"), encoding='utf-8')
 
    CreatingCountries(DataFrame)
    return("CountriesListAPI")