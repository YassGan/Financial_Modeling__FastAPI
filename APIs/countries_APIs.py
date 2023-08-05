from config.db import get_database 
from fastapi import APIRouter,Response

import pandas as pd
import requests

Country=APIRouter()



@Country.get('/Hello_countryAPIs')
async def Hello_countryPage():
    return 'Hello country Page'




def get_countries_collection():
    db = get_database()
    return db["countries"]





# Function that takes the ISO code of the country and returns the flag URL
def CountryFlag(isoCode):
    lowerIsoCode=isoCode.lower()
    return f'https://flagcdn.com/w320/{lowerIsoCode}.png'

# Endpoint to get the flag URL of a country by its ISO code
@Country.get("/flag/{isoCode}")
def get_flag(isoCode: str):
    flag_url = CountryFlag(isoCode)
    return flag_url



#function that returns the official name of a country by its isoCode
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
    print( get_country_name(isoCode))
    return(get_country_name(isoCode))



#Function that gets all the countries of the dataframe and insert them in the database with a url of thier flag and their official name
def CreatingCountries():


    DataFrame = pd.read_csv("data.csv", encoding='utf-8')



    #cleaning the information of the countries (dropping duplicants, removing empty values) 
    Uniquecountries = DataFrame['country'].drop_duplicates()
    UniquecountriesDF=Uniquecountries.to_frame()
    UniquecountriesDF.dropna(inplace=True)
    print("CleanedCountriesInformation")
    print(UniquecountriesDF)




    # Adding the columns of the official name of the country as well as its flag
    UniquecountriesDF['official_name'] = UniquecountriesDF['country'].apply(get_country_name)
    UniquecountriesDF['flag']=UniquecountriesDF['country'].apply(get_flag)

    print(UniquecountriesDF)
    print(type(UniquecountriesDF))



    if not UniquecountriesDF.empty:
        new_countries = UniquecountriesDF.to_dict(orient='records')
        
        existing_countries = set(get_countries_collection().distinct("country"))
        new_countries_to_create = [country for country in new_countries if country["country"] not in existing_countries]

        if new_countries_to_create:
            get_countries_collection().insert_many(new_countries_to_create)
            print( f"- {len(new_countries_to_create)} new countries created successfully.")
        else:
            print( "- No new countries to create.")
    else:
        print( "- No new countries to create.")
    



#API that launches the function CreatingCountries 
@Country.get('/CreatingCountries')
async def CountriesListAPI():    
    CreatingCountries()
    return("CountriesListAPI")

