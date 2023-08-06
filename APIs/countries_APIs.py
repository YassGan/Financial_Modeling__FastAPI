from config.db import get_database 
from fastapi import APIRouter,Response
from schemas.Industry import serializeList
import pandas as pd
import requests

Country=APIRouter()



@Country.get('/Hello_countryAPIs')
async def Hello_countryPage():
    return 'Hello country Page'




def get_countries_collection():
    db = get_database()
    return db["countries"]



def get_subregion_collection():
    db = get_database()
    return db["subregions"]    


#find_Country_id_by_name
def find_Country_id_by_name(isoCode):
    countriesCollection=get_countries_collection()
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
def CreatingSubregion():
    DataFrame = pd.read_csv("data.csv", encoding='utf-8')
    DataFrame_Countries = DataFrame[['country']]  

    DataFrame_Countries=DataFrame_Countries.drop_duplicates()

    DataFrame_Countries['subregion']=DataFrame_Countries['country'].apply(get_country_Subregion)
    DataFrame_Subregion=pd.DataFrame()
    DataFrame_Subregion['subregion']=DataFrame_Countries['subregion'].drop_duplicates()
    


    if not DataFrame_Subregion.empty:
            new_subregions = DataFrame_Subregion.to_dict(orient='records')
            
            existing_subregions = set(get_subregion_collection().distinct("subregion"))
            new_subregions_to_create = [subregion for subregion in new_subregions if subregion.get("subregion") not in existing_subregions]

            if new_subregions_to_create:
                get_subregion_collection().insert_many(new_subregions_to_create)
                print (f"- {len(new_subregions_to_create)} new subregions created successfully.")
            else:
                print( "- No new subregions to create.")
    else:
            print( "- No new subregions to create.")





@Country.get("/CreatingSubregions")
def CreatingSubregion_API():
    CreatingSubregion()
    return 'CreatingSubregion_API'
    



# Function that returns all the subregions from the database
def gettingAllSubregions():
    return serializeList(get_subregion_collection().find())


#API to get all the industries from the database
@Country.get('/AllSubregions')
async def AllSubregions_API():
    return gettingAllSubregions()


   




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
    countryNameMajus=countryName.upper()

    subregionCollection=get_countries_collection()
    result = subregionCollection.find_one({
        'country': countryNameMajus
    })
    
    if result:
        return str(result['_id'])
    else:
        return "subregion not found "
    
# API that calls the find_Country_id_by_name
@Country.get("/subregionMongoIdbyCountry/{countryName}")
def get_subregion(countryName: str):
    countryNameMajus=countryName.upper()

    subregionid = find_subregion_id_by_Countryname(countryNameMajus)
    return subregionid







# Function that takes the ISO code of the country and returns the flag URL
def CountryFlag(isoCode):
    lowerIsoCode=isoCode.lower()
    return f'https://flagcdn.com/w320/{lowerIsoCode}.png'

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
    UniquecountriesDF['currency']=UniquecountriesDF['country'].apply(get_country_currency)
    UniquecountriesDF['region']=UniquecountriesDF['country'].apply(get_country_region)
    UniquecountriesDF['subregion']=UniquecountriesDF['country'].apply(get_country_Subregion)


    sectors=gettingAllSubregions()
    UniquecountriesList=[]

    for index, row in UniquecountriesDF.iterrows():
        subregion_name = row['subregion']
        for sector_obj in sectors:
            if sector_obj['subregion'] == subregion_name:
                UniquecountriesList.append({
                    "official_name": row['official_name'],
                    "flag": row['flag'],
                    "country":row['country'],
                    "currency": row['currency'],
                    "region": row['region'],
                    "subregion": row['subregion'],
                    "subregionId":sector_obj['_id']
                })
    print("->->->->->->->->")
    print(UniquecountriesList)

    UniquecountriesDF=pd.DataFrame(UniquecountriesList)
    print("->->->->->->->->")
    print(UniquecountriesDF)


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

