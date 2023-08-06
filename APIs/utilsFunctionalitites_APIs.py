from fastapi import APIRouter,Response
from config.db import get_database 
import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
load_dotenv()

import os

api_key = os.getenv("API_KEY")
UtilsFunc=APIRouter()








@UtilsFunc.get('/Read_CSV')
async def Read_CSV():
    #Reading the csv file 
    global DataFrame
    DataFrame = pd.read_csv(os.getenv("OUTPUT_CSV_FILE"), encoding='utf-8')
    print('Printing Data Frame ------------')
    print(DataFrame)
    print('Printing Data Frame ------------')





#function that downloads the csv file from an api 
def download_csv_from_url(url, file_path):
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()  

            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0

            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
                    downloaded_size += len(chunk)
                    progress = (downloaded_size / total_size) * 100
                    print(f"Download progress: {progress:.2f}%")

        print("File downloaded successfully.")
        return True
    except requests.exceptions.RequestException as e:
        print("Request Error:", e)
        return False
    except IOError as e:
        print("File Error:", e)
        return False
    


#API that once is launched it downloads the csv file from the online api of the modeling website    
@UtilsFunc.get('/download_csv')
async def download_csv():
            # url = f"https://financialmodelingprep.com/api/v3/profile/{Symbol}?apikey={api_key}"

    url = f"https://financialmodelingprep.com/api/v4/profile/all?apikey={api_key}"
    print(url)
    file_path = os.getenv("CSV_FILE") 

    success = download_csv_from_url(url, file_path)
    if success:
        return Response(content="File downloaded successfully.", media_type="text/plain")
    else:
        return Response(content="Failed to download the file.", media_type="text/plain")


# API to create a new .csv file that has the first elements of the big dataframe file 
def create_csv_with_first_elements(Number,input_file_path, output_file_path):
    try:
        df = pd.read_csv(input_file_path)

        first_5_elements = df.head(Number)

        first_5_elements.to_csv(output_file_path, index=False)

        print("- File with new elements created successfully.")
        return True
    except pd.errors.EmptyDataError:
        print("Error: Input CSV file is empty.")
        return False
    except FileNotFoundError:
        print("Error: Input CSV file not found.")
        return False
    except Exception as e:
        print("Error:", e)
        return False

# create_csv_with_first_elements(15,'data.csv','data_with_17.csv')





# API that launches the create_csv_with_first_elements function
@UtilsFunc.get('/create_csv_with_first_elements/{Number}')
async def create_csv_endpoint(Number: int):
    input_file_path = 'data.csv' 
    output_file_path = os.getenv("SMALL_OUTPUT_CSV_FILE") 

    result = create_csv_with_first_elements(Number, input_file_path, output_file_path)

    if result:
        return Response(status_code=200, content="File with new elements created successfully.")
    else:
        return Response(status_code=400, content="Failed to create the file.")



# API that creates a json file from a dataframe, but the csv file should be read first
@UtilsFunc.get('/DownloadFirstElemAsJson/{Number}')
async def download_first_10_as_json(Number: int):
    DataFrame = pd.read_csv(os.getenv("CSV_FILE"), encoding='utf-8')

    first_10_df = DataFrame.head(Number)  
    json_data = first_10_df.to_json(orient='records', lines=True) 

   
    filename = "firstlements.json"

    with open(filename, 'w', encoding='utf-8') as file:
        file.write(json_data)

    response = Response(content=json_data, media_type='application/json')
    response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response





@UtilsFunc.get('/PrintingDataFrame')
async def find_all_industries():
    DataFrame = pd.read_csv(os.getenv("CSV_FILE"), encoding='utf-8')
    # Si vous souhaitez compter le nombre de duplications
    count_duplicates = DataFrame.duplicated().sum()
    unique_combinations = DataFrame[['industry', 'sector']]
    print('unique_combinations ',unique_combinations)

    unique_sectors = DataFrame.drop_duplicates(subset=['industry'])

    print('Unique sectors ')
    print(unique_sectors)
    np.savetxt('Unique_sectors.csv', unique_sectors, delimiter=';', fmt='%s', encoding='utf-8')



    unique_industries = DataFrame["industry"].unique()
    print('unique_industries  ')
    print(unique_industries)
    np.savetxt('unique_industries.csv', unique_industries, delimiter=';', fmt='%s')

    return 'PrintingDataFrame'

