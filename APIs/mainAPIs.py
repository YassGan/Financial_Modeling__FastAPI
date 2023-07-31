
from fastapi import APIRouter,Response
from models.Industry import Industry 
from config.db import get_database 

import pandas as pd
import requests


Main=APIRouter()


DataFrame=pd.DataFrame()


def get_industry_collection():
    db = get_database()
    return db["industries"]

@Main.get('/Hello_MainPage')
async def Hello_MainPage():
    return 'Hello Main Page'


@Main.get('/Read_CSV')
async def Read_CSV():
    #Reading the csv file 
    global DataFrame
    DataFrame = pd.read_csv("output_csv_file.csv", encoding='utf-8')
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
@Main.get('/download_csv')
async def download_csv():
    url = "https://financialmodelingprep.com/api/v4/profile/all?apikey=96051dba5181978c2f0ce23c1ef4014b"
    file_path = "data.csv"  

    success = download_csv_from_url(url, file_path)
    if success:
        return Response(content="File downloaded successfully.", media_type="text/plain")
    else:
        return Response(content="Failed to download the file.", media_type="text/plain")



# Function to fetch comany's data from the API by its symbol
def fetch_data_from_api_bySymbol(Symbol):
    api_url = f"https://financialmodelingprep.com/api/v3/profile/{Symbol}?apikey=96051dba5181978c2f0ce23c1ef4014b"
    response = requests.get(api_url)
    return response.json()


# Define the API route to fetch data from the API
@Main.get("/getCompanyBySymbol/{Symbol}")
def getCompanyBySymbol(Symbol: str):
    data = fetch_data_from_api_bySymbol(Symbol)
    return data








# Function to create new industries from the DataFrame efficiently
def create_industries_from_dataframe(DataF):
    Industries_variables = DataF[['Symbol','sector', 'companyName']]  # Accessing multiple columns

    if not DataF.empty:
        new_industries = Industries_variables.to_dict(orient='records')
        
        existing_symbols = set(get_industry_collection().distinct("Symbol"))
        new_industries_to_create = [industry for industry in new_industries if industry["Symbol"] not in existing_symbols]

        if new_industries_to_create:
            get_industry_collection().insert_many(new_industries_to_create)
            return f"{len(new_industries_to_create)} new industries created successfully."
        else:
            return "No new industries to create."
    else:
        return "No new industries to create."


# API to create new industries from the DataFrame efficiently
@Main.post('/CreateIndustriesFromDataFrame')
async def create_industries_api():
    DataF = pd.read_csv("data_with_17.csv", encoding='utf-8')
    result = create_industries_from_dataframe(DataF)
    if "new industries created successfully" in result:
        return Response(status_code=201, content=result)
    else:
        return Response(status_code=200, content=result)



# API to create a new .csv file that has the first elements of the big dataframe file 
def create_csv_with_first_elements(Number,input_file_path, output_file_path):
    try:
        df = pd.read_csv(input_file_path)

        first_5_elements = df.head(Number)

        first_5_elements.to_csv(output_file_path, index=False)

        print("File with first  elements created successfully.")
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






@Main.post('/create_csv_with_first_elements/{Number}')
async def create_csv_endpoint(Number: int):
    input_file_path = "data.csv"
    output_file_path = "data_with_17.csv"

    result = create_csv_with_first_elements(Number, input_file_path, output_file_path)

    if result:
        return Response(status_code=200, content="File created successfully.")
    else:
        return Response(status_code=400, content="Failed to create the file.")








from apscheduler.schedulers.background import BackgroundScheduler

def run_job():
    DataF = pd.read_csv("data_with_17.csv", encoding='utf-8')
    result = create_industries_from_dataframe(DataF)
    print(result)

# Define the scheduler function
def run_job():
    DataF = pd.read_csv("data_with_17.csv", encoding='utf-8')
    result = create_industries_from_dataframe(DataF)
    print(result)

# Create the scheduler
scheduler = BackgroundScheduler()

# Add the job to the scheduler with max_instances set to 1
scheduler.add_job(run_job, trigger='interval', seconds=10, max_instances=1)

# Start the scheduler
scheduler.start()