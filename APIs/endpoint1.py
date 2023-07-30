from fastapi import APIRouter,Response
import requests



router = APIRouter() 


@router.get('/hello')
async def hello():
    return 'hello'


import requests

url = "https://financialmodelingprep.com/api/v4/profile/all?apikey=96051dba5181978c2f0ce23c1ef4014b"


def download_csv_from_url(url, file_path):
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()  # Raise an error if the response status is not 200

            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

        print("File downloaded successfully.")
    except requests.exceptions.RequestException as e:
        # Handle any request-related exceptions (e.g., connection error, timeout)
        print("Request Error:", e)
    except IOError as e:
        # Handle file-related errors (e.g., file path not accessible)
        print("File Error:", e)

# Example usage:
url = "https://financialmodelingprep.com/api/v4/profile/all?apikey=96051dba5181978c2f0ce23c1ef4014b"
file_path = "data.csv"  # Change this to the desired file path where you want to save the CSV file

download_csv_from_url(url, file_path)

