from fastapi import APIRouter, Response
import requests

router = APIRouter()

@router.get('/hello')
async def hello():
    return 'hello'



def download_csv_from_url(url, file_path):
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()  # Raise an error if the response status is not 200

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
        # Handle any request-related exceptions (e.g., connection error, timeout)
        print("Request Error:", e)
        return False
    except IOError as e:
        # Handle file-related errors (e.g., file path not accessible)
        print("File Error:", e)
        return False




@router.get('/download_csv')
async def download_csv():
    url = "https://financialmodelingprep.com/api/v4/profile/all?apikey=96051dba5181978c2f0ce23c1ef4014b"
    file_path = "data.csv"  # Change this to the desired file path where you want to save the CSV file

    success = download_csv_from_url(url, file_path)
    if success:
        # Return a response with a download link to the client
        return Response(content="File downloaded successfully.", media_type="text/plain")
    else:
        return Response(content="Failed to download the file.", media_type="text/plain")
