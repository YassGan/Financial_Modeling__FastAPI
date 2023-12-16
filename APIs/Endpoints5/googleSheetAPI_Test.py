from fastapi import FastAPI, HTTPException, APIRouter, Query
from fastapi.responses import JSONResponse
from google.oauth2 import service_account
from googleapiclient.discovery import build

googleSheetRouter = APIRouter()

# Load credentials from the JSON file you downloaded from GCP
credentials = service_account.Credentials.from_service_account_file(
    'config/GoogleSheetAPI_JSON/GoogleSheetAPI_JSON.json',
    scopes=['https://www.googleapis.com/auth/spreadsheets']
)

# Create a Google Sheets API service
service = build('sheets', 'v4', credentials=credentials)

# Example endpoint to get data from Google Sheets
@googleSheetRouter.get("/get_data_from_sheets")
async def get_data_from_sheets(sheet_id: str = Query(..., description="ID of the Google Sheet"), range_name: str = Query(..., description="Name of the range")):
    try:
        # Call the Google Sheets API to get values from a specific range
        result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
        values = result.get('values', [])

        return JSONResponse(content={"values": values}, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
