from fastapi import FastAPI, HTTPException, APIRouter, Query
from fastapi.responses import JSONResponse
from google.oauth2 import service_account
from googleapiclient.discovery import build

import pandas as pd


googleSheetRouter = APIRouter()

# Load credentials from the JSON file you downloaded from GCP
credentials = service_account.Credentials.from_service_account_file(
    'config/GoogleSheetAPI_JSON/GoogleSheetAPI_JSON.json',
    scopes=['https://www.googleapis.com/auth/spreadsheets']
)

# Create a Google Sheets API service
service = build('sheets', 'v4', credentials=credentials)

# Example endpoint to get data from Google Sheets
#### endpoint example http://localhost:1002/get_data_from_sheets?sheet_id=18fv1_nvo2WW9jgC5hzjrZpgqIf4PZbgPX2Sxc1_nt5c&range_name=A1:B10

@googleSheetRouter.get("/get_data_from_sheets")
async def get_data_from_sheets(sheet_id: str = Query(..., description="ID of the Google Sheet"), range_name: str = Query(..., description="Name of the range")):
    try:
        # Call the Google Sheets API to get values from a specific range
        result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
        values = result.get('values', [])

        return JSONResponse(content={"values": values}, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




def update_googleSheet_data_in(sheet_id: str, symbol: str, date: str):
    try:
    

        # Call the Google Sheets API to get values from a specific range
        result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range="Sheet1").execute()
        values = result.get('values', [])

        # Convert the values to a DataFrame
        df = pd.DataFrame(values[1:], columns=values[0])

        if symbol in df['symbol'].values:
            df.loc[df['symbol'] == symbol, 'date'] = date
        else:
            new_entry = {'symbol': symbol, 'date': date}
            new_df = pd.DataFrame([new_entry])
            df = pd.concat([df, new_df], ignore_index=True)

        # Convert the DataFrame back to values
        updated_values = [df.columns.tolist()] + df.values.tolist()

        # Update the data in the Google Sheet
        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range="Sheet1",
            body={'values': updated_values},
            valueInputOption="RAW"
        ).execute()

        print("update_googleSheet_data_in updated successfully.")

    except Exception as e:
        print(f"Error updating data in Google Sheets: {e}")

# # Example usage:
# update_googleSheet_data_in("18fv1_nvo2WW9jgC5hzjrZpgqIf4PZbgPX2Sxc1_nt5c", "AAPL", "2023-01-01")









def read_data_from_sheets(sheet_id: str, range_name: str):
    try:
  
        result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
        values = result.get('values', [])

        if not values:
            print("No data found.")
            return None

        # Convert the values to a DataFrame
        df = pd.DataFrame(values[1:], columns=values[0])

        return df

    except Exception as e:
        print(f"Error reading data from Google Sheets: {e}")
        return None
    
# Example usage:
# sheet_id = "18fv1_nvo2WW9jgC5hzjrZpgqIf4PZbgPX2Sxc1_nt5c"
# range_name = "YourRange"  # Replace with the actual range in your Google Sheet
# SymbolDateQuotesDF = read_data_from_sheets(sheet_id, range_name)

# if SymbolDateQuotesDF is not None:
#     # Your DataFrame is now loaded with data from Google Sheets
#     print(SymbolDateQuotesDF)