from fastapi import HTTPException, FastAPI, Response, status, APIRouter,Query
from fastapi.responses import JSONResponse
from schemas.Sector import serializeList2

import pandas as pd

import time



import os

from config.db import get_database 




Get_Financial_Info = APIRouter()
api_key = os.getenv("API_KEY")

def get_BalanceSheetAnnual_collection():
    db = get_database()
    Annual_Balance_Sheet=db["Annual_Balance_Sheet"]
    Annual_Balance_Sheet.create_index([("_id", 1)])

    return Annual_Balance_Sheet
BalanceSheetAnnualCollection=get_BalanceSheetAnnual_collection()




def get_BalanceSheetQuarter_collection():
    db = get_database()
    Quarter_Balance_Sheet=db["Quarter_Balance_Sheet"]
    Quarter_Balance_Sheet.create_index([("_id", 1)])

    return Quarter_Balance_Sheet
BalanceSheetQuarterCollection=get_BalanceSheetQuarter_collection()




def get_BalanceCashFlowAnnual_collection():
    db = get_database()
    Annual_Cash_Flow=db["Annual_Cash_Flow"]
    Annual_Cash_Flow.create_index([("_id", 1)])

    return Annual_Cash_Flow
CashFlowAnnualCollection=get_BalanceCashFlowAnnual_collection()




def get_CashFlowQuarter_collection():
    db = get_database()
    Quarter_Cash_Flow=db["Quarter_Cash_Flow"]
    Quarter_Cash_Flow.create_index([("_id", 1)])

    return Quarter_Cash_Flow
CashFlowQuarterCollection=get_CashFlowQuarter_collection()




def get_IncomeStatementAnnual_collection():
    db = get_database()
    Annual_Income_Statement=db["Annual_Income_Statement"]
    Annual_Income_Statement.create_index([("_id", 1)])

    return Annual_Income_Statement
IncomeStatementAnnualCollection=get_IncomeStatementAnnual_collection()




def get_IncomeStatementQuarter_collection():
    db = get_database()
    Quarter_Income_Statement=db["Quarter_Income_Statement"]
    Quarter_Income_Statement.create_index([("_id", 1)])

    return Quarter_Income_Statement
IncomeStatementQuarterCollection=get_IncomeStatementQuarter_collection()







@Get_Financial_Info.get('/AllBS')
async def find_all_sectors():
    return serializeList2(BalanceSheetAnnualCollection.find())



import time
from datetime import datetime

def get_all_BalanceSheetAnnual_Data(symbol,start_date, end_date,maxElem,StatementType,Frequency):
    try:

        if(start_date):
            print(f">>> start_date: {start_date}, end_date: {end_date}, maxElem: {maxElem}, StatementType: {StatementType}, Frequency: {Frequency}")


        if(not start_date):
            start_date="1950-01-01"

        if(not end_date):
            current_date = datetime.now()
            formatted_current_date = current_date.strftime("%Y-%m-%d")
            end_date=formatted_current_date
        
        if(not maxElem):
            maxElem="1000"

        if(not StatementType):
            raise HTTPException(status_code=404, detail="Statement type required")
 
        if(not Frequency):
            raise HTTPException(status_code=404, detail="Frequency type required")

        int_maxElem = int(maxElem)


        if StatementType=="BS" and Frequency=="A":
            Collection=BalanceSheetAnnualCollection

        if StatementType=="BS" and Frequency=="Q":
            Collection=BalanceSheetQuarterCollection






        if StatementType=="PL" and Frequency=="A":
            Collection=IncomeStatementAnnualCollection

        if StatementType=="PL" and Frequency=="Q":
            Collection=IncomeStatementQuarterCollection






        if StatementType=="CF" and Frequency=="A":
            Collection=CashFlowAnnualCollection

        if StatementType=="CF" and Frequency=="Q":
            Collection=CashFlowQuarterCollection




        all_BalanceSheetAnnual = Collection.find({
            "$and": [
                {"date": {"$gte": start_date, "$lte": end_date}},
                {"symbol": symbol}  
            ]
        }).sort("date", 1).limit(int_maxElem)



        if not all_BalanceSheetAnnual:
            return []

        start_time = time.time()
        result = serializeList2(all_BalanceSheetAnnual)
        end_time = time.time()
        print(f"Elapsed Time: {end_time - start_time:.2f} seconds")

        return result
    except Exception as e:
        raise e




from datetime import datetime


#A curl example to this api 
#http://localhost:1001/get_all_BalanceSheetAnnual?symbol=GOOS.TO&start_date_str=2018-01-05&end_date_str=2022-01-05&limit=10&StatementType=PL&Frequency=Q
@Get_Financial_Info.get('/get_all_BalanceSheetAnnual')
def get_balance_sheet_annual(
    symbol: str = Query(None, title="symbol"),
    start_date_str: str = Query(None, title="start_date_str"),
    end_date_str: str = Query(None, title="end_date_str"),
    limit: str = Query(None, title="limit"),
    StatementType: str = Query(None, title="StatementType"),
    Frequency: str = Query(None, title="Frequency"),


):
    try:


        return get_all_BalanceSheetAnnual_Data(symbol,start_date_str, end_date_str,limit,StatementType,Frequency)
    except Exception as e:
        raise e