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








import calendar

def LTM_date_adjuster(date_str):
    try:
        # Convert the input date string to a datetime object
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")

        
        year = date_obj.year
        month = date_obj.month

        print(f"The extracted month of the entered date is  {month}  and its type is {type(month)}")



        if(month>=1 and month<=3):
            month=12
            year=year-1

        elif(month>3 and month<6):
            month=3

        elif(month>=6 and month<9):
            month=6

        elif(month>=9 and month<=12):
            month=9
        

        
        _, last_day = calendar.monthrange(year, month)
        
        last_day_date_str = f"{year}-{month:02d}-{last_day:02d}"

        print(">>>> The entered date ",date_str)
        print(">>>> The adjusted date ",last_day_date_str)

        
        return last_day_date_str
    except ValueError:
        return "Invalid date format. Please use dd-mm-yyyy."




def get_previous_year_date(input_date_str):
    try:
        year, month, day = input_date_str.split("-")
        
        previous_year = int(year) - 1
        
        previous_year_date_str = f"{previous_year}-{month}-{day}"
        
        return previous_year_date_str
    except ValueError:
        return "Invalid date format. Please use YYYY-MM-DD."






@Get_Financial_Info.get('/AllBS')
async def find_all_sectors():
    return serializeList2(BalanceSheetAnnualCollection.find())



import time
from datetime import datetime

def get_Financials_Data(symbol,start_date, end_date,maxElem,StatementType,Frequency):
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
            


# LTM example, the company should have an income statement at the given date otherwise it will return an empty array 
#http://localhost:1001/financials?symbol=AY&start_date_str=2015-04-05&limit=4&StatementType=PL&Frequency=LTM
        if StatementType=="PL" and Frequency=="LTM":
            Collection=IncomeStatementQuarterCollection

            # print("Before ")
            # print("start_date ",start_date)
            # print("Before ")

            adjusted_LTM_start_date=LTM_date_adjuster(start_date)
            start_date=adjusted_LTM_start_date

            last_Year_LTM_start_date=get_previous_year_date(start_date)
            end_date=last_Year_LTM_start_date
            Collection=IncomeStatementQuarterCollection

            aux=start_date
            start_date=end_date
            end_date=aux


            # print("Starting date ",start_date)
            # print("Ending date ",end_date)




        if StatementType=="CF" and Frequency=="A":
            Collection=CashFlowAnnualCollection

        if StatementType=="CF" and Frequency=="Q":
            Collection=CashFlowQuarterCollection


        if StatementType=="CF" and Frequency=="LTM":

            # print("Before ")
            # print("start_date ",start_date)
            # print("Before ")

            adjusted_LTM_start_date=LTM_date_adjuster(start_date)
            start_date=adjusted_LTM_start_date

            last_Year_LTM_start_date=get_previous_year_date(start_date)
            end_date=last_Year_LTM_start_date
            Collection=CashFlowQuarterCollection

            aux=start_date
            start_date=end_date
            end_date=aux


            # print("Starting date ",start_date)
            # print("Ending date ",end_date)







        print("")
        print("")
        print(">>> Starting the treatement of collection.find")
        print(f" start_date {start_date} / the end_date {end_date} / the collection {Collection}")
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

        reversed_data = result[::-1]
        result = serializeList2(reversed_data)

        end_time = time.time()
        print(f"Elapsed Time: {end_time - start_time:.2f} seconds")

        return result
    except Exception as e:
        raise e




from datetime import datetime


#A curl example to this api 
#http://localhost:1001/financials?symbol=GOOS.TO&start_date_str=2018-01-05&end_date_str=2022-01-05&limit=10&StatementType=PL&Frequency=Q
@Get_Financial_Info.get('/financials')
def get_financials_API(
    symbol: str = Query(None, title="symbol"),
    start_date_str: str = Query(None, title="start_date_str"),
    end_date_str: str = Query(None, title="end_date_str"),
    limit: str = Query(None, title="limit"),
    StatementType: str = Query(None, title="StatementType"),
    Frequency: str = Query(None, title="Frequency"),


):
    try:

        return get_Financials_Data(symbol,start_date_str, end_date_str,limit,StatementType,Frequency)
    except Exception as e:
        raise e
    

@Get_Financial_Info.get("/date_LTM_Adjuster_tester/{entered_Date}")
def date_LTM_Adjuster_tester_API(entered_Date: str):
    adjustedDate = LTM_date_adjuster(entered_Date)
    last_Year=get_previous_year_date(adjustedDate)
    return {"adjustedDate":adjustedDate,"last_Year":last_Year}