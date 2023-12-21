

from fastapi import HTTPException, FastAPI, Response, status, APIRouter
from fastapi.responses import JSONResponse
from datetime import date

import pandas as pd

from schemas.Sector import serializeList2

from schemas.Sector import serializeDict2


from pymongo import MongoClient, UpdateOne

from APIs.Endpoints2.Quotes import QuotesCollection

from config.db import get_database
from APIs.Endpoints1.companies_APIs import get_company_symbols,CompaniesCollection

from APIs.Endpoints3.FOREX import FOREX_IndexesCollection
from APIs.Endpoints4.stock_indexes_Quotes import STOCKIndexes_QuotesCollection



from APIs.Endpoints5.googleSheetAPI import read_data_from_sheets
from APIs.Endpoints5.googleSheetAPI import update_googleSheet_data_in



from APIs.Endpoints1.sectors_APIs import sectorsCollection



from typing import List


import os
import asyncio
import aiohttp
import time
import datetime

Quotes_update = APIRouter()
api_key = os.getenv("API_KEY")

import httpx
import pymongo
import pymongo.errors as errors
import numpy as np



def get_QuotesStatistics_collection():
    db = get_database()
    QuotesStatistics=db["QuotesStatistics"]
    QuotesStatistics.create_index([("_id", 1)])
    return QuotesStatistics

QuotesStatisticsCollection=get_QuotesStatistics_collection()


def get_QuotesTest_collection():
    db = get_database()
    QuotesTest=db["QuotesTest"]
    QuotesTest.create_index([("_id", 1)])
    return QuotesTest

QuotesTestCollection=get_QuotesTest_collection()






def get_date_for_symbol(dataframe, symbol):
    # print("working with this dataframe")
    # print(dataframe)
    # print("Treating the symbol  ", symbol)
    filtered_df = dataframe[dataframe['symbol'] == symbol]

    if not filtered_df.empty:
        return filtered_df['date'].iloc[0]
    else:
        return None




def update_csv_with_symbol_and_date(csv_url, symbol, date):
    try:
        df = pd.read_csv(csv_url)
    except Exception as e:
        print(f"Error loading the .csv file: {e}")
        return

    if symbol in df['symbol'].values:
        df.loc[df['symbol'] == symbol, 'date'] = date
    else:
        new_entry = {'symbol': symbol, 'date': date}
        new_df = pd.DataFrame([new_entry]) 
        df = pd.concat([df, new_df], ignore_index=True)  

    try:
        df.to_csv(csv_url, index=False)
        print("CSV file updated successfully.")
    except Exception as e:
        print(f"Error saving the .csv file: {e}")





def find_nearest_date(date, dates):
    return min(dates, key=lambda d: abs(datetime.datetime.strptime(d, "%Y-%m-%d") - datetime.datetime.strptime(date, "%Y-%m-%d")))




def compare_dates(csv_date_str, date_to_compare_str):
    csv_date_format = "%Y-%m-%d"
    date_format = csv_date_format

    csv_date = csv_date_str
    date_to_compare = date_to_compare_str

    if csv_date is None or date_to_compare is None:
        print("One or both dates are missing.")
        return 1  

    if csv_date < date_to_compare:
        print(csv_date, " is in the past.")
        return -1
    elif csv_date == date_to_compare:
        print(csv_date, " is today.")
        return 1
    else:
        print(csv_date, " is in the future.")
        return 1







def get_stock_data(historiqueStockSymbol,symbol, target_date):
    start_date = (pd.to_datetime(target_date) - pd.DateOffset(years=10)).strftime('%Y-%m-%d')

    results = [entry for entry in historiqueStockSymbol if start_date <= entry['date'] <= target_date]
    if results:
        data_list = list(results)
        # print("Retrieved data from MongoDB:")
        # print(data_list)

        df = pd.DataFrame(data_list)

        columns_to_extract = ["date", "adjClose"]
        # print("Columns in DataFrame:", df.columns)

        df = df[columns_to_extract]
        # print("DataFrame after column extraction:")
        # print(df)

        return df
    else:
        print(f"No data found for {symbol} in the date range from {start_date} to {target_date}")
        return None



DEFAULT_PERIODS = [0.5] + np.arange(1, 10, 1).tolist()


#ajouter symbole stockdata, qui va prendre toute l'historique du stock
def construct_statistics(historiqueStockSymbol,Symbol, current_date, analysis_periods=DEFAULT_PERIODS):
    
    df = get_stock_data(historiqueStockSymbol,Symbol, current_date)

    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date", inplace=False)
    df = df.sort_values(by="date")
    last_date = df.index[-1]

    statitcs = {}


    metrics = ["maxPrice", "minPrice", "averagePrice", "emAveragePrice" ,"return", "maxDrowDown", "drawUp", "daysNoChangePercentage",
               "daysUpPercentage","daysDownPercentage","dailyVol", "weeklyVol",
               "monthlyVol", "dailyEmaVol", "weeklyEmaVol", "monthlyEmaVol"]

    for metric in metrics:
        statitcs[metric] = {}

    periods_name = [f"{period}y" for period in analysis_periods]
    periods_name.append("all")

    analysis_dates = []

    for period in analysis_periods:
        end_date = last_date - pd.DateOffset(months=period * 12)
        nearest_date = df.index[df.index <= end_date].max()
        analysis_dates.append(nearest_date)

    analysis_dates.append(df.index[0])

    for i, nearest_date in enumerate(analysis_dates):
        period_name = periods_name[i]

        if pd.notna(nearest_date):
            selected_prices = df.loc[nearest_date:last_date, 'adjClose']
            percentage_change = selected_prices.pct_change()
            cum_returns = (1 + percentage_change).cumprod()
            positive_returns = percentage_change.apply(lambda x: max(0, x))
            selected_prices_weekly = selected_prices.resample("W").last()
            selected_prices_monthly = selected_prices.resample("M").last()
            daily_log_returns = np.log(selected_prices / selected_prices.shift(1))
            weekly_log_returns = np.log(
                selected_prices_weekly / selected_prices_weekly.shift(1))
            monthly_log_returns = np.log(
                selected_prices_monthly / selected_prices_monthly.shift(1))

            daily_ema_volatility = daily_log_returns.ewm(span=10, min_periods=0, adjust=False).std().iloc[-1]
            weekly_ema_volatility = weekly_log_returns.ewm(span=10, min_periods=0, adjust=False).std().iloc[-1]
            monthly_ema_volatility = monthly_log_returns.ewm(span=10, min_periods=0, adjust=False).std().iloc[-1]
            statitcs["averagePrice"][period_name] = selected_prices.mean()
            statitcs["emAveragePrice"][period_name]=selected_prices.ewm(span=10, min_periods=0, adjust=False).mean().iloc[-1]
            statitcs["maxPrice"][period_name] = selected_prices.max()
            statitcs["minPrice"][period_name] = selected_prices.min()
            statitcs["return"][period_name] = selected_prices[-1] / selected_prices[0] - 1
            statitcs["maxDrowDown"][period_name] = (cum_returns / cum_returns.cummax() - 1).min()
            statitcs["drawUp"][period_name] = (1 + positive_returns).cumprod().iloc[-1] - 1
            statitcs["daysNoChangePercentage"][period_name] = (percentage_change == 0).sum() / len(percentage_change)
            statitcs["daysUpPercentage"][period_name] = (percentage_change > 0).sum() / len(percentage_change)
            statitcs["daysDownPercentage"][period_name] = (percentage_change < 0).sum() / len(percentage_change)
            statitcs["dailyVol"][period_name] = np.std(daily_log_returns) * np.sqrt(252)
            statitcs["weeklyVol"][period_name] = np.std(weekly_log_returns) * np.sqrt(52)
            statitcs["monthlyVol"][period_name] = np.std(monthly_log_returns) * np.sqrt(12)
            statitcs["dailyEmaVol"][period_name] = daily_ema_volatility * np.sqrt(252)
            statitcs["weeklyEmaVol"][period_name] = weekly_ema_volatility *  np.sqrt(52)
            statitcs["monthlyEmaVol"][period_name] = monthly_ema_volatility * np.sqrt(12)

        else:

            for metric in metrics:
                statitcs[metric][period_name] = np.nan

    return statitcs








@Quotes_update.get("/v1/testerFonctionStatistiques")
async def testerFonctionsStatistiques():
    # statitcs = construct_statistics("ENGIY", current_date="2023-11-10")
    statitcs = construct_statistics("AGGRU", current_date="2023-11-10")

    print(statitcs)
    return serializeDict2(statitcs)



def search_company_by_symbol(symbol):
    result = CompaniesCollection.find_one({"Symbol": symbol})
    if result:
        return {"symbol": result.get("Symbol"),
                
                "sector": result.get("sector"), 
                "sectorId": result.get("sectorId"), 

                "country": result.get("country"), 
                "countryId": result.get("countryId"), 

                "industry": result.get("industry"),     
                "industryId": result.get("industryId")     
                 }
    else:
        return None


















# #####  The magic solution that updates quotes with marketCap values and EV with a way more performant logic that uses batches
from typing import List




import json
import numpy as np







##The same api as /v1/update_marketCap_EValues_API but with batch and parrallel logic, it has some issues that are not logged in the console so i can't debug them 
@Quotes_update.get('/vY/update_quotes_statistics')
async def update_marketCap_EValues_function():
    batch_size = 4
    # allCompaniesSymobls=[ "AJINF", "AGGRU",  "RGBD", "TVTY", "RAC.AX", "4248.T", "REXR", "600936.SS", "CAMLINFINE.NS", "FINGF", "CPFXF", "AGTT", "CNNA", "LMNR", "JPFA.JK", "300368.SZ", "CPD.WA", "090350.KS", "002223.SZ", "ARYN.SW", "FROTO.IS", "GPIL.NS", "SOFT", "LSTR", "MTX", "FBVA", "TVPC", "USCTU", "LIVK", "GQMLF", "QELL", "AMIN.JK", "BRAC", "GBGPF", "ICGUF", "GRVI", "OTLKW", "PIPP", "EXRO.TO", "UMGNF", "PRU.DE", "FDUSZ", "CNBN", "STEELCAS.NS", "ICDSLTD.NS", "RATCH-R.BK", "SHMAY", "BRLIU", "CAMS.NS", "MNGG", "RFLFF", "RVVTF", "EXPI", "CKISF", "WRTBF", "1370.HK", "PHN.MI", "300546.SZ", "PGPEF", "LOV.AX", "STBI", "NTST", "LLKKF", "DMPZF", "605296.SS", "0HDK.L", "FDY.TO", "OBSN.SW", "ELK.OL", "MLLOI.PA", "MGYOY", "BNP.WA", "GZPHF", "300252.SZ", "SWTF.F", "ALSO3.SA", "2764.T", "TAINWALCHM.NS", "JSDA", "MUNJALSHOW.NS", "000856.SZ", "ASHTF", "MSON-A.ST", "WIB.CN", "9428.T", "0856.HK", "BBB.L", "601865.SS", "TSPG", "5658.T", "1982.T", "600748.SS", "IMPAL.NS", "4044.T", "GMAB.CO", "2379.TW", "TTE.PA", "6901.T", "WINE.L", "BXMT", "KARE.AT", "RGEN", "CAKE", "600612.SS", "6748.T", "MGA", "WFC", "0IV3.L", "DND.TO", "CIBUS.ST", "CYBERMEDIA.NS", "002273.SZ", "LEN-B", "DEC.PA", "NAVNETEDUL.NS", "4118.T", "EXC", "ELLKF", "3699.HK", "CTPTY", "LEVL", "LMN.SW", "THYROCARE.NS", "3056.TW", "ALQ.AX", "ELUXY", "301007.SZ", "MCPH", "REPH", "603918.SS", "002901.SZ", "ELMN.SW", "GWRE", "1447.TW", "023530.KS", "NSTS", "VSYD.ME", "603085.SS", "LAC", "GCEI", "F3C.DE", "002341.SZ", "FBTT", "IVAC", "HELN.SW", "STRNW", "SQSP", "CI.BK", "603212.SS", "0HFB.L", "601928.SS", "APO", "8289.T", "8096.T", "FLWS", "MXC.L", "PGOL.CN", "SKKRF", "PORBF", "SEMR", "603027.SS", "YPB.AX", "SREV", "PNV.AX", "CHWAW", "MBHCF", "GL.CN", "0QZ4.L", "0KYZ.L", "HO7.DE", "PREM.L", "MNIN.TA", "JIM.L", "SBGSF", "WNNR-UN", "CBY.AX", "BRSYF", "ASB-PE", "KIDS", "NCPL", "AKO-B", "3101.T", "9932.T", "1515.T", "FME.L", "GPOR", "KROS", "SCHAND.NS", "603703.SS", "03473K.KS", "MMTS", "0992.HK", "000021.SZ", "MFT.MI", "AKSHAR.NS", "ISOLF", "300689.SZ", "SKUE.OL", "SFT.F", "EMA.TO", "000413.SZ", "8387.T", "600099.SS", "TOOL","OG.V", "300790.SZ", "SHMUF", "AXE.V", "BUD.V", "ECPN", "TELIA1.HE", "PIER.L", "MSLH.L", "6032.T", "FKWL", "HAR.DE", "HITECHCORP.NS", "2590.T", "9322.T", "ONEXF", "0688.HK", "KBH", "CRWD", "FTOCU", "BYRG", "BRGE12.SA", "0631.HK", "1813.HK", "APS.TO", "5406.T", "000903.SZ", "ZIN.L", "ENBI.CN", "CRSQ", "300261.SZ", "MGG.V", "002928.SZ", "HUM.AX", "FPIP.ST", "UNIP3.SA", "000048.SZ", "2376.HK", "AMAOU", "5GG.AX", "WEGOF", "AWTRF", "ROSE.SW", "CDSG", "TRII", "002555.SZ", "000055.SZ", "SASQ.CN", "NICU.V", "NZS.AX", "BCOMF", "000953.SZ", "AYAL.TA", "002692.SZ", "CLH.JO", "THEP.PA", "TPC", "LTMAQ", "ENUA.F", "0R2Y.L", "BGOPF", "KEN.TA", "TANGI.ST", "TEAM.CN", "0118.HK", "EDHN.SW", "RAUTE.HE", "GAPAW", "CBLNF", "PCOR", "49GP.L", "IVC.AX", "JMFINANCIL.NS", "ICLD", "SKA.WA", "7762.T", "GIL.TO", "SKHSF", "SRI.V", "ALWEC.PA", "BFINVEST.NS", "GZF.DE", "ECHO", "600271.SS", "ETG.TO", "IOSP", "CDXFF", "ABSOF", "SYHLF"]

    allCompaniesSymobls=[ "AJINF", "AGGRU"]

    print("----- The length of all the symbols ", len(allCompaniesSymobls))

    # Process symbols in batches
    for i in range(0, len(allCompaniesSymobls), batch_size):
        batch_symbols = allCompaniesSymobls[i:i + batch_size]
        tasks = [update_quotes_statisticsFunction(entry) for entry in batch_symbols]
        await asyncio.gather(*tasks)

    return 'update_marketCap_EValues_API'



@Quotes_update.get('/v1/update_quotes_statistics')
async def update_quotes_statistics_API():

    # allCompaniesSymobls = get_company_symbols()
    # allCompaniesSymbolsList = list(allCompaniesSymobls)
    # print("-- All the companies symbols list ")
    # #print(allCompaniesSymobls)

    # allCompaniesSymobls=["MLIFC.PA", "AJINF", "AGGRU", "MTGRF", "RGBD", "TVTY", "RAC.AX", "4248.T", "REXR", "600936.SS", "CAMLINFINE.NS", "FINGF", "CPFXF", "AGTT", "CNNA", "LMNR", "JPFA.JK", "300368.SZ", "CPD.WA", "090350.KS", "002223.SZ", "ARYN.SW", "FROTO.IS", "GPIL.NS", "SOFT", "LSTR", "MTX", "FBVA", "TVPC", "USCTU", "LIVK", "GQMLF", "QELL", "AMIN.JK", "BRAC", "GBGPF", "ICGUF", "GRVI", "OTLKW", "PIPP", "EXRO.TO", "UMGNF", "PRU.DE", "FDUSZ", "CNBN", "STEELCAS.NS", "ICDSLTD.NS", "RATCH-R.BK", "SHMAY", "BRLIU", "CAMS.NS", "MNGG", "RFLFF", "RVVTF", "EXPI", "CKISF", "WRTBF", "1370.HK", "PHN.MI", "300546.SZ", "PGPEF", "LOV.AX", "STBI", "NTST", "LLKKF", "DMPZF", "605296.SS", "0HDK.L", "FDY.TO", "OBSN.SW", "ELK.OL", "MLLOI.PA", "MGYOY", "BNP.WA", "GZPHF", "300252.SZ", "SWTF.F", "ALSO3.SA", "2764.T", "TAINWALCHM.NS", "JSDA", "MUNJALSHOW.NS", "000856.SZ", "ASHTF", "MSON-A.ST", "WIB.CN", "9428.T", "0856.HK", "BBB.L", "601865.SS", "TSPG", "5658.T", "1982.T", "600748.SS", "IMPAL.NS", "4044.T", "GMAB.CO", "2379.TW", "TTE.PA", "6901.T", "WINE.L", "BXMT", "KARE.AT", "RGEN", "CAKE", "600612.SS", "6748.T", "MGA", "WFC", "0IV3.L", "DND.TO", "CIBUS.ST", "CYBERMEDIA.NS", "002273.SZ", "LEN-B", "DEC.PA", "NAVNETEDUL.NS", "4118.T", "EXC", "ELLKF", "3699.HK", "CTPTY", "LEVL", "LMN.SW", "THYROCARE.NS", "3056.TW", "ALQ.AX", "ELUXY", "301007.SZ", "MCPH", "REPH", "603918.SS", "002901.SZ", "ELMN.SW", "GWRE", "1447.TW", "023530.KS", "NSTS", "VSYD.ME", "603085.SS", "LAC", "GCEI", "F3C.DE", "002341.SZ", "FBTT", "IVAC", "HELN.SW", "STRNW", "SQSP", "CI.BK", "603212.SS", "0HFB.L", "601928.SS", "APO", "8289.T", "8096.T", "FLWS", "MXC.L", "PGOL.CN", "SKKRF", "PORBF", "SEMR", "603027.SS", "YPB.AX", "SREV", "PNV.AX", "CHWAW", "MBHCF", "GL.CN", "0QZ4.L", "0KYZ.L", "HO7.DE", "PREM.L", "MNIN.TA", "JIM.L", "SBGSF", "WNNR-UN", "CBY.AX", "BRSYF", "ASB-PE", "KIDS", "NCPL", "AKO-B", "3101.T", "9932.T", "1515.T", "FME.L", "GPOR", "KROS", "SCHAND.NS", "603703.SS", "03473K.KS", "MMTS", "0992.HK", "000021.SZ", "MFT.MI", "AKSHAR.NS", "ISOLF", "300689.SZ", "SKUE.OL", "SFT.F", "EMA.TO", "000413.SZ", "8387.T", "600099.SS", "TOOL","OG.V", "300790.SZ", "SHMUF", "AXE.V", "BUD.V", "ECPN", "TELIA1.HE", "PIER.L", "MSLH.L", "6032.T", "FKWL", "HAR.DE", "HITECHCORP.NS", "2590.T", "9322.T", "ONEXF", "0688.HK", "KBH", "CRWD", "FTOCU", "BYRG", "BRGE12.SA", "0631.HK", "1813.HK", "APS.TO", "5406.T", "000903.SZ", "ZIN.L", "ENBI.CN", "CRSQ", "300261.SZ", "MGG.V", "002928.SZ", "HUM.AX", "FPIP.ST", "UNIP3.SA", "000048.SZ", "2376.HK", "AMAOU", "5GG.AX", "WEGOF", "AWTRF", "ROSE.SW", "CDSG", "TRII", "002555.SZ", "000055.SZ", "SASQ.CN", "NICU.V", "NZS.AX", "BCOMF", "000953.SZ", "AYAL.TA", "002692.SZ", "CLH.JO", "THEP.PA", "TPC", "LTMAQ", "ENUA.F", "0R2Y.L", "BGOPF", "KEN.TA", "TANGI.ST", "TEAM.CN", "0118.HK", "EDHN.SW", "RAUTE.HE", "GAPAW", "CBLNF", "PCOR", "49GP.L", "IVC.AX", "JMFINANCIL.NS", "ICLD", "SKA.WA", "7762.T", "GIL.TO", "SKHSF", "SRI.V", "ALWEC.PA", "BFINVEST.NS", "GZF.DE", "ECHO", "600271.SS", "ETG.TO", "IOSP", "CDXFF", "ABSOF", "SYHLF"]
    

    QuotesStatistics_csv_file_path_stockIndexesCSV_Id = '16aaqkVf7K1N9B9mQwwfvvp2gT5uLNegd4w7EujLu1YM'
    SymbolDateQuotesDF = read_data_from_sheets(QuotesStatistics_csv_file_path_stockIndexesCSV_Id,"Sheet1")

    print("DF of the QuotesStatistics_csv_file_path_stockIndexesCSV_Id")
    print(SymbolDateQuotesDF)

    symbols_list = SymbolDateQuotesDF['symbol'].tolist()

    allCompaniesSymobls=symbols_list



    print("----- The length of all the symbols ",len(allCompaniesSymobls))

    #To work with only two symbols for testing purposes

    print("Number of all the symbols ")
    print(len(allCompaniesSymobls))


    for entry in allCompaniesSymobls:
       await update_quotes_statisticsFunction(entry)

    return "Quotes statistics update finished"



import time 




import calendar
import datetime  




def is_last_day_of_month(date_str):
    date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    _, last_day = calendar.monthrange(date_obj.year, date_obj.month)
    return date_obj.day == last_day





async def update_quotes_statisticsFunction(companySymbol):
    print("----  update_quotes_statisticsFunction ",companySymbol)


    QuotesStatistics_csv_file_path = '1lI_ihXxz0ofnEYNCQr8lOs1LvFx6RboSX2wrEjuPX98'
    SymbolDateQuotesStatisticsDF = read_data_from_sheets(QuotesStatistics_csv_file_path,"Sheet1")

    current_date = datetime.datetime.now()
    formatted_todayDate = current_date.strftime("%Y-%m-%d")

    get_date_for_symbol(SymbolDateQuotesStatisticsDF,companySymbol)



    if get_date_for_symbol(SymbolDateQuotesStatisticsDF,companySymbol)==None or get_date_for_symbol(SymbolDateQuotesStatisticsDF,companySymbol)<formatted_todayDate:

        total_updates = 0  
        successful_updates = 0
        error_updates = 0
        no_change_updates = 0

        query = {"symbol": companySymbol}
        historiqueStock = STOCKIndexes_QuotesCollection.find(query)
        historiqueStock = list(historiqueStock)

        # Check the type of historiqueStock
        print(f"Type of historiqueStock: {type(historiqueStock)}")

        # Check the number of elements in historiqueStock
        print(f"Number of elements in historiqueStock: {len(historiqueStock)}")


        filtered_list = [element for element in historiqueStock if is_last_day_of_month(element['date'])]

        print(f"Number of elements in filtered_list: {len(filtered_list)}")
        # print(filtered_list)

        historiqueStock=filtered_list


        # print("---------- Historique stock ")
        # print(historiqueStock)
        # print(" Type Historique stock ",type(historiqueStock))
        # print("---------- Fin Historique stock ")

        cursor = STOCKIndexes_QuotesCollection.find({"symbol":companySymbol}, {"symbol": 1, "date": 1, "_id": 0})


        

        QuotesStatistics_csv_file_path = '1lI_ihXxz0ofnEYNCQr8lOs1LvFx6RboSX2wrEjuPX98'
        SymbolDateQuotesStatisticsDF = read_data_from_sheets(QuotesStatistics_csv_file_path,"Sheet1")


        # Convert the cursor to a list of dictionaries
        symbols_and_dates_list = list(cursor)

        # Display the result

        symbols_and_dates_list.reverse()



        # print("------- symbols_and_dates_list before the filter with the latest date from the csv")
        # print(symbols_and_dates_list)


                    # Filter elements with dates older than "the latest csv date"
        if get_date_for_symbol(SymbolDateQuotesStatisticsDF, companySymbol) !=None:
                    symbols_and_dates_list = [quote for quote in symbols_and_dates_list
                                        if quote.get("date") >= get_date_for_symbol(SymbolDateQuotesStatisticsDF,companySymbol)]
                    


        print("------- length symbols_and_dates_list after the filter with the latest date from the csv")
        print(len(symbols_and_dates_list))

        filtered_list_symbols_and_dates_list  = [element for element in symbols_and_dates_list if is_last_day_of_month(element['date'])]

        symbols_and_dates_list=filtered_list_symbols_and_dates_list
        
        # #print(">>>>>> The length of symbols_and_dates_list that we are going to create statistics for them ", len(symbols_and_dates_list))




        company_info = search_company_by_symbol(companySymbol)


        nombre_par_insert_many=1000
        compteur=0
        bulk_documents=[]
        for entry in symbols_and_dates_list:

            compteur=compteur+1
            
            symbol = entry['symbol']
            date = entry['date']
            #print("----  creating statistics for the symbol ",symbol," and date : ",date)
            # extraire la dernière date de mise à jour du csv

            if (compare_dates(get_date_for_symbol(SymbolDateQuotesStatisticsDF, symbol), date ) == 1):
                #Ca veut dire que la date du quote dans la base de donnée est après la date de la dernière mise à jour, ça veut dire qu'on doit performer le traitement de quotes statistics update
                
                start_time = time.time()  # Record the start time

                statistics = construct_statistics(historiqueStock,symbol, date)
                JsonValues = serializeDict2(statistics)

                JsonValues = {key: int(value) if isinstance(value, np.int64) else value for key, value in JsonValues.items()}

                # Convert to JSON to handle numpy types
                JsonValues = json.loads(json.dumps(JsonValues, default=str))


                elapsed_time = time.time() - start_time
                print(f"----- Retour des données statistiques {elapsed_time:.2f} seconds")
        
                # print("------------ JSON values")
                # print(JsonValues)


                # print("------------ company_info")
                # print(company_info)



                # filter_query = {"symbol": symbol, "date": date}
                document = {
                        "max_price": JsonValues['maxPrice'],
                        "minPrice": JsonValues['minPrice'],
                        "emAveragePrice": JsonValues['emAveragePrice'],
                        "averagePrice": JsonValues['averagePrice'],
                        "return": JsonValues['return'],
                        "maxDrowDown": JsonValues['maxDrowDown'],

                        "daysNoChangePercentage": JsonValues['daysNoChangePercentage'],
                        "daysUpPercentage": JsonValues['daysUpPercentage'],

                        "daysDownPercentage": JsonValues['daysDownPercentage'],
                        "dailyVol": JsonValues['dailyVol'],
                        "weeklyVol": JsonValues['weeklyVol'],

                        "monthlyVol": JsonValues['monthlyVol'],
                        "dailyEmaVol": JsonValues['dailyEmaVol'],
                        "weeklyEmaVol": JsonValues['weeklyEmaVol'],
                        "monthlyEmaVol": JsonValues['monthlyEmaVol'],

                        "date":date,

                        **company_info
                    
                }

                bulk_documents.append(document)

                if compteur>=nombre_par_insert_many:
                    try:
                        result = QuotesStatisticsCollection.insert_many(bulk_documents, ordered=False)


                        compteur=0
                        bulk_documents=[]


                    except errors.PyMongoError as e:
                        error_updates += 1
                        status = "Error"
                        message = f"EV Error updating document: {e}"
                    # Insert the remaining documents if any
        if compteur > 0:
            try:
                result = QuotesStatisticsCollection.insert_many(bulk_documents, ordered=False)
            except errors.PyMongoError as e:
                error_updates += 1
                status = "Error"
                message = f"EV Error updating document: {e}"
            
            update_googleSheet_data_in(QuotesStatistics_csv_file_path, symbol, formatted_todayDate)


    return "Statistics update finished"





from datetime import datetime as dt




def return_EV_element_with_closest_date(data_list, target_date):
    target_datetime = dt.strptime(target_date, "%Y-%m-%d")
    
    # Sort the list by date to simplify the search
    sorted_data = sorted(data_list, key=lambda x: dt.strptime(x["date"], "%Y-%m-%d"))
    
    closest_element = None
    closest_date = None
    closest_datetime = None
    min_difference = float('inf')

    for item in sorted_data:
        item_datetime = dt.strptime(item["date"], "%Y-%m-%d")
        difference = abs((item_datetime - target_datetime).days)

        if difference < min_difference:
            min_difference = difference
            closest_element = item
            closest_date = item["date"]
            closest_datetime = item_datetime

    return closest_element



def find_closest_date(data_list, target_date):
    target_datetime = dt.strptime(target_date, "%Y-%m-%d")
    
    # Sort the list by date to simplify the search
    sorted_data = sorted(data_list, key=lambda x: dt.strptime(x["date"], "%Y-%m-%d"))
    
    closest_date = None
    closest_datetime = None
    min_difference = float('inf')

    for item in sorted_data:
        item_datetime = dt.strptime(item["date"], "%Y-%m-%d")
        difference = abs((item_datetime - target_datetime).days)

        if difference < min_difference:
            min_difference = difference
            closest_date = item["date"]
            closest_datetime = item_datetime

    if closest_date == target_date:
        return closest_date, "Date exacte"
    else:
        return closest_date, "Date proche"



async def update_marketCap_EValues(companySymbol):
    print("----  update_marketCap_EValues ",companySymbol)

    total_updates = 0  
    successful_updates = 0
    error_updates = 0
    no_change_updates = 0
    marketCapUrl = f"https://financialmodelingprep.com/api/v3/historical-market-capitalization/{companySymbol}?apikey={api_key}"
    EV_resultsUrl = f"https://financialmodelingprep.com/api/v3/enterprise-values/{companySymbol}/?period=quarter&apikey=96051dba5181978c2f0ce23c1ef4014b"


    # Getting today's date
    current_date = datetime.datetime.now()
    formatted_todayDate = current_date.strftime("%Y-%m-%d")
    InsertionResult = QuotesCollection.insert_one({"UpdatingDay":formatted_todayDate})


    # QuotesStatistics_csv_file_path = 'HistoriqueCSV/Quotes_CSV_file/Quotes_marketCap_EV_update.csv'
    # SymbolDateQuotesDF = pd.read_csv(QuotesStatistics_csv_file_path)

    QuotesStatistics_csv_file_path = '1yi6oJ1MF5MJrs77RG_N2wuRdMPbnkT-UdkMFvcglwUk'
    SymbolDateQuotesDF = read_data_from_sheets(QuotesStatistics_csv_file_path)



    async with httpx.AsyncClient() as client:
        response_marketCap = await client.get(marketCapUrl)
        response_marketCap.raise_for_status()
        marketCap_result = response_marketCap.json()

        marketCap_result.reverse()

        if len(marketCap_result)>0:
            ########making the update by adding the marketcap

            #print("------The marketCap results before the filter ")
            #print(marketCap_result)
            print("------ ")
            print("------ ")
            print("------ ")

            # Filter elements with dates older than "the latest csv date"
            if get_date_for_symbol(SymbolDateQuotesDF, companySymbol) !=None:
                marketCap_result = [marketCap_element for marketCap_element in marketCap_result
                                    if marketCap_element.get("date") >= get_date_for_symbol(SymbolDateQuotesDF,companySymbol)]
                
            #print("------The marketCap results after the filter ")
            #print(marketCap_result)




            for marketCap_element in marketCap_result:
                    date = marketCap_element["date"]
                    #print("-- la date dans le CSV du symbol ",companySymbol," est :",get_date_for_symbol(SymbolDateQuotesDF,marketCap_element["symbol"]))
                    # print("-- la date du quote ",date)

                    if compare_dates(get_date_for_symbol(SymbolDateQuotesDF,marketCap_element["symbol"]),date)==1:
                        symbol = marketCap_element["symbol"]
                        marketCapitalization = marketCap_element["marketCap"]

                        filter_query = {"symbol": symbol, "date": date}
                        update_query = {"$set": {"marketCap": marketCapitalization}}

                        try:

                            result = QuotesCollection.update_one(filter_query, update_query,upsert=True)
                            total_updates += 1

                            if result.modified_count > 0:
                                successful_updates += 1
                                status = "Success"
                                message = f"MarketCap Update successful for {symbol} on {date}"
                            else:
                                no_change_updates += 1
                                status = "NoChange"
                                message = f"MarketCap No changes for {symbol} on {date}"
                        except errors.PyMongoError as e:
                            error_updates += 1
                            status = "Error"
                            message = f"MarketCap Error updating document: {e}"

                        ## Print progress
                        print(f"MarketCap updating : symbol {symbol} date{date}  Processed {total_updates} updates - {successful_updates} successful, {no_change_updates} no change, {error_updates} errors")


    all__Quotes_of_the_symbol=serializeList2(QuotesCollection.find({"symbol":companySymbol}))

    print("All quotes ",companySymbol)
    print(all__Quotes_of_the_symbol)



    async with httpx.AsyncClient() as client:
        response_EV = await client.get(EV_resultsUrl)
        response_EV.raise_for_status()
        EV_result = response_EV.json()

        EV_result.reverse()

        if len(EV_result)>2:
            ########making the update by adding the EVs
           
           
            # Filter elements with dates older than "the latest csv date"
            if get_date_for_symbol(SymbolDateQuotesDF, companySymbol) !=None:
                all__Quotes_of_the_symbol = [quote for quote in all__Quotes_of_the_symbol
                                if quote.get("date") >= get_date_for_symbol(SymbolDateQuotesDF, companySymbol)]


            for quote in all__Quotes_of_the_symbol:
                quote_date = quote["date"]
                if compare_dates(get_date_for_symbol(SymbolDateQuotesDF,companySymbol),quote_date)==1:#Procéder au traitement de l'EV

                    result_closest_date = find_closest_date(EV_result, quote_date)

                    # print(f"Date cible : {quote_date}")
                    # print(f"Date la plus proche : {result[0]} ({result[1]})")
                    # print("élément retourné")

                    element_EV=return_EV_element_with_closest_date(EV_result, quote_date)
                    # print(element)
                    numberOfShares = element_EV["numberOfShares"]
                    minusCashAndCashEquivalents = element_EV["minusCashAndCashEquivalents"]
                    addTotalDebt = element_EV["addTotalDebt"]
                    enterpriseValue = element_EV["enterpriseValue"]

                    filter_query = {"symbol": companySymbol, "date": quote_date}
                    update_query = {"$set": {
                            
                            "numberOfShares": numberOfShares,
                            "minusCashAndCashEquivalents": minusCashAndCashEquivalents,
                            "addTotalDebt": addTotalDebt,
                            "enterpriseValue": enterpriseValue
                                                }}
                    try:
                            result = QuotesCollection.update_one(filter_query, update_query,upsert=True)
                            total_updates += 1

                            if result.modified_count > 0:
                                successful_updates += 1
                                status = "Success"
                                message = f"EV Update successful for {companySymbol} on {date}"
                            else:
                                no_change_updates += 1
                                status = "NoChange"
                                message = f"EV No changes for {companySymbol} on {date}"
                    except errors.PyMongoError as e:
                            error_updates += 1
                            status = "Error"
                            message = f"EV Error updating document: {e}"

                        # Print progress
                    update_googleSheet_data_in(QuotesStatistics_csv_file_path, companySymbol, date)
                    print(f"EValues updating : symbol {companySymbol} date{date}  Processed {total_updates} updates - {successful_updates} successful, {no_change_updates} no change, {error_updates} errors")
    return "update_marketCap_EValues_API"



@Quotes_update.get('/v1/update_marketCap_EValues_API')
async def update_marketCap_EValues_function():
    allCompaniesSymobls=[ "AJINF", "AGGRU",  "RGBD", "TVTY", "RAC.AX", "4248.T", "REXR", "600936.SS", "CAMLINFINE.NS", "FINGF", "CPFXF", "AGTT", "CNNA", "LMNR", "JPFA.JK", "300368.SZ", "CPD.WA", "090350.KS", "002223.SZ", "ARYN.SW", "FROTO.IS", "GPIL.NS", "SOFT", "LSTR", "MTX", "FBVA", "TVPC", "USCTU", "LIVK", "GQMLF", "QELL", "AMIN.JK", "BRAC", "GBGPF", "ICGUF", "GRVI", "OTLKW", "PIPP", "EXRO.TO", "UMGNF", "PRU.DE", "FDUSZ", "CNBN", "STEELCAS.NS", "ICDSLTD.NS", "RATCH-R.BK", "SHMAY", "BRLIU", "CAMS.NS", "MNGG", "RFLFF", "RVVTF", "EXPI", "CKISF", "WRTBF", "1370.HK", "PHN.MI", "300546.SZ", "PGPEF", "LOV.AX", "STBI", "NTST", "LLKKF", "DMPZF", "605296.SS", "0HDK.L", "FDY.TO", "OBSN.SW", "ELK.OL", "MLLOI.PA", "MGYOY", "BNP.WA", "GZPHF", "300252.SZ", "SWTF.F", "ALSO3.SA", "2764.T", "TAINWALCHM.NS", "JSDA", "MUNJALSHOW.NS", "000856.SZ", "ASHTF", "MSON-A.ST", "WIB.CN", "9428.T", "0856.HK", "BBB.L", "601865.SS", "TSPG", "5658.T", "1982.T", "600748.SS", "IMPAL.NS", "4044.T", "GMAB.CO", "2379.TW", "TTE.PA", "6901.T", "WINE.L", "BXMT", "KARE.AT", "RGEN", "CAKE", "600612.SS", "6748.T", "MGA", "WFC", "0IV3.L", "DND.TO", "CIBUS.ST", "CYBERMEDIA.NS", "002273.SZ", "LEN-B", "DEC.PA", "NAVNETEDUL.NS", "4118.T", "EXC", "ELLKF", "3699.HK", "CTPTY", "LEVL", "LMN.SW", "THYROCARE.NS", "3056.TW", "ALQ.AX", "ELUXY", "301007.SZ", "MCPH", "REPH", "603918.SS", "002901.SZ", "ELMN.SW", "GWRE", "1447.TW", "023530.KS", "NSTS", "VSYD.ME", "603085.SS", "LAC", "GCEI", "F3C.DE", "002341.SZ", "FBTT", "IVAC", "HELN.SW", "STRNW", "SQSP", "CI.BK", "603212.SS", "0HFB.L", "601928.SS", "APO", "8289.T", "8096.T", "FLWS", "MXC.L", "PGOL.CN", "SKKRF", "PORBF", "SEMR", "603027.SS", "YPB.AX", "SREV", "PNV.AX", "CHWAW", "MBHCF", "GL.CN", "0QZ4.L", "0KYZ.L", "HO7.DE", "PREM.L", "MNIN.TA", "JIM.L", "SBGSF", "WNNR-UN", "CBY.AX", "BRSYF", "ASB-PE", "KIDS", "NCPL", "AKO-B", "3101.T", "9932.T", "1515.T", "FME.L", "GPOR", "KROS", "SCHAND.NS", "603703.SS", "03473K.KS", "MMTS", "0992.HK", "000021.SZ", "MFT.MI", "AKSHAR.NS", "ISOLF", "300689.SZ", "SKUE.OL", "SFT.F", "EMA.TO", "000413.SZ", "8387.T", "600099.SS", "TOOL","OG.V", "300790.SZ", "SHMUF", "AXE.V", "BUD.V", "ECPN", "TELIA1.HE", "PIER.L", "MSLH.L", "6032.T", "FKWL", "HAR.DE", "HITECHCORP.NS", "2590.T", "9322.T", "ONEXF", "0688.HK", "KBH", "CRWD", "FTOCU", "BYRG", "BRGE12.SA", "0631.HK", "1813.HK", "APS.TO", "5406.T", "000903.SZ", "ZIN.L", "ENBI.CN", "CRSQ", "300261.SZ", "MGG.V", "002928.SZ", "HUM.AX", "FPIP.ST", "UNIP3.SA", "000048.SZ", "2376.HK", "AMAOU", "5GG.AX", "WEGOF", "AWTRF", "ROSE.SW", "CDSG", "TRII", "002555.SZ", "000055.SZ", "SASQ.CN", "NICU.V", "NZS.AX", "BCOMF", "000953.SZ", "AYAL.TA", "002692.SZ", "CLH.JO", "THEP.PA", "TPC", "LTMAQ", "ENUA.F", "0R2Y.L", "BGOPF", "KEN.TA", "TANGI.ST", "TEAM.CN", "0118.HK", "EDHN.SW", "RAUTE.HE", "GAPAW", "CBLNF", "PCOR", "49GP.L", "IVC.AX", "JMFINANCIL.NS", "ICLD", "SKA.WA", "7762.T", "GIL.TO", "SKHSF", "SRI.V", "ALWEC.PA", "BFINVEST.NS", "GZF.DE", "ECHO", "600271.SS", "ETG.TO", "IOSP", "CDXFF", "ABSOF", "SYHLF"]
    print("----- The length of all the symbols ",len(allCompaniesSymobls))


    for entry in allCompaniesSymobls:
        await update_marketCap_EValues(entry)

    return 'update_marketCap_EValues_API'

 
  
##The same api as /v1/update_marketCap_EValues_API but with batch and parrallel logic, it has some issues that are not logged in the console so i can't debug them 
@Quotes_update.get('/vY/update_marketCap_EValues_API')
async def update_marketCap_EValues_function():
    batch_size = 2
    allCompaniesSymobls=[ "AJINF", "AGGRU",  "RGBD", "TVTY", "RAC.AX", "4248.T", "REXR", "600936.SS", "CAMLINFINE.NS", "FINGF", "CPFXF", "AGTT", "CNNA", "LMNR", "JPFA.JK", "300368.SZ", "CPD.WA", "090350.KS", "002223.SZ", "ARYN.SW", "FROTO.IS", "GPIL.NS", "SOFT", "LSTR", "MTX", "FBVA", "TVPC", "USCTU", "LIVK", "GQMLF", "QELL", "AMIN.JK", "BRAC", "GBGPF", "ICGUF", "GRVI", "OTLKW", "PIPP", "EXRO.TO", "UMGNF", "PRU.DE", "FDUSZ", "CNBN", "STEELCAS.NS", "ICDSLTD.NS", "RATCH-R.BK", "SHMAY", "BRLIU", "CAMS.NS", "MNGG", "RFLFF", "RVVTF", "EXPI", "CKISF", "WRTBF", "1370.HK", "PHN.MI", "300546.SZ", "PGPEF", "LOV.AX", "STBI", "NTST", "LLKKF", "DMPZF", "605296.SS", "0HDK.L", "FDY.TO", "OBSN.SW", "ELK.OL", "MLLOI.PA", "MGYOY", "BNP.WA", "GZPHF", "300252.SZ", "SWTF.F", "ALSO3.SA", "2764.T", "TAINWALCHM.NS", "JSDA", "MUNJALSHOW.NS", "000856.SZ", "ASHTF", "MSON-A.ST", "WIB.CN", "9428.T", "0856.HK", "BBB.L", "601865.SS", "TSPG", "5658.T", "1982.T", "600748.SS", "IMPAL.NS", "4044.T", "GMAB.CO", "2379.TW", "TTE.PA", "6901.T", "WINE.L", "BXMT", "KARE.AT", "RGEN", "CAKE", "600612.SS", "6748.T", "MGA", "WFC", "0IV3.L", "DND.TO", "CIBUS.ST", "CYBERMEDIA.NS", "002273.SZ", "LEN-B", "DEC.PA", "NAVNETEDUL.NS", "4118.T", "EXC", "ELLKF", "3699.HK", "CTPTY", "LEVL", "LMN.SW", "THYROCARE.NS", "3056.TW", "ALQ.AX", "ELUXY", "301007.SZ", "MCPH", "REPH", "603918.SS", "002901.SZ", "ELMN.SW", "GWRE", "1447.TW", "023530.KS", "NSTS", "VSYD.ME", "603085.SS", "LAC", "GCEI", "F3C.DE", "002341.SZ", "FBTT", "IVAC", "HELN.SW", "STRNW", "SQSP", "CI.BK", "603212.SS", "0HFB.L", "601928.SS", "APO", "8289.T", "8096.T", "FLWS", "MXC.L", "PGOL.CN", "SKKRF", "PORBF", "SEMR", "603027.SS", "YPB.AX", "SREV", "PNV.AX", "CHWAW", "MBHCF", "GL.CN", "0QZ4.L", "0KYZ.L", "HO7.DE", "PREM.L", "MNIN.TA", "JIM.L", "SBGSF", "WNNR-UN", "CBY.AX", "BRSYF", "ASB-PE", "KIDS", "NCPL", "AKO-B", "3101.T", "9932.T", "1515.T", "FME.L", "GPOR", "KROS", "SCHAND.NS", "603703.SS", "03473K.KS", "MMTS", "0992.HK", "000021.SZ", "MFT.MI", "AKSHAR.NS", "ISOLF", "300689.SZ", "SKUE.OL", "SFT.F", "EMA.TO", "000413.SZ", "8387.T", "600099.SS", "TOOL","OG.V", "300790.SZ", "SHMUF", "AXE.V", "BUD.V", "ECPN", "TELIA1.HE", "PIER.L", "MSLH.L", "6032.T", "FKWL", "HAR.DE", "HITECHCORP.NS", "2590.T", "9322.T", "ONEXF", "0688.HK", "KBH", "CRWD", "FTOCU", "BYRG", "BRGE12.SA", "0631.HK", "1813.HK", "APS.TO", "5406.T", "000903.SZ", "ZIN.L", "ENBI.CN", "CRSQ", "300261.SZ", "MGG.V", "002928.SZ", "HUM.AX", "FPIP.ST", "UNIP3.SA", "000048.SZ", "2376.HK", "AMAOU", "5GG.AX", "WEGOF", "AWTRF", "ROSE.SW", "CDSG", "TRII", "002555.SZ", "000055.SZ", "SASQ.CN", "NICU.V", "NZS.AX", "BCOMF", "000953.SZ", "AYAL.TA", "002692.SZ", "CLH.JO", "THEP.PA", "TPC", "LTMAQ", "ENUA.F", "0R2Y.L", "BGOPF", "KEN.TA", "TANGI.ST", "TEAM.CN", "0118.HK", "EDHN.SW", "RAUTE.HE", "GAPAW", "CBLNF", "PCOR", "49GP.L", "IVC.AX", "JMFINANCIL.NS", "ICLD", "SKA.WA", "7762.T", "GIL.TO", "SKHSF", "SRI.V", "ALWEC.PA", "BFINVEST.NS", "GZF.DE", "ECHO", "600271.SS", "ETG.TO", "IOSP", "CDXFF", "ABSOF", "SYHLF"]
    print("----- The length of all the symbols ", len(allCompaniesSymobls))

    # Process symbols in batches
    for i in range(0, len(allCompaniesSymobls), batch_size):
        batch_symbols = allCompaniesSymobls[i:i + batch_size]
        tasks = [update_marketCap_EValues(entry) for entry in batch_symbols]
        await asyncio.gather(*tasks)

    return 'update_marketCap_EValues_API'







from tqdm import tqdm


def fetch_large_data_from_mongodb(collection, batch_size=1000):
    total_documents = collection.count_documents({})  # Get the total number of documents
    # cursor = collection.find({}).limit(2000)
    cursor = collection.find({}).limit(500)
    
    # Use tqdm for progress bar
    cursor.batch_size(batch_size)
    cursor = tqdm(cursor, total=total_documents, desc="Fetching data from MongoDB", unit="docs")

    data = []
    for document in cursor:
        data.append(document)

    return data


# statisticsDBdataList the statistics database data should be called from the database just one time and
# should be converted to a list and passed in the parameters of the SpecialStatistics function


def DFSpecialStatistics(statisticsDBdataList, given_date, given_sector):
    df = pd.DataFrame(statisticsDBdataList)
    # print("statisticsDBdataList ")
    # print(statisticsDBdataList)

    filtered_data = df[(df['date'] == given_date) & (df['sector'] == given_sector)]
    return filtered_data


def ListSpecialStatistics_Sectors(statisticsDBdataList, given_date, given_sector):
    filtered_data = [
        item for item in statisticsDBdataList
        if item.get('date') == given_date and item.get('sector') == given_sector
    ]
    return filtered_data





def get_date_for_symbol(symbol, df):
    filtered_data = df.loc[df['symbol'] == symbol, 'date'].values
    return filtered_data[0] if len(filtered_data) > 0 else None



from dateutil.relativedelta import relativedelta

def generate_month_end_dates(start_date, end_date):
    # Parse the start and end dates
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Generate a range of dates with month end frequency
    date_range = pd.date_range(start=start_date, end=end_date, freq='M')

    # Adjust the dates to the last day of the month
    last_day_of_month = date_range + pd.offsets.MonthEnd(0)

    return last_day_of_month.strftime('%Y-%m-%d').tolist()




@Quotes_update.get('/v1/SpecialStatisticsAPI_Sectors_Creation')
async def SpecialStatisticsAPIFunction():
    print("Inside SpecialStatisticsAPIFunction")

    statisticsDBdataList = fetch_large_data_from_mongodb(QuotesStatisticsCollection)
    

    ### This returns the values of the sectors that are in the collection of the sectors
    # AllSectorsfrom_DB=sectorsCollection.find({})
    # AllSectors_List = list(AllSectorsfrom_DB)
    # names_list = [item['name'] for item in AllSectors_List]
    # AllSectors_List=names_list

    ## This returns only the sectors with them that are created the statistics
    unique_sectors = QuotesStatisticsCollection.distinct('sector')


    AllSectors_List=unique_sectors

    print("The secotrs with them are the statistcs created ")
    print(AllSectors_List)




    SpecialStatistics_csv_file_path_stockIndexesCSV_Id = '1IYqXr5n1vipIvQ6mlI4duM3GDlDEU-1Yk2OB-nUJ5No'
    SpecialStatisticsDF = read_data_from_sheets(SpecialStatistics_csv_file_path_stockIndexesCSV_Id,"Sheet1")
    
    print(SpecialStatisticsDF)

    for sector in AllSectors_List:

        earliest_date_result = QuotesStatisticsCollection.find(
            {"date": {"$exists": True}, "sector": sector},
            {"_id": 0, "date": 1}
        ).sort("date", 1).limit(1)
        
        earliestDate_DB=earliest_date_result[0]["date"]


        oldest_date_result = QuotesStatisticsCollection.find(
            {"date": {"$exists": True}},
            {"_id": 0, "date": 1}
        ).sort("date", pymongo.ASCENDING).limit(1)

        oldestDate_DB=oldest_date_result[0]["date"]


        date_to_begin=get_date_for_symbol("S_"+sector,SpecialStatisticsDF)

        if date_to_begin==None:
            date_to_begin=oldestDate_DB

        date_to_finish=earliestDate_DB

        date_range = generate_month_end_dates(date_to_begin, date_to_finish)



        print("oldest date in the database for ",sector," is ",oldestDate_DB)  
        print("earliest date in the database for ",sector," is ",earliestDate_DB)  

        print("Date range ")
        print(date_range)





        for date in date_range:
            result = ListSpecialStatistics_Sectors(statisticsDBdataList, date, sector)
            print("Nombre d'éléments qu'on va utiliser dans la création des statistiques spéciales ",len(result))
            # Process the result as needed
            print(f"sector {sector} date {date}: on a n° {len(result)}")

        

        # print("The reuslt of the filtered statistics")
        # print(result)

 
    return 'SpecialStatisticsAPI'







def calculate_average(number_list):
    if not number_list:
        return None

    cleaned_list = [0 if value != value else value for value in number_list]

    average_value = sum(cleaned_list) / len(cleaned_list)
    return average_value


import numpy as np
def calculate_mean(number_list):
    if not number_list:
        return None

    # Replace NaN values with 0
    cleaned_list = [0 if value != value else value for value in number_list]

    mean_value = np.mean(cleaned_list)
    return mean_value


def calculate_max(number_list):
    if not number_list:
        return None

    # Replace NaN values with 0
    cleaned_list = [0 if value != value else value for value in number_list]

    max_value = max(cleaned_list)
    return max_value


def calculate_min(number_list):
    if not number_list:
        return None

    cleaned_list = [0 if value != value else value for value in number_list]

    min_value = min(cleaned_list)
    return min_value


def replace_nan_with_zero(number_list):
    return [0 if np.isnan(value) else value for value in number_list]

def calculate_q10(number_list):
    if not number_list:
        return None
    cleaned_list = replace_nan_with_zero(number_list)
    q10_value = np.percentile(cleaned_list, 10)
    return q10_value

def calculate_q25(number_list):
    if not number_list:
        return None
    cleaned_list = replace_nan_with_zero(number_list)
    q25_value = np.percentile(cleaned_list, 25)
    return q25_value

def calculate_q75(number_list):
    if not number_list:
        return None
    cleaned_list = replace_nan_with_zero(number_list)
    q75_value = np.percentile(cleaned_list, 75)
    return q75_value

def calculate_q50(number_list):
    if not number_list:
        return None
    cleaned_list = replace_nan_with_zero(number_list)
    q50_value = np.median(cleaned_list)
    return q50_value

def calculate_q90(number_list):
    if not number_list:
        return None
    cleaned_list = replace_nan_with_zero(number_list)
    q90_value = np.percentile(cleaned_list, 90)
    return q90_value




@Quotes_update.get('/v1/SpecialStatistics_sector_realTime_data_return/{sector}/{date}')
async def SpecialStatisticsAPIFunction(sector:str,date:str):

    data_to_treat_cursor = QuotesStatisticsCollection.find({"date":date,"sector":sector})
    data_to_treat=list(data_to_treat_cursor)
    print("data_to_treat for {sector} {date} has n° ",len(data_to_treat))



    ### return
    return_0_5y=[]
    return_1y=[]
    return_2y=[]
    return_3y=[]
    return_4y=[]
    return_5y=[]
    return_6y=[]
    return_7y=[]
    return_8y=[]
    return_9y=[]
    return_all=[]



    ###weeklyVol
    weeklyVol_0_5y=[]
    weeklyVol_1y=[]
    weeklyVol_2y=[]
    weeklyVol_3y=[]
    weeklyVol_4y=[]
    weeklyVol_5y=[]
    weeklyVol_6y=[]
    weeklyVol_7y=[]
    weeklyVol_8y=[]
    weeklyVol_9y=[]
    weeklyVol_all=[]

    ###dailyVol
    dailyVol_0_5y=[]
    dailyVol_1y=[]
    dailyVol_2y=[]
    dailyVol_3y=[]
    dailyVol_4y=[]
    dailyVol_5y=[]
    dailyVol_6y=[]
    dailyVol_7y=[]
    dailyVol_8y=[]
    dailyVol_9y=[]
    dailyVol_all=[]

    ### monthlyVol
    monthlyVol_0_5y=[]
    monthlyVol_1y=[]
    monthlyVol_2y=[]
    monthlyVol_3y=[]
    monthlyVol_4y=[]
    monthlyVol_5y=[]
    monthlyVol_6y=[]
    monthlyVol_7y=[]
    monthlyVol_8y=[]
    monthlyVol_9y=[]
    monthlyVol_all=[]



    ### dailyEmaVol
    dailyEmaVol_0_5y=[]
    dailyEmaVol_1y=[]
    dailyEmaVol_2y=[]
    dailyEmaVol_3y=[]
    dailyEmaVol_4y=[]
    dailyEmaVol_5y=[]
    dailyEmaVol_6y=[]
    dailyEmaVol_7y=[]
    dailyEmaVol_8y=[]
    dailyEmaVol_9y=[]
    dailyEmaVol_all=[]


    ### weeklyEmaVol
    weeklyEmaVol_0_5y=[]
    weeklyEmaVol_1y=[]
    weeklyEmaVol_2y=[]
    weeklyEmaVol_3y=[]
    weeklyEmaVol_4y=[]
    weeklyEmaVol_5y=[]
    weeklyEmaVol_6y=[]
    weeklyEmaVol_7y=[]
    weeklyEmaVol_8y=[]
    weeklyEmaVol_9y=[]
    weeklyEmaVol_all=[]




    ### monthlyEmaVol
    monthlyEmaVol_0_5y=[]
    monthlyEmaVol_1y=[]
    monthlyEmaVol_2y=[]
    monthlyEmaVol_3y=[]
    monthlyEmaVol_4y=[]
    monthlyEmaVol_5y=[]
    monthlyEmaVol_6y=[]
    monthlyEmaVol_7y=[]
    monthlyEmaVol_8y=[]
    monthlyEmaVol_9y=[]
    monthlyEmaVol_all=[]








    for element in data_to_treat[:10]:
        # print ("  ")
        return_0_5y.append(element['return']['0.5y'])
        return_1y.append(element['return']['1y'])
        return_2y.append(element['return']['2y'])
        return_3y.append(element['return']['3y'])
        return_4y.append(element['return']['4y'])
        return_5y.append(element['return']['5y'])
        return_6y.append(element['return']['6y'])
        return_7y.append(element['return']['7y'])
        return_8y.append(element['return']['8y'])
        return_9y.append(element['return']['9y'])
        return_all.append(element['return']['all'])

        # print (" weeklyVol treatment ")
        weeklyVol_0_5y.append(element['return']['0.5y'])
        weeklyVol_1y.append(element['weeklyVol']['1y'])
        weeklyVol_2y.append(element['weeklyVol']['2y'])
        weeklyVol_3y.append(element['weeklyVol']['3y'])
        weeklyVol_4y.append(element['weeklyVol']['4y'])
        weeklyVol_5y.append(element['weeklyVol']['5y'])
        weeklyVol_6y.append(element['weeklyVol']['6y'])
        weeklyVol_7y.append(element['weeklyVol']['7y'])
        weeklyVol_8y.append(element['weeklyVol']['8y'])
        weeklyVol_9y.append(element['weeklyVol']['9y'])
        weeklyVol_all.append(element['weeklyVol']['all'])

        # print (" dailyVol treatment ")
        dailyVol_0_5y.append(element['dailyVol']['0.5y'])
        dailyVol_1y.append(element['dailyVol']['1y'])
        dailyVol_2y.append(element['dailyVol']['2y'])
        dailyVol_3y.append(element['dailyVol']['3y'])
        dailyVol_4y.append(element['dailyVol']['4y'])
        dailyVol_5y.append(element['dailyVol']['5y'])
        dailyVol_6y.append(element['dailyVol']['6y'])
        dailyVol_7y.append(element['dailyVol']['7y'])
        dailyVol_8y.append(element['dailyVol']['8y'])
        dailyVol_9y.append(element['dailyVol']['9y'])
        dailyVol_all.append(element['dailyVol']['all'])

        # print (" monthlyVol treatment ")
        monthlyVol_0_5y.append(element['monthlyVol']['0.5y'])
        monthlyVol_1y.append(element['monthlyVol']['1y'])
        monthlyVol_2y.append(element['monthlyVol']['2y'])
        monthlyVol_3y.append(element['monthlyVol']['3y'])
        monthlyVol_4y.append(element['monthlyVol']['4y'])
        monthlyVol_5y.append(element['monthlyVol']['5y'])
        monthlyVol_6y.append(element['monthlyVol']['6y'])
        monthlyVol_7y.append(element['monthlyVol']['7y'])
        monthlyVol_8y.append(element['monthlyVol']['8y'])
        monthlyVol_9y.append(element['monthlyVol']['9y'])
        monthlyVol_all.append(element['monthlyVol']['all'])

        # print (" dailyEmaVol treatment ")
        dailyEmaVol_0_5y.append(element['dailyEmaVol']['0.5y'])
        dailyEmaVol_1y.append(element['dailyEmaVol']['1y'])
        dailyEmaVol_2y.append(element['dailyEmaVol']['2y'])
        dailyEmaVol_3y.append(element['dailyEmaVol']['3y'])
        dailyEmaVol_4y.append(element['dailyEmaVol']['4y'])
        dailyEmaVol_5y.append(element['dailyEmaVol']['5y'])
        dailyEmaVol_6y.append(element['dailyEmaVol']['6y'])
        dailyEmaVol_7y.append(element['dailyEmaVol']['7y'])
        dailyEmaVol_8y.append(element['dailyEmaVol']['8y'])
        dailyEmaVol_9y.append(element['dailyEmaVol']['9y'])
        dailyEmaVol_all.append(element['dailyEmaVol']['all'])

        # print (" weeklyEmaVol treatment ")
        weeklyEmaVol_0_5y.append(element['dailyEmaVol']['0.5y'])
        weeklyEmaVol_1y.append(element['weeklyEmaVol']['1y'])
        weeklyEmaVol_2y.append(element['weeklyEmaVol']['2y'])
        weeklyEmaVol_3y.append(element['weeklyEmaVol']['3y'])
        weeklyEmaVol_4y.append(element['weeklyEmaVol']['4y'])
        weeklyEmaVol_5y.append(element['weeklyEmaVol']['5y'])
        weeklyEmaVol_6y.append(element['weeklyEmaVol']['6y'])
        weeklyEmaVol_7y.append(element['weeklyEmaVol']['7y'])
        weeklyEmaVol_8y.append(element['weeklyEmaVol']['8y'])
        weeklyEmaVol_9y.append(element['weeklyEmaVol']['9y'])
        weeklyEmaVol_all.append(element['weeklyEmaVol']['1y'])

        # print (" monthlyEmaVol treatment ")
        monthlyEmaVol_0_5y.append(element['monthlyEmaVol']['0.5y'])
        monthlyEmaVol_1y.append(element['monthlyEmaVol']['1y'])
        monthlyEmaVol_2y.append(element['monthlyEmaVol']['2y'])
        monthlyEmaVol_3y.append(element['monthlyEmaVol']['3y'])
        monthlyEmaVol_4y.append(element['monthlyEmaVol']['4y'])
        monthlyEmaVol_5y.append(element['monthlyEmaVol']['5y'])
        monthlyEmaVol_6y.append(element['monthlyEmaVol']['6y'])
        monthlyEmaVol_7y.append(element['monthlyEmaVol']['7y'])
        monthlyEmaVol_8y.append(element['monthlyEmaVol']['8y'])
        monthlyEmaVol_9y.append(element['monthlyEmaVol']['9y'])
        monthlyEmaVol_all.append(element['monthlyEmaVol']['all'])

        document = {
                    "monthlyEmaVol": {"0.5y":
                                      {
                                        "min":calculate_min(monthlyEmaVol_0_5y) ,
                                        "max":calculate_max(monthlyEmaVol_0_5y),
                                        "mean":calculate_mean(monthlyEmaVol_0_5y),
                                        "avg":calculate_average(monthlyEmaVol_0_5y),
                                        "q10":calculate_q10(monthlyEmaVol_0_5y),
                                        "q25":calculate_q25(monthlyEmaVol_0_5y),
                                        "q50":calculate_q50(monthlyEmaVol_0_5y),
                                        "q75":calculate_q75(monthlyEmaVol_0_5y),
                                        "q90":calculate_q90(monthlyEmaVol_0_5y)
                                             }, 

                                    "1y":{
                                        "min":calculate_min(monthlyEmaVol_1y) ,
                                        "max":calculate_max(monthlyEmaVol_1y),
                                        "mean":calculate_mean(monthlyEmaVol_1y),
                                        "avg":calculate_average(monthlyEmaVol_1y),
                                        "q10":calculate_q10(monthlyEmaVol_1y),
                                        "q25":calculate_q25(monthlyEmaVol_1y),
                                        "q50":calculate_q50(monthlyEmaVol_1y),
                                        "q75":calculate_q75(monthlyEmaVol_1y),
                                        "q90":calculate_q90(monthlyEmaVol_1y)
                                             }
                                             }
                   
                   }






    # print("")
    # print("return_0_5y",return_0_5y)
    # print("return_1y",return_1y)
    # print("return_2y",return_2y)
    # print("return_3y",return_3y)
    # print("return_4y",return_4y)
    # print("return_5y",return_5y)
    # print("return_6y",return_6y)
    # print("return_7y",return_7y)
    # print("return_8y",return_8y)
    # print("return_9y",return_9y)
    # print("return_all",return_all)
    # print("")

    # print("")
    # print("weeklyVol_0_5y",weeklyVol_0_5y)
    # print("weeklyVol_1y",weeklyVol_1y)
    # print("weeklyVol_2y",weeklyVol_2y)
    # print("weeklyVol_3y",weeklyVol_3y)
    # print("weeklyVol_4y",weeklyVol_4y)
    # print("weeklyVol_5y",weeklyVol_5y)
    # print("weeklyVol_6y",weeklyVol_6y)
    # print("weeklyVol_7y",weeklyVol_7y)
    # print("weeklyVol_8y",weeklyVol_8y)
    # print("weeklyVol_9y",weeklyVol_9y)
    # print("weeklyVol_all",weeklyVol_all)
    # print("")


    # print("")
    # print("dailyVol_0_5y",dailyVol_0_5y)
    # print("dailyVol_1y",dailyVol_1y)
    # print("dailyVol_2y",dailyVol_2y)
    # print("dailyVol_3y",dailyVol_3y)
    # print("dailyVol_4y",dailyVol_4y)
    # print("dailyVol_5y",dailyVol_5y)
    # print("dailyVol_6y",dailyVol_6y)
    # print("dailyVol_7y",dailyVol_7y)
    # print("dailyVol_8y",dailyVol_8y)
    # print("dailyVol_9y",dailyVol_9y)
    # print("dailyVol_all",dailyVol_all)
    # print("")


    # print("")
    # print("monthlyVol_0_5y",monthlyVol_0_5y)
    # print("monthlyVol_1y",monthlyVol_1y)
    # print("monthlyVol_2y",monthlyVol_2y)
    # print("monthlyVol_3y",monthlyVol_3y)
    # print("monthlyVol_4y",monthlyVol_4y)
    # print("monthlyVol_5y",monthlyVol_5y)
    # print("monthlyVol_6y",monthlyVol_6y)
    # print("monthlyVol_7y",monthlyVol_7y)
    # print("monthlyVol_8y",monthlyVol_8y)
    # print("monthlyVol_9y",monthlyVol_9y)
    # print("monthlyVol_all",monthlyVol_all)
    # print("")





    # print("")
    # print("dailyEmaVol_0_5y",dailyEmaVol_0_5y)
    # print("dailyEmaVol_1y",dailyEmaVol_1y)
    # print("dailyEmaVol_2y",dailyEmaVol_2y)
    # print("dailyEmaVol_3y",dailyEmaVol_3y)
    # print("dailyEmaVol_4y",dailyEmaVol_4y)
    # print("dailyEmaVol_5y",dailyEmaVol_5y)
    # print("dailyEmaVol_6y",dailyEmaVol_6y)
    # print("dailyEmaVol_7y",dailyEmaVol_7y)
    # print("dailyEmaVol_8y",dailyEmaVol_8y)
    # print("dailyEmaVol_9y",dailyEmaVol_9y)
    # print("dailyEmaVol_all",dailyEmaVol_all)
    # print("")


    # print("")
    # print("weeklyEmaVol_0_5y",weeklyEmaVol_0_5y)
    # print("weeklyEmaVol_1y",weeklyEmaVol_1y)
    # print("weeklyEmaVol_2y",weeklyEmaVol_2y)
    # print("weeklyEmaVol_3y",weeklyEmaVol_3y)
    # print("weeklyEmaVol_4y",weeklyEmaVol_4y)
    # print("weeklyEmaVol_5y",weeklyEmaVol_5y)
    # print("weeklyEmaVol_6y",weeklyEmaVol_6y)
    # print("weeklyEmaVol_7y",weeklyEmaVol_7y)
    # print("weeklyEmaVol_8y",weeklyEmaVol_8y)
    # print("weeklyEmaVol_9y",weeklyEmaVol_9y)
    # print("weeklyEmaVol_all",weeklyEmaVol_all)
    # print("")

    # print("")
    # print("monthlyEmaVol_0_5y",monthlyEmaVol_0_5y)
    # print("monthlyEmaVol_1y",monthlyEmaVol_1y)
    # print("monthlyEmaVol_2y",monthlyEmaVol_2y)
    # print("monthlyEmaVol_3y",monthlyEmaVol_3y)
    # print("monthlyEmaVol_4y",monthlyEmaVol_4y)
    # print("monthlyEmaVol_5y",monthlyEmaVol_5y)
    # print("monthlyEmaVol_6y",monthlyEmaVol_6y)
    # print("monthlyEmaVol_7y",monthlyEmaVol_7y)
    # print("monthlyEmaVol_8y",monthlyEmaVol_8y)
    # print("monthlyEmaVol_9y",monthlyEmaVol_9y)
    # print("monthlyEmaVol_all",monthlyEmaVol_all)
    # print("")

    return ("SpecialStatistics_sector_realTime_data_return",document)





















