
from APIs.Endpoints1.countries_APIs import CreatingCountries
from APIs.Endpoints1.countries_APIs import CreatingSubregion
from APIs.Endpoints1.exchange_APIs import creatingExchanges
from APIs.Endpoints1.sectors_APIs import create_sectors_from_dataframe
from APIs.Endpoints1.Industries_APIs import create_new_industries
from APIs.Endpoints1.companies_APIs import CompaniesCreationProcess
from APIs.Endpoints1.companies_APIs import Function_Intersection_Old_New_CSV



import time


from apscheduler.schedulers.background import BackgroundScheduler

def run_job():
    start_time_Task_scheduler = time.time()


    print("->->->->->->->-> Début Task scheduler ")

    start_time_create_sectors_from_dataframe = time.time()




    DataFrameToWorkWith=Function_Intersection_Old_New_CSV()













    print("")
    print("->->->->->->->->-> Beggining of the create_sectors_from_dataframe Function -<-<-<-<-<-<-<-<-<")
    create_sectors_from_dataframe(DataFrameToWorkWith)
    # print("->->->->->->->->-> Finishing of the create_sectors_from_dataframe Function -<-<-<-<-<-<-<-<-<")
    print("")
    end_time_create_sectors_from_dataframe = time.time()

    elapsed_time_create_sectors_from_dataframe = end_time_create_sectors_from_dataframe - start_time_create_sectors_from_dataframe
    print("->->->->->->->->->Finishing of the create_sectors_from_dataframe Function Elapsed time: %.2f seconds" % elapsed_time_create_sectors_from_dataframe)



    print("")
    print("->->->->->->->->-> Beggining of the create_new_dataframe_with_sector_industry_info Function -<-<-<-<-<-<-<-<-<")
    create_new_industries(DataFrameToWorkWith)
    print("->->->->->->->->-> Finishing of the create_new_dataframe_with_sector_industry_info Function -<-<-<-<-<-<-<-<-<")
    print("")



    print("")
    print("->->->->->->->->-> Beggining of the creatingExchanges Function -<-<-<-<-<-<-<-<-<")
    creatingExchanges(DataFrameToWorkWith)
    print("->->->->->->->->-> Finishing of the creatingExchanges Function -<-<-<-<-<-<-<-<-<")
    print("")




    print("")
    print("->->->->->->->->-> Beggining of the CreatingSubregion Function -<-<-<-<-<-<-<-<-<")
    CreatingSubregion(DataFrameToWorkWith)
    print("->->->->->->->->-> Finishing of the CreatingSubregion Function -<-<-<-<-<-<-<-<-<")
    print("")



    print("")
    print("->->->->->->->->-> Beggining of the CreatingCountries Function -<-<-<-<-<-<-<-<-<")
    CreatingCountries(DataFrameToWorkWith)
    print("->->->->->->->->-> Finishing of the CreatingCountries Function -<-<-<-<-<-<-<-<-<")
    print("")


    print("")
    print("->->->->->->->->-> Beggining of the creatingCompanies Function -<-<-<-<-<-<-<-<-<")
    CompaniesCreationProcess(DataFrameToWorkWith)
    print("->->->->->->->->-> Finishing of the creatingCompanies Function -<-<-<-<-<-<-<-<-<")
    print("")




    end_timeTotal = time.time()

    elapsed_time_TaskScheduler = end_timeTotal - start_time_Task_scheduler
    print("->->->->->->->->->Finishing of the create_sectors_from_dataframe Function Elapsed time: %.2f seconds" % elapsed_time_TaskScheduler)






scheduler = BackgroundScheduler()

scheduler.add_job(run_job, trigger='interval', seconds=10, max_instances=1)

scheduler.start() 