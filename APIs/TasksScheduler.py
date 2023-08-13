
from APIs.countries_APIs import CreatingCountries
from APIs.countries_APIs import CreatingSubregion

from APIs.exchange_APIs import creatingExchanges
from APIs.sectors_APIs import create_sectors_from_dataframe
from APIs.Industries_APIs import create_new_industries

from APIs.companies_APIs import CompaniesCreationProcess

import time


from apscheduler.schedulers.background import BackgroundScheduler

def run_job():



    start_time_create_sectors_from_dataframe = time.time()

    print("")
    print("->->->->->->->->-> Beggining of the create_sectors_from_dataframe Function -<-<-<-<-<-<-<-<-<")
    create_sectors_from_dataframe()
    # print("->->->->->->->->-> Finishing of the create_sectors_from_dataframe Function -<-<-<-<-<-<-<-<-<")
    print("")
    end_time_create_sectors_from_dataframe = time.time()

    elapsed_time_create_sectors_from_dataframe = end_time_create_sectors_from_dataframe - start_time_create_sectors_from_dataframe
    print("->->->->->->->->->Finishing of the create_sectors_from_dataframe Function Elapsed time: %.2f seconds" % elapsed_time_create_sectors_from_dataframe)



    print("")
    print("->->->->->->->->-> Beggining of the create_new_dataframe_with_sector_industry_info Function -<-<-<-<-<-<-<-<-<")
    create_new_industries()
    print("->->->->->->->->-> Finishing of the create_new_dataframe_with_sector_industry_info Function -<-<-<-<-<-<-<-<-<")
    print("")



    print("")
    print("->->->->->->->->-> Beggining of the creatingExchanges Function -<-<-<-<-<-<-<-<-<")
    creatingExchanges()
    print("->->->->->->->->-> Finishing of the creatingExchanges Function -<-<-<-<-<-<-<-<-<")
    print("")




    print("")
    print("->->->->->->->->-> Beggining of the CreatingSubregion Function -<-<-<-<-<-<-<-<-<")
    CreatingSubregion()
    print("->->->->->->->->-> Finishing of the CreatingSubregion Function -<-<-<-<-<-<-<-<-<")
    print("")



    print("")
    print("->->->->->->->->-> Beggining of the CreatingCountries Function -<-<-<-<-<-<-<-<-<")
    CreatingCountries()
    print("->->->->->->->->-> Finishing of the CreatingCountries Function -<-<-<-<-<-<-<-<-<")
    print("")


    print("")
    print("->->->->->->->->-> Beggining of the creatingCompanies Function -<-<-<-<-<-<-<-<-<")
    CompaniesCreationProcess()
    print("->->->->->->->->-> Finishing of the creatingCompanies Function -<-<-<-<-<-<-<-<-<")
    print("")



scheduler = BackgroundScheduler()

scheduler.add_job(run_job, trigger='interval', seconds=10, max_instances=1)

scheduler.start() 