
from APIs.countries_APIs import CreatingCountries
from APIs.countries_APIs import CreatingSubregion

from APIs.exchange_APIs import creatingExchanges
from APIs.sectors_APIs import create_sectors_from_dataframe
from APIs.Industries_APIs import create_new_industries

from APIs.companies_APIs import creatingCompanies




from apscheduler.schedulers.background import BackgroundScheduler

def run_job():

    print("")
    print("->->->->->->->->-> Beggining of the create_sectors_from_dataframe Function -<-<-<-<-<-<-<-<-<")
    create_sectors_from_dataframe()
    print("->->->->->->->->-> Finishing of the create_sectors_from_dataframe Function -<-<-<-<-<-<-<-<-<")
    print("")



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
    creatingCompanies()
    print("->->->->->->->->-> Finishing of the creatingCompanies Function -<-<-<-<-<-<-<-<-<")
    print("")



scheduler = BackgroundScheduler()

scheduler.add_job(run_job, trigger='interval', seconds=10, max_instances=1)

scheduler.start() 