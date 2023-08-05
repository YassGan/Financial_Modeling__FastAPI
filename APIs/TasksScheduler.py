
from APIs.countries_APIs import CreatingCountries
from APIs.exchange_APIs import creatingExchanges

from apscheduler.schedulers.background import BackgroundScheduler

def run_job():

    creatingExchanges()
    CreatingCountries()



scheduler = BackgroundScheduler()

scheduler.add_job(run_job, trigger='interval', seconds=3600, max_instances=1)

scheduler.start() 