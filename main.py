

from fastapi import FastAPI




from APIs.mainAPIs import Main 
from APIs.countries_APIs import Country 
from APIs.exchange_APIs import Exchange 
from APIs.utilsFunctionalitites_APIs import UtilsFunc 
from APIs.sectors_APIs import Sector 
from APIs.companies_APIs import Company 

from APIs.Industries_APIs import Industry 

import APIs.TasksScheduler 


app = FastAPI()


app.include_router(Main)
app.include_router(Country)
app.include_router(Exchange)
app.include_router(UtilsFunc)
app.include_router(Sector)
app.include_router(Industry)

app.include_router(Company)


