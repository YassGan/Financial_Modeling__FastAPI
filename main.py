

from fastapi import FastAPI




from APIs.Endpoints1.mainAPIs import Main 
from APIs.Endpoints1.countries_APIs import Country 
from APIs.Endpoints1.exchange_APIs import Exchange 
from APIs.Endpoints1.utilsFunctionalitites_APIs import UtilsFunc 
from APIs.Endpoints1.sectors_APIs import Sector 
from APIs.Endpoints1.companies_APIs import Company 
from APIs.Endpoints1.Industries_APIs import Industry 
from APIs.Endpoints1.companiesFiltering import CompanyFiltering 


from APIs.Endpoints2.balanceSheetAnnual import BS_Annual 


#import APIs.dataManipulation



# le déclencheur du scheduler se fait quand on importe le fichier qui contient le code du scheduler
#import APIs.Endpoints1.TasksScheduler 


app = FastAPI()


app.include_router(Main)
app.include_router(Country)
app.include_router(Exchange)
app.include_router(UtilsFunc)
app.include_router(Sector)
app.include_router(Industry)

app.include_router(Company)
app.include_router(CompanyFiltering)


app.include_router(BS_Annual)

