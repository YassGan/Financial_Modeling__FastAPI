

from fastapi import FastAPI




from APIs.Endpoints1.mainAPIs import Main 
from APIs.Endpoints1.countries_APIs import Country 
from APIs.Endpoints1.exchange_APIs import Exchange 
from APIs.Endpoints1.utilsFunctionalitites_APIs import UtilsFunc 
from APIs.Endpoints1.sectors_APIs import Sector 
from APIs.Endpoints1.companies_APIs import Company 
from APIs.Endpoints1.Industries_APIs import Industry 
from APIs.Endpoints1.companiesFiltering import CompanyFiltering 


from APIs.Endpoints2.Financial_Information import Financial_Info 
from APIs.Endpoints2.Get_Financial_Information import Get_Financial_Info 
from APIs.Endpoints2.Quotes import Quotes 
from APIs.Endpoints2.Get_Quotes import Get_Quotes 


from APIs.Endpoints3.FOREX import FOREX 
from APIs.Endpoints3.FOREXQuotes import FOREX_Quotes 



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


app.include_router(Financial_Info)
app.include_router(Get_Financial_Info)

app.include_router(Quotes)
app.include_router(Get_Quotes)


app.include_router(FOREX)
app.include_router(FOREX_Quotes)

