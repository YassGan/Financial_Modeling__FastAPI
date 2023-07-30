

from fastapi import FastAPI
from APIs.Industries_API import industry 

from APIs.Industry_endpoint import industry 
from APIs.endpoint1 import router 
from APIs.Industries_API import industry 



app = FastAPI()
app.include_router(industry)
app.include_router(router)