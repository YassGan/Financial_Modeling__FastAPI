

from fastapi import FastAPI



# from APIs.Industry_endpoint import End 
# from APIs.endpoint1 import router 
# from APIs.Industries_API import industry 
from APIs.mainAPIs import Main 



app = FastAPI()
# app.include_router(industry)
# app.include_router(router)
# app.include_router(End)

app.include_router(Main)
