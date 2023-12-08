from pymongo import MongoClient
from pymongo.database import Database



######### La première base de données utilisée
# #Old Satured DataBase 
# Define the MongoDB Cloud credentials and database details as variables
# USERNAME = "financialmetamodeling"
# PASSWORD = "financialmetamodeling"
# CLUSTER_NAME = "cluster0"
# DATABASE_NAME = "Financial_Meta_ModelingDB"


# # mongodb+srv://financialmetamodeling:financialmetamodeling@cluster0.h6y69zd.mongodb.net/Financial_Meta_ModelingDB?retryWrites=true&w=majority
# MONGO_CONNECTION_STRING = f"mongodb+srv://{USERNAME}:{PASSWORD}@{CLUSTER_NAME}.h6y69zd.mongodb.net/{DATABASE_NAME}?retryWrites=true&w=majority"




######## La deuxième base de données utilisée
USERNAME = "mupeers1000"
PASSWORD = "mupeers1000"
CLUSTER_NAME = "cluster0"
DATABASE_NAME = "Financial_Meta_ModelingDB"


# # # mongodb+srv://mupeers1000:mupeers1000@cluster0.jw2gohm.mongodb.net/Financial_Meta_ModelingDB?retryWrites=true&w=majority
# MONGO_CONNECTION_STRING = f"mongodb+srv://{USERNAME}:{PASSWORD}@{CLUSTER_NAME}.jw2gohm.mongodb.net/{DATABASE_NAME}?retryWrites=true&w=majority"




######### La base de données payante 
MONGO_CONNECTION_STRING = "mongodb+srv://mupeers:mupeers@stockdata.0hno10f.mongodb.net/mupeers?retryWrites=true&w=majority"



client = MongoClient(MONGO_CONNECTION_STRING, maxPoolSize=10)
#Using connection Pooling to reuse established connections instead of creating a new connection every time. This can significantly reduce connection overhead.
def get_database() -> Database:
    try:
        import time
        start_time = time.time()
        db = client.get_database()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print("Successfully connected to the database. Elapsed time: %.2f seconds" % elapsed_time)
        return db
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise



