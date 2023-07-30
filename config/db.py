from pymongo import MongoClient
from pymongo.database import Database

# Define the MongoDB Cloud credentials and database details as variables
USERNAME = "financialmetamodeling"
PASSWORD = "financialmetamodeling"
CLUSTER_NAME = "cluster0"
DATABASE_NAME = "Financial_Meta_ModelingDB"


# mongodb+srv://financialmetamodeling:financialmetamodeling@cluster0.h6y69zd.mongodb.net/Financial_Meta_ModelingDB?retryWrites=true&w=majority

MONGO_CONNECTION_STRING = f"mongodb+srv://{USERNAME}:{PASSWORD}@{CLUSTER_NAME}.h6y69zd.mongodb.net/{DATABASE_NAME}?retryWrites=true&w=majority"



def get_database() -> Database:
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client.get_database()
        print("Successfully connected to the database.")
        return db
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise
