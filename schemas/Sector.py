

from bson import ObjectId


# schemas/Sector.py
def serializeDict2(entity):
    # Convert ObjectId to string if "_id" field exists
    if "_id" in entity and isinstance(entity["_id"], ObjectId):
        entity["_id"] = str(entity["_id"])

    return {
        **{i: str(entity[i]) for i in entity if i == '_id'},
        **{i: entity[i] for i in entity if i != '_id'}
    }

def serializeList2(entity) -> list:
    return [serializeDict2(a) for a in entity]