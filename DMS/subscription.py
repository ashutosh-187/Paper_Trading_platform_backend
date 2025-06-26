from typing import List
from pymongo.collection import Collection


def subscribe_instrument(
    master_collection: Collection,
    subscription_collection: Collection,
    data: dict
) -> dict:
    """
    Save subscription data to MongoDB collection.
    First verifies that instrument_id and instrument_name exist in master_collection.
    """
    instrument_id = data.get("instrument_id")
    instrument_name = data.get("instrument_name")
    # Validate against master file
    record = master_collection.find_one({
        "instrument_id": instrument_id,
        "instrument_name": instrument_name
    })
    if not record:
        raise ValueError("Instrument ID and name do not match any master record")
    # Save subscription
    result = subscription_collection.insert_one(data)
    return {"inserted_id": str(result.inserted_id)}


def unsubscribe_instrument(collection: Collection, instrument_id: str) -> dict:
    """
    Remove a subscription by instrument_id from MongoDB collection.
    """
    result = collection.delete_one({"instrument_id": instrument_id})
    if result.deleted_count == 0:
        raise Exception("Subscription not found")
    return {"deleted_count": result.deleted_count}


def list_subscriptions(collection: Collection) -> List[dict]:
    """
    Retrieve all subscriptions from MongoDB collection.
    """
    docs = list(collection.find({}, {"_id": 0}))
    return docs