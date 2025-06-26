import os
import redis
from datetime import datetime
import pandas as pd
import numpy as np

def fetch_all_data_strip_prefix(redis_client):
    """Fetch all hash keys from Redis, strip the given prefix in the returned dict keys."""
    # redis_client = redis.StrictRedis(
    #     host='127.0.0.1',
    #     port=6379,
    #     db=0,
    #     decode_responses=True
    # )
    prefix="latest:"
    data = {}
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match='*', count=100)
        for key in keys:
            if redis_client.type(key) == 'hash':
                # Determine the output key name: strip prefix if present
                out_key = key
                if key.startswith(prefix):
                    out_key = key[len(prefix):]
                data[out_key] = redis_client.hgetall(key)
        if cursor == 0:
            break
    return data

def rename_hash_keys_remove_prefix(redis_client):
    """
    Rename all hash keys that start with the given prefix by removing the prefix.
    Warning: if a target key (without prefix) already exists, RENAME will fail. 
    This function skips those to avoid overwriting.
    """
    # redis_client = redis.StrictRedis(
    #     host='127.0.0.1',
    #     port=6379,
    #     db=0,
    #     decode_responses=True
    # )
    prefix="latest:"
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=f"{prefix}*", count=100)
        for key in keys:
            if redis_client.type(key) != 'hash':
                continue
            new_key = key[len(prefix):]
            # Check if new_key already exists
            if redis_client.exists(new_key):
                print(f"Skipping rename {key} → {new_key}: target exists")
                continue
            try:
                redis_client.rename(key, new_key)
                print(f"Renamed {key} → {new_key}")
            except redis.ResponseError as e:
                # e.g., if key is modified concurrently or other error
                print(f"Error renaming {key}: {e}")
        if cursor == 0:
            break

def place_order_if_price_match(
    instrument_id, price, order_side, redis_client, trade_book_collection, trade_logs_collection, stop_loss=None
):
    """
    Checks if the given instrument_id exists and price matches (within ±price_tolerance).
    If matched, places the order and logs to trade_book and trade_logs in MongoDB.
    The trade_book and trade_logs will include 'order_placement_price' (redis price), 'order_price' (argument price),
    and an optional 'stop_loss' field if provided.
    """
    price_tolerance = 1
    all_data = fetch_all_data_strip_prefix(redis_client)
    instrument_data = all_data.get(str(instrument_id))
    now = datetime.utcnow().isoformat()
    print(all_data)
    if instrument_data:
        try:
            redis_price = float(instrument_data.get("price", 0))
        except (TypeError, ValueError):
            redis_price = 0.0
        print("REDIS_PRICE =>", redis_price)
        if abs(redis_price - float(price)) <= price_tolerance:
            # Place order
            order_doc = {
                "instrument_id": instrument_id,
                "order_placed_time": now,
                "order_placement_price": redis_price,
                "order_price": float(price),
                "order_side": order_side
            }
            if stop_loss is not None:
                order_doc["stop_loss"] = stop_loss
            trade_book_collection.insert_one(order_doc)

            log_doc = {
                "instrument_id": instrument_id,
                "order_placed_time": now,
                "status": "filled",
                "order_placement_price": redis_price,
                "order_price": float(price),
                "order_side": order_side
            }
            if stop_loss is not None:
                log_doc["stop_loss"] = stop_loss
            trade_logs_collection.insert_one(log_doc)

            result = {
                "status": "order placed",
                "instrument_id": instrument_id,
                "order_placement_price": redis_price,
                "order_price": float(price),
                "order_side": order_side
            }
            if stop_loss is not None:
                result["stop_loss"] = stop_loss
            return result
        else:
            # Price not matched
            log_doc = {
                "instrument_id": instrument_id,
                "order_placed_time": now,
                "status": "price not matched",
                "order_placement_price": redis_price,
                "order_price": float(price),
                "order_side": order_side
            }
            if stop_loss is not None:
                log_doc["stop_loss"] = stop_loss
            trade_logs_collection.insert_one(log_doc)

            result = {
                "status": "price not matched",
                "instrument_id": instrument_id,
                "order_placement_price": redis_price,
                "order_price": float(price),
                "order_side": order_side
            }
            if stop_loss is not None:
                result["stop_loss"] = stop_loss
            return result
    else:
        # Instrument not found
        log_doc = {
            "instrument_id": instrument_id,
            "order_placed_time": now,
            "status": "not filled",
            "order_placement_price": None,
            "order_price": float(price),
            "order_side": order_side
        }
        if stop_loss is not None:
            log_doc["stop_loss"] = stop_loss
        trade_logs_collection.insert_one(log_doc)

        result = {
            "status": "instrument not found",
            "instrument_id": instrument_id,
            "order_placement_price": None,
            "order_price": float(price),
            "order_side": order_side
        }
        if stop_loss is not None:
            result["stop_loss"] = stop_loss
        return result

    
def square_off(instrument_id, trade_book_collection, trade_logs_collection, redis_client):
    position = pd.DataFrame(trade_book_collection.find(
        {
            "instrument_id": instrument_id
        },
        {
            "_id": 0
        }
    ))
    if not position.empty and "order_placed_time" in position.columns:
        position["order_placed_time"] = pd.to_datetime(position["order_placed_time"], errors="coerce")
        position = position.sort_values("order_placed_time")
        position["order_placed_time"] = position["order_placed_time"].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    if not position.empty:
        # Get the latest price from Redis for this instrument
        live_data = fetch_all_data_strip_prefix(redis_client=None if 'redis_client' not in globals() else redis_client)
        # If redis_client is not available, you may need to pass it as an argument to square_off
        instrument_id_str = str(instrument_id)
        instrument_data = live_data.get(instrument_id_str)
        if instrument_data and "price" in instrument_data:
            price = float(instrument_data["price"])
            now = datetime.utcnow().isoformat()
            order_doc = {
            "instrument_id": instrument_id,
            "order_placed_time": now,
            "order_placement_price": price,
            "order_price": price,
            "order_side": "sell"
            }
            trade_book_collection.insert_one(order_doc)
            log_doc = {
            "instrument_id": instrument_id,
            "order_placed_time": now,
            "status": "filled",
            "order_placement_price": price,
            "order_price": price,
            "order_side": "sell"
            }
            trade_logs_collection.insert_one(log_doc)
            print({"status": "square off order placed", "order": order_doc})
            return {"status": "square off order placed", "order": order_doc}
        else:
            print({"status": "instrument not found in live data", "instrument_id": instrument_id})
            return {"status": "instrument not found in live data", "instrument_id": instrument_id}

def pending_list_orders(redis_client, trade_logs_collection, trade_book_collection):
    not_filled_orders = trade_logs_collection.find(
        {
            "status": "not filled"
        },
        {
            "_id": 0
        }
    )
    # Convert cursor to list for processing
    not_filled_orders_list = list(not_filled_orders)
    # Fetch live market data
    live_data = fetch_all_data_strip_prefix(redis_client)
    for order in not_filled_orders_list:
        instrument_id = order.get("instrument_id")
        order_placement_price = float(order.get("order_placement_price", 0))
        order_price = float(order.get("order_price", 0))
        live_info = live_data.get(instrument_id)
        if live_info:
            live_price = float(live_info.get("price", 0))
            # Check if price matches or difference is within [-1, +1]
            if abs(live_price - order_placement_price) <= 1:
                # Update status in trade_logs_collection
                trade_logs_collection.update_one(
                    {
                        "instrument_id": instrument_id,
                        "order_placed_time": order.get("order_placed_time"),
                        "status": "not filled"
                    },
                    {
                        "$set": {"status": "filled"}
                    }
                )
                # Save details in trade_book_collection
                trade_book_collection.insert_one({
                    "instrument_id": instrument_id,
                    "order_placed_time": order.get("order_placed_time"),
                    "status": "filled",
                    "order_placement_price": order_placement_price,
                    "order_price": order_price
                })
    filled_orders = [
        order for order in not_filled_orders_list
        if live_data.get(order.get("instrument_id")) and
        abs(float(live_data[order.get("instrument_id")].get("price", 0)) - float(order.get("order_placement_price", 0))) <= 1
    ]
    if filled_orders:
        print({"status": "order(s) filled", "orders": filled_orders})
        return {"status": "order(s) filled", "orders": filled_orders}
    else:
        return {"status": "no order has been filled"}
    
def implement_stop_loss(redis_client, trade_logs_collection, trade_book_collection):
    stop_loss_orders = list(trade_book_collection.find(
        {
            "stop_loss": 
                    {
                        "$exists": True
                    }
        }, 
        {
            "_id": 0
        }
    ))
    print(stop_loss_orders)
    live_data = fetch_all_data_strip_prefix(redis_client)
    # print(live_data)
    for order in stop_loss_orders:
        instrument_id = order.get("instrument_id")
        stop_loss = float(order.get("stop_loss", 0))
        order_placement_price = float(order.get("order_placement_price", 0))
        live_info = live_data.get(str(instrument_id))
        if live_info:
            live_price = float(live_info.get("price", 0))
            diff = abs(live_price - order_placement_price)
            print("DIF", diff)
            if diff <= stop_loss:
                trade_book_collection.delete_one({
                    "instrument_id": instrument_id,
                    "order_placed_time": order.get("order_placed_time"),
                    "order_placement_price": order_placement_price,
                    "stop_loss": order.get("stop_loss")
                })
                # Update status in trade_logs_collection as well
                trade_logs_collection.update_many(
                    {
                        "instrument_id": instrument_id,
                        "order_placed_time": order.get("order_placed_time"),
                        "order_placement_price": order_placement_price,
                        "stop_loss": order.get("stop_loss"),
                        "status": "filled"
                    },
                    {
                        "$set": {"status": "stop loss triggered"}
                    }
                )
                return {
                    "status": "stop loss triggered",
                    "instrument_id": instrument_id,
                    "order_placed_time": order.get("order_placed_time"),
                    "order_placement_price": order_placement_price,
                    "stop_loss": order.get("stop_loss")
                }

if __name__ == "__main__":
        # Example: just fetch with stripped keys
    from pymongo import MongoClient
    import redis
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    DB_NAME = os.getenv("DB_NAME", "market_data_db")
    # MASTER_COLLECTION = os.getenv("MASTER_COLLECTION", "master_file")
    # SUBSCRIPTION_COLLECTION = os.getenv("SUBSCRIPTION_COLLECTION", "subscriptions")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    master_collection = db["master_file"]
    subscription_collection = db["subscriptions"]
    trade_book_collection = db["trade_book"]
    trade_logs_collection = db["trade_logs"]

    redis_client = redis.StrictRedis(
        host='127.0.0.1',
        port=6379,
        db=0,
        decode_responses=True
    )
    square_off("1_2", trade_book_collection, trade_logs_collection, redis_client)
    # while True:
    #     print(pending_list_orders(redis_client, trade_logs_collection, trade_book_collection))
    # If you want to actually rename keys in Redis:
    # rename_hash_keys_remove_prefix()
