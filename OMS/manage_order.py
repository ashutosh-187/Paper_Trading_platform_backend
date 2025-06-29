import os
import redis
from datetime import datetime
import pandas as pd
import numpy as np
from pymongo import MongoClient

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
    The trade_book and trade_logs will include 'order_placement_time', 'order_execution_time', 
    'order_placement_price' (redis price), 'order_price' (argument price),
    and an optional 'stop_loss' field if provided.
    """
    price_tolerance = 1
    all_data = fetch_all_data_strip_prefix(redis_client)
    instrument_data = all_data.get(str(instrument_id))
    placement_time = datetime.utcnow().isoformat()
    
    # print(all_data)
    if instrument_data:
        try:
            redis_price = float(instrument_data.get("price", 0))
        except (TypeError, ValueError):
            redis_price = 0.0
        print("REDIS_PRICE =>", redis_price)

        if abs(redis_price - float(price)) <= price_tolerance:
            # Price matched, so placement and execution happen at the same time.
            execution_time = placement_time
            
            # Create the document for the trade book
            order_doc = {
                "instrument_id": instrument_id,
                "order_placement_time": placement_time,
                "order_execution_time": execution_time,
                "order_placement_price": redis_price,
                "order_price": float(price),
                "order_side": order_side
            }
            if stop_loss is not None:
                order_doc["stop_loss"] = stop_loss
            trade_book_collection.insert_one(order_doc)

            # Create the log document
            log_doc = {
                "instrument_id": instrument_id,
                "order_placement_time": placement_time,
                "order_execution_time": execution_time,
                "status": "filled",
                "order_placement_price": redis_price,
                "order_price": float(price),
                "order_side": order_side
            }
            if stop_loss is not None:
                log_doc["stop_loss"] = stop_loss
            trade_logs_collection.insert_one(log_doc)

            # Prepare the result to return
            result = {
                "status": "order placed",
                "instrument_id": instrument_id,
                "order_placement_time": placement_time,
                "order_execution_time": execution_time,
                "order_placement_price": redis_price,
                "order_price": float(price),
                "order_side": order_side
            }
            if stop_loss is not None:
                result["stop_loss"] = stop_loss
            return result
        else:
            # Price not matched, log it but no execution.
            log_doc = {
                "instrument_id": instrument_id,
                "order_placement_time": placement_time,
                "order_execution_time": None, # Not executed
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
                "order_placement_time": placement_time,
                "order_execution_time": None,
                "order_placement_price": redis_price,
                "order_price": float(price),
                "order_side": order_side
            }
            if stop_loss is not None:
                result["stop_loss"] = stop_loss
            return result
    else:
        # Instrument not found, log it but no execution.
        log_doc = {
            "instrument_id": instrument_id,
            "order_placement_time": placement_time,
            "order_execution_time": None, # Not executed
            "status": "not filled", # Using "not filled" for consistency with pending_list_orders
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
            "order_placement_time": placement_time,
            "order_execution_time": None,
            "order_placement_price": None,
            "order_price": float(price),
            "order_side": order_side
        }
        if stop_loss is not None:
            result["stop_loss"] = stop_loss
        return result
    
def square_off(instrument_id, trade_book_collection, trade_logs_collection, redis_client):
    """ Squares off a position, adding placement and execution times. """
    position_df = pd.DataFrame(list(trade_book_collection.find(
        {"instrument_id": instrument_id},
        {"_id": 0}
    )))

    if not position_df.empty:
        # Get the latest price from Redis for this instrument
        live_data = fetch_all_data_strip_prefix(redis_client)
        instrument_id_str = str(instrument_id)
        instrument_data = live_data.get(instrument_id_str)

        if instrument_data and "price" in instrument_data:
            price = float(instrument_data["price"])
            # Placement and execution are simultaneous for a market square-off
            event_time = datetime.utcnow().isoformat()
            
            order_doc = {
                "instrument_id": instrument_id,
                "order_placement_time": event_time,
                "order_execution_time": event_time,
                "order_placement_price": price,
                "order_price": price,
                "order_side": "sell" # Assuming square off is always a sell
            }
            trade_book_collection.insert_one(order_doc)
            
            log_doc = {
                "instrument_id": instrument_id,
                "order_placement_time": event_time,
                "order_execution_time": event_time,
                "status": "filled",
                "order_placement_price": price,
                "order_price": price,
                "order_side": "sell"
            }
            trade_logs_collection.insert_one(log_doc)
            
            result = {"status": "square off order placed", "order": order_doc}
            print(result)
            return result
        else:
            result = {"status": "instrument not found in live data", "instrument_id": instrument_id}
            print(result)
            return result
    else:
        result = {"status": "no open position found to square off", "instrument_id": instrument_id}
        print(result)
        return result

def pending_list_orders(redis_client, trade_logs_collection, trade_book_collection):
    """ Executes pending orders if price matches, adding execution time. """
    not_filled_orders = list(trade_logs_collection.find(
        {"status": "price not matched"},
        {"_id": 0}
    ))
    
    if not not_filled_orders:
        return {"status": "no pending orders to check"}

    live_data = fetch_all_data_strip_prefix(redis_client)
    filled_orders = []

    for order in not_filled_orders:
        instrument_id = order.get("instrument_id")
        # The original placement price is the price from the user's order
        order_price = float(order.get("order_price", 0)) 
        live_info = live_data.get(str(instrument_id))

        if live_info:
            live_price = float(live_info.get("price", 0))
            # Check if price matches or difference is within tolerance
            if abs(live_price - order_price) <= 1:
                execution_time = datetime.utcnow().isoformat()
                
                # Update status and add execution time in trade_logs
                trade_logs_collection.update_one(
                    {
                        "instrument_id": instrument_id,
                        "order_placement_time": order.get("order_placement_time"),
                        "status": "price not matched"
                    },
                    {
                        "$set": {
                            "status": "filled",
                            "order_execution_time": execution_time,
                            "order_placement_price": live_price # Update with actual execution price
                        }
                    }
                )
                
                # Add the executed order to the trade_book
                trade_book_doc = {
                    "instrument_id": instrument_id,
                    "order_placement_time": order.get("order_placement_time"),
                    "order_execution_time": execution_time,
                    "order_placement_price": live_price,
                    "order_price": order_price,
                    "order_side": order.get("order_side"),
                }
                if order.get("stop_loss") is not None:
                    trade_book_doc["stop_loss"] = order.get("stop_loss")

                trade_book_collection.insert_one(trade_book_doc)
                
                order['status'] = 'filled'
                order['order_execution_time'] = execution_time
                filled_orders.append(order)

    if filled_orders:
        result = {"status": "order(s) filled", "orders": filled_orders}
        print(result)
        return result
    else:
        return {"status": "no order has been filled"}

def implement_stop_loss(redis_client, trade_logs_collection, trade_book_collection):
    """ Triggers stop loss and records the execution time. """
    # Find orders with a stop loss in the trade book (active positions)
    stop_loss_orders = list(trade_book_collection.find(
        {"stop_loss": {"$exists": True, "$ne": None}}, 
        {"_id": 0}
    ))

    if not stop_loss_orders:
        return {"status": "no active stop loss orders found"}

    print(f"Found active stop loss orders: {stop_loss_orders}")
    live_data = fetch_all_data_strip_prefix(redis_client)
    
    for order in stop_loss_orders:
        instrument_id = order.get("instrument_id")
        stop_loss_price = float(order.get("stop_loss", 0))
        live_info = live_data.get(str(instrument_id))

        if live_info:
            live_price = float(live_info.get("price", 0))
            
            # Simple stop-loss logic: if live price hits or goes below the stop loss price
            if live_price <= stop_loss_price:
                execution_time = datetime.utcnow().isoformat()
                print(f"Stop loss triggered for {instrument_id} at price {live_price}")

                # Create a square-off order in the trade book
                square_off_doc = {
                    "instrument_id": instrument_id,
                    "order_placement_time": execution_time,
                    "order_execution_time": execution_time,
                    "order_placement_price": live_price,
                    "order_price": live_price,
                    "order_side": "sell", # Assuming the original was a buy
                    "reason": "stop loss triggered"
                }
                trade_book_collection.insert_one(square_off_doc)

                # Update the original order log to show it was closed by stop loss
                trade_logs_collection.update_one(
                    {
                        "instrument_id": instrument_id,
                        "order_placement_time": order.get("order_placement_time")
                    },
                    {
                        "$set": {
                            "status": "stop loss triggered",
                            "stop_loss_execution_time": execution_time,
                            "stop_loss_execution_price": live_price
                        }
                    }
                )
                
                # Delete the original buy order from the trade book as it's now closed.
                trade_book_collection.delete_one({
                    "instrument_id": instrument_id,
                    "order_placement_time": order.get("order_placement_time")
                })

                return {
                    "status": "stop loss triggered",
                    "instrument_id": instrument_id,
                    "execution_time": execution_time,
                    "execution_price": live_price
                }
    return {"status": "no stop loss conditions met"}


if __name__ == "__main__":
    # Example connection setup
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    DB_NAME = os.getenv("DB_NAME", "market_data_db")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    trade_book_collection = db["trade_book"]
    trade_logs_collection = db["trade_logs"]

    redis_client = redis.StrictRedis(
        host='127.0.0.1',
        port=6379,
        db=0,
        decode_responses=True
    )

    # --- Example Usage ---

    # Example 1: Place an order that might not fill immediately
    # print("Placing a new order...")
    # place_order_if_price_match(
    #     instrument_id="101", 
    #     price=500, 
    #     order_side="buy", 
    #     redis_client=redis_client, 
    #     trade_book_collection=trade_book_collection, 
    #     trade_logs_collection=trade_logs_collection, 
    #     stop_loss=495
    # )

    # Example 2: Check for pending orders to execute them
    # print("\nChecking for pending orders...")
    # pending_list_orders(redis_client, trade_logs_collection, trade_book_collection)
    
    # Example 3: Check if any stop losses should be triggered
    # print("\nChecking for stop loss triggers...")
    # implement_stop_loss(redis_client, trade_logs_collection, trade_book_collection)

    # Example 4: Square off a position manually
    # print("\nSquaring off a position...")
    # square_off("1_2", trade_book_collection, trade_logs_collection, redis_client)

    # To run a continuous loop
    # import time
    # while True:
    #     print("--- Cycle Start ---")
    #     pending_list_orders(redis_client, trade_logs_collection, trade_book_collection)
    #     implement_stop_loss(redis_client, trade_logs_collection, trade_book_collection)
    #     print("--- Cycle End ---\n")
    #     time.sleep(5) # Wait for 5 seconds before next cycle
