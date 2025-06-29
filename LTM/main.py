from datetime import datetime
from pymongo import MongoClient
import os
import redis

def analyze_trade_latency(trade_logs_collection):
    """
    Analyzes a list of trade logs from a MongoDB collection to calculate
    the latency between order placement and execution.

    For each trade that has been executed ('filled'), this function calculates:
    1. Absolute Latency: The time difference in microseconds.
    2. Percentage Latency: The time difference as a percentage relative to the
       total time from the Unix epoch to the order placement time.

    Args:
        trade_logs_collection: A pymongo collection object for the trade logs.
    """
    print("--- Trade Latency Analysis ---")

    # Fetch all documents from the collection.
    # It's generally safer to handle the cursor directly.
    trade_logs = list(trade_logs_collection.find({}))

    for trade in trade_logs:
        # Check if the order has an execution time. Latency can only be
        # calculated for executed/filled orders.
        if trade.get("order_execution_time"):
            try:
                # CORRECTED: Handle BSON ObjectId from pymongo
                # by checking its type before trying to access it like a dictionary.
                if isinstance(trade.get("_id"), dict):
                    trade_id = trade["_id"].get("$oid", "N/A")
                else:
                    trade_id = str(trade.get("_id", "N/A"))

                # Convert time strings to datetime objects.
                placement_time_str = trade["order_placement_time"]
                execution_time_str = trade["order_execution_time"]
                
                placement_time = datetime.fromisoformat(placement_time_str)
                execution_time = datetime.fromisoformat(execution_time_str)

                # 1. Calculate the absolute latency
                latency = execution_time - placement_time
                latency_in_micros = latency.total_seconds() * 1_000_000

                # 2. Calculate the "percentage" latency.
                placement_timestamp_secs = placement_time.timestamp()
                # Avoid division by zero for very old timestamps
                if placement_timestamp_secs > 0:
                    percentage_latency = (latency.total_seconds() / placement_timestamp_secs) * 100
                else:
                    percentage_latency = 0

                # Print the results for the current trade
                print(f"\nTrade ID: {trade_id}")
                print(f"  Status: {trade.get('status', 'N/A')}")
                print(f"  Order Placement Time: {placement_time_str}")
                print(f"  Order Execution Time: {execution_time_str}")
                print("-" * 25)
                print(f"  => Absolute Latency: {latency_in_micros:,.0f} microseconds")
                print(f"  => Percentage Latency: {percentage_latency:.8f} %")

            except (KeyError, TypeError, AttributeError) as e:
                # Get the trade ID safely for the error message
                error_trade_id = str(trade.get("_id", "Unknown ID"))
                print(f"\nSkipping record {error_trade_id} due to missing or invalid data: {e}")
        else:
            # Handle trades that were not executed
            # CORRECTED: Safely handle the _id for non-executed trades as well.
            if isinstance(trade.get("_id"), dict):
                trade_id = trade.get("_id", {}).get("$oid", "N/A")
            else:
                trade_id = str(trade.get("_id", "N/A"))
                
            print(f"\nTrade ID: {trade_id}")
            print(f"  Status: {trade.get('status', 'N/A')}")
            print("  => Latency not applicable (order not executed).")

    print("\n--- End of Analysis ---")


if __name__ == "__main__":
    # --- Database and Redis Connection Setup ---
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    DB_NAME = os.getenv("DB_NAME", "market_data_db")
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        
        # Define collections
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
        
        # --- Run Analysis ---
        analyze_trade_latency(trade_logs_collection)

    except Exception as e:
        print(f"An error occurred during setup or execution: {e}")

