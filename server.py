import os
from flask import Flask, request, jsonify, abort
from pymongo import MongoClient
import threading
import time

import redis

from DMS.master_file import (
    create_master_file, 
    get_master_file
)
from DMS.subscription import (
    subscribe_instrument, 
    unsubscribe_instrument, 
    list_subscriptions
)
from OMS.manage_order import (
    fetch_all_data_strip_prefix, 
    place_order_if_price_match,
    pending_list_orders,
    implement_stop_loss
)

from RMS.pnl_summary import (
    calculate_mtm_pnl
)
from RMS.alerts import (
    check_trade_losses
)

# from LTM.main import(
#     start_dashboard
# )
# Initialize Flask app
app = Flask(__name__)

# Setup MongoDB client and collections
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

# Master File Routes
@app.route('/master_file', methods=['POST'])
def post_master_file():
    try:
        result = create_master_file(master_collection)
        return jsonify(result), 201
    except Exception as e:
        abort(500, description=str(e))

@app.route('/master_file', methods=['GET'])
def read_master_file():
    try:
        data = get_master_file(master_collection)
        return jsonify(data)
    except Exception as e:
        abort(500, description=str(e))

# Subscription Routes
@app.route('/subscribe', methods=['POST'])
def subscribe():
    try:
        req = request.get_json()
        if not all(k in req for k in ("instrument_id", "instrument_name")):
            abort(400, description="Missing fields")
        result = subscribe_instrument(
            master_collection,
            subscription_collection,
            req
        )
        return jsonify(result), 201
    except ValueError as ve:
        abort(400, description=str(ve))
    except Exception as e:
        abort(500, description=str(e))

@app.route('/unsubscribe', methods=['POST'])
def unsubscribe():
    try:
        req = request.get_json()
        instrument_id = req.get('instrument_id')
        if not instrument_id:
            abort(400, description="instrument_id required")
        result = unsubscribe_instrument(subscription_collection, instrument_id)
        return jsonify(result)
    except Exception as e:
        abort(500, description=str(e))

@app.route('/subscriptions', methods=['GET'])
def subscription_list():
    try:
        data = list_subscriptions(subscription_collection)
        return jsonify(data)
    except Exception as e:
        abort(500, description=str(e))


@app.route("/get_prices", methods=["GET"])
def get_prices():
    try:
        data = fetch_all_data_strip_prefix(redis_client)
        return jsonify(data)
    except Exception as e:
        abort(500, description=str(e))

@app.route("/place_order", methods=["POST"])
def make_trade():
    req = request.get_json()
    req_body_instrument_id = req.get("instrument_id")
    req_body_price = req.get("price")
    req_body_stop_loss = req.get("stop_loss")
    req_body_order_side = req.get("order_side")
    response = place_order_if_price_match(req_body_instrument_id, req_body_price, req_body_order_side, redis_client, trade_book_collection, trade_logs_collection, req_body_stop_loss)
    if response.get("status") == "order placed":
        return jsonify(response), 201
    else:
        abort(400, description=str(response))

@app.route("/get_mtm", methods=["GET"])
def net_pnl():
    response = calculate_mtm_pnl(trade_book_collection, redis_client)
    return jsonify(response)

@app.route("/get_alerts", methods=["GET"])
def trade_alerts():
    response = check_trade_losses(redis_client, trade_book_collection)
    print(response)
    return jsonify(response)

# @app.route("/get_latency", methods=["GET"])
# def latency():
#     response = start_dashboard(redis_client, db)
#     print(response)
#     return jsonify(response)

# def pending_list_thread():
#     print("Starting pending list thread...")
#     while True:
#         try:
#             # Call with correct arguments in correct order
#             result = pending_list_orders(redis_client, trade_logs_collection, trade_book_collection)
#             if result.get("status") == "order(s) filled":
#                 print(f"Orders filled: {result}")
#             # Add sleep to prevent high CPU usage and allow other processes to run
#             time.sleep(5)  # Check every 5 seconds
#         except Exception as e:
#             print(f"Error in pending_list_thread: {e}")
#             time.sleep(5)  # Wait before retrying on error
# # Start the thread
# print("Initializing pending list thread...")
# thread = threading.Thread(target=pending_list_thread, daemon=True)
# thread.start()
# print("Pending list thread started!")

def pending_list_thread():
    print("Starting pending list thread...")
    while True:
        try:
            # Check pending orders
            result = pending_list_orders(redis_client, trade_logs_collection, trade_book_collection)
            if result.get("status") == "order(s) filled":
                print(f"Pending list update: {result}")
            # Check stop-loss orders
            # sl_result = implement_stop_loss(redis_client, trade_logs_collection, trade_book_collection)
            # if sl_result:
            #     print(f"Stop loss triggered: {sl_result}")
            # Sleep to avoid busy loop
            # adjust interval as needed
            time.sleep(1)
        except Exception as e:

            time.sleep(5)

def stop_loss_thread():
    print("Starting stop-loss thread...")
    while True:
        try:
            sl_result = implement_stop_loss(redis_client, trade_logs_collection, trade_book_collection)
            # if sl_result:
                # print(f"Stop loss triggered: {sl_result}")
            time.sleep(5)  # interval for stop-loss checks
        except Exception as e:
            print(f"Error in stop_loss_thread: {e}")
            time.sleep(5)

def alert_thread():
    print("Starting alert thread...")
    while True:
        try:
            sl_result = check_trade_losses(redis_client, trade_book_collection)
            # if sl_result:
            #     print(f"Alert triggered: {sl_result}")
            time.sleep(5)  # interval for stop-loss checks
        except Exception as e:
            print(f"Error in alert thread: {e}")
            time.sleep(5)

# Start pending-list thread
print("Initializing pending list thread...")
thread_pending = threading.Thread(target=pending_list_thread, daemon=True)
thread_pending.start()
print("Pending list thread started!")

# Start stop-loss thread
print("Initializing stop-loss thread...")
thread_stop_loss = threading.Thread(target=stop_loss_thread, daemon=True)
thread_stop_loss.start()
print("Stop-loss thread started!")

# Start alert thread
print("Initializing alert thread...")
thread_alert = threading.Thread(target=alert_thread, daemon=True)
thread_alert.start()
print("Stop-loss thread started!")

if __name__ == '__main__':
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 5000))
    app.run(host=host, port=port, debug=True)
