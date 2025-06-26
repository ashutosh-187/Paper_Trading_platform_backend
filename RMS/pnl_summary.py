import pandas as pd
import numpy as np
from pymongo.collection import Collection
from typing import Dict, Any, List, Tuple

from OMS.manage_order import fetch_all_data_strip_prefix

def calculate_mtm_pnl(trade_book_collection, redis_client):
    """
    Computes realized, unrealized, and MTM PnL per instrument_id and overall summary.
    Handles FIFO matching for longs and shorts, grouping strictly by instrument_id.
    """
    # 1. Fetch all trades
    trades = list(trade_book_collection.find())
    
    # 2. Fetch live prices
    live_data = fetch_all_data_strip_prefix(redis_client)
    
    # 3. Group trades by instrument_id
    trades_by_instrument: Dict[str, List[dict]] = {}
    for trade in trades:
        inst = trade.get('instrument_id')
        if inst is None:
            continue
        trades_by_instrument.setdefault(inst, []).append(trade)
    
    results: Dict[str, Any] = {}
    total_realized = 0.0
    total_unrealized = 0.0
    
    for instrument_id, instrument_trades in trades_by_instrument.items():
        # Sort by order_placed_time for FIFO
        instrument_trades.sort(key=lambda x: x['order_placed_time'])
        
        # Get live price if available
        live_entry = live_data.get(instrument_id)
        if not live_entry:
            # If no live price, we can skip unrealized but still compute realized from closed matches
            live_price = None
        else:
            try:
                live_price = float(live_entry["price"])
            except:
                live_price = None
        
        # FIFO queues: store (entry_price, quantity)
        long_queue: List[Tuple[float, float]] = []
        short_queue: List[Tuple[float, float]] = []
        realized_pnl = 0.0
        
        for trade in instrument_trades:
            side = trade.get('order_side')
            price = float(trade.get('order_price', 0.0))
            qty = float(trade.get('quantity', 1.0))  # use 'quantity' field if present, else 1
            
            if side.lower() == "buy":
                # First match against any open shorts
                remaining_qty = qty
                while remaining_qty > 0 and short_queue:
                    short_price, short_qty = short_queue[0]
                    match_qty = min(remaining_qty, short_qty)
                    # For a short, we sold at short_price earlier; now we buy at price to close
                    # Realized PnL for this matched qty = (short_price - buy_price) * qty
                    realized_pnl += (short_price - price) * match_qty
                    remaining_qty -= match_qty
                    if match_qty == short_qty:
                        short_queue.pop(0)
                    else:
                        # reduce front of queue
                        short_queue[0] = (short_price, short_qty - match_qty)
                # Any leftover becomes a long position
                if remaining_qty > 0:
                    long_queue.append((price, remaining_qty))
            
            elif side.lower() == "sell":
                # First match against any open longs
                remaining_qty = qty
                while remaining_qty > 0 and long_queue:
                    long_price, long_qty = long_queue[0]
                    match_qty = min(remaining_qty, long_qty)
                    # Realized PnL = (sell_price - long_price) * qty
                    realized_pnl += (price - long_price) * match_qty
                    remaining_qty -= match_qty
                    if match_qty == long_qty:
                        long_queue.pop(0)
                    else:
                        long_queue[0] = (long_price, long_qty - match_qty)
                # Any leftover becomes a short position
                if remaining_qty > 0:
                    short_queue.append((price, remaining_qty))
            
            else:
                # Unknown side; skip or log
                continue
        
        # Compute unrealized PnL if live_price is known
        unrealized_pnl = 0.0
        if live_price is not None:
            # longs: (live_price - entry_price) * qty
            for entry_price, qty in long_queue:
                unrealized_pnl += (live_price - entry_price) * qty
            # shorts: (entry_price - live_price) * qty
            for entry_price, qty in short_queue:
                unrealized_pnl += (entry_price - live_price) * qty
        else:
            unrealized_pnl = None  # or keep as 0.0 but indicate unknown
        
        mtm = realized_pnl + (unrealized_pnl if unrealized_pnl is not None else 0.0)
        
        results[instrument_id] = {
            "realized_pnl": round(realized_pnl, 2),
            "unrealized_pnl": None if unrealized_pnl is None else round(unrealized_pnl, 2),
            "mtm_pnl": None if unrealized_pnl is None else round(mtm, 2),
            "live_price": live_price,
            # For debugging/inspection: list remaining open positions
            "open_long_positions": [{"entry_price": ep, "qty": q} for ep, q in long_queue],
            "open_short_positions": [{"entry_price": ep, "qty": q} for ep, q in short_queue],
        }
        
        total_realized += realized_pnl
        if unrealized_pnl is not None:
            total_unrealized += unrealized_pnl
    
    # Optionally include an overall summary
    overall = {
        "total_realized_pnl": round(total_realized, 2),
        "total_unrealized_pnl": round(total_unrealized, 2) if trades_by_instrument else 0.0,
        "overall_mtm_pnl": round(total_realized + total_unrealized, 2) if trades_by_instrument else 0.0
    }
    results["_overall"] = overall
    return results


if __name__ == "__main__":
    from pymongo import MongoClient
    import redis
    import os
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
    calculate_mtm_pnl(trade_book_collection, redis_client)
