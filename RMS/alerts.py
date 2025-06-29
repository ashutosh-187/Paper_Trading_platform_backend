import json
from datetime import datetime
from OMS.manage_order import fetch_all_data_strip_prefix

def check_trade_losses(
    redis_client,
    trade_book_coll,        # now a pymongo Collection
    threshold_pct=1.0,
    alerted_ids=None,       # set to track already alerted trades
    log_file_path='loss_alerts.json'
):
    """
    Scans all documents in the `trade_book_coll` for unrealized losses
    above `threshold_pct`. Alerts once per trade_id.
    """
    if alerted_ids is None:
        alerted_ids = set()

    # 1. Fetch market data
    market_data = fetch_all_data_strip_prefix(redis_client)
    now_str = datetime.utcnow().isoformat()

    # 2. Pull all open trades from MongoDB
    #    (you can filter here—for example only trades not yet closed)
    cursor = trade_book_coll.find({}, {
        "order_placement_time": 0,
        "order_execution_time": 0
    })
    for trade in cursor:
        # normalize trade_id
        raw_id = trade.get("_id")
        if isinstance(raw_id, dict) and "$oid" in raw_id:
            trade_id = raw_id["$oid"]
        else:
            trade_id = str(raw_id)

        if trade_id in alerted_ids:
            continue

        instr = trade.get("instrument_id")
        if instr not in market_data:
            continue

        # parse prices
        try:
            order_price = float(trade["order_price"])
            current_price = float(market_data[instr]["price"])
        except (KeyError, TypeError, ValueError):
            continue

        side = trade.get("order_side", "").lower()
        if side == "buy":
            loss_pct = (order_price - current_price) / order_price * 100
        elif side == "sell":
            loss_pct = (current_price - order_price) / order_price * 100
        else:
            continue

        if loss_pct > threshold_pct:
            alert = {
                "trade_id":         trade_id,
                "instrument_id":    instr,
                "order_side":       side,
                "order_price":      order_price,
                "current_price":    current_price,
                "loss_pct":         round(loss_pct, 2),
                "order_placed_time": trade.get("order_placed_time"),
                "market_timestamp": market_data[instr].get("timestamp"),
                "alert_time_utc":    now_str
            }
            # print(json.dumps({"type": "LOSS_ALERT", **alert}))
            try:
                with open(log_file_path, "a") as f:
                    f.write(json.dumps({"type": "LOSS_ALERT", **alert}) + "\n")
            except Exception as e:
                print(f"Failed to write alert: {e}")

            alerted_ids.add(str(trade_id))
    return list(alerted_ids)
