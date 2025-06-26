import asyncio
import json
import redis
import websockets
from simulation import MarketSimulator
from pymongo import MongoClient
import os
from subscription import list_subscriptions

async def data_handler(websocket):
    # Load active subscriptions via subscription module
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    DB = os.getenv("DB_NAME", "market_data_db")
    SUBS_COLLECTION = os.getenv("SUBSCRIPTION_COLLECTION", "subscriptions")
    client = MongoClient(MONGO_URI)
    db = client[DB]
    sub_collection = db[SUBS_COLLECTION]

    # Connect to Redis at default localhost:6379, DB 0
    redis_client = redis.StrictRedis(
        host='127.0.0.1',
        port=6379,
        db=0,
        decode_responses=True  # so hget returns strings
    )

    # Retrieve subscribed instruments
    subs = list_subscriptions(sub_collection)

    # Initialize simulators for subscribed instruments
    sims = {
        s["instrument_id"]: MarketSimulator(
            s["instrument_id"], s["instrument_name"]
        ) for s in subs
    }

    try:
        while True:
            for instrument_id, sim in sims.items():
                # Generate a new tick for this instrument
                tick_data = sim.tick()

                # Send tick over WebSocket
                await websocket.send(json.dumps(tick_data))

                # Safely grab price and timestamp
                price = tick_data.get("price")
                timestamp = tick_data.get("timestamp")

                # Only set if at least one value exists
                if price is not None or timestamp is not None:
                    # Use a Redis hash keyed by instrument, e.g. "latest:<instrument_id>"
                    hash_key = f"latest:{instrument_id}"
                    mapping = {}
                    if price is not None:
                        # store as string
                        mapping["price"] = str(price)
                    if timestamp is not None:
                        mapping["timestamp"] = str(timestamp)
                    # HSET mapping will set only the provided fields
                    redis_client.hset(hash_key, mapping=mapping)

            # wait before next tick cycle
            await asyncio.sleep(1)
    except websockets.exceptions.ConnectionClosed:
        pass

async def main():
    host = os.getenv("WS_HOST", "0.0.0.0")
    port = int(os.getenv("WS_PORT", 8765))
    server = await websockets.serve(data_handler, host, port)
    print(f"WebSocket server running on ws://{host}:{port}")
    await server.wait_closed()

if __name__ == '__main__':
    asyncio.run(main())
