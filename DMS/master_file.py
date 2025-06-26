# master_file.py
from typing import List
from fastapi import HTTPException
from pydantic import BaseModel

# Static configuration for instrument generation
date_str = "03 June 2025"
base_strike = 23000

class Instrument(BaseModel):
    instrument_id: str
    instrument_name: str

async def create_master_file(collection) -> dict:
    """
    Generate 500 NIFTY instruments (250 ITM, 250 OTM) and save to provided MongoDB collection.
    Returns dict with inserted_count.
    """
    # Remove existing records
    collection.delete_many({})

    instruments: List[Instrument] = []

    # ITM strikes (below base)
    for i in range(1, 251):
        strike = base_strike - 4 * i
        inst_id = f"1_{i}"
        name = f"NIFTY {date_str} {strike}"
        instruments.append(Instrument(instrument_id=inst_id, instrument_name=name))

    # OTM strikes (above base)
    for i in range(1, 251):
        strike = base_strike + 4 * i
        inst_id = f"1_{250 + i}"
        name = f"NIFTY {date_str} {strike}"
        instruments.append(Instrument(instrument_id=inst_id, instrument_name=name))

    try:
        docs = [inst.dict() for inst in instruments]
        result = collection.insert_many(docs)
        return {"inserted_count": len(result.inserted_ids)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def get_master_file(collection) -> List[Instrument]:
    """
    Fetch all instruments from provided MongoDB collection and return as list of Instrument.
    """
    try:
        docs = list(collection.find({}, {"_id": 0}))
        return [Instrument(**doc) for doc in docs]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))