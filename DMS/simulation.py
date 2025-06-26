import random
from datetime import datetime
import numpy as np

class NIFTYSymbolGenerator:
    """Generates realistic NIFTY option symbol names and prices."""
    def __init__(self):
        self.expiry_dates = ["26JUN2025", "31JUL2025", "28AUG2025", "25SEP2025"]
        self.base_strikes = list(range(20000, 28000, 50))
        self.option_types = ["CE", "PE"]

    def generate(self, symbol_id):
        expiry = random.choice(self.expiry_dates)
        strike = random.choice(self.base_strikes)
        otype = random.choice(self.option_types)
        name = f"{symbol_id} {expiry} {strike} {otype}"
        return name

class MarketSimulator:
    """Simulates price/volume ticks with Brownian motion."""
    def __init__(self, symbol_id, name):
        self.id = symbol_id
        self.name = name
        self.price = random.uniform(10, 200)
        self.rng = np.random.default_rng()

    def tick(self):
        z = self.rng.normal()
        self.price += self.price * 0.01 * z
        if self.price < 0.05:
            self.price = 0.05
        vol = int(self.rng.integers(10, 5000))
        return {
            "instrument_id": self.id,
            "instrument_name": self.name,
            "price": round(self.price, 2),
            "volume": vol,
            "timestamp": datetime.now().isoformat()
        }
