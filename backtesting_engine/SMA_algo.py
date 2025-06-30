import pandas as pd
import numpy as np
from datetime import datetime

class SmaCross:
    """
    A class to implement a Simple Moving Average (SMA) Crossover trading strategy
    and run a backtest to generate a history of trades.
    """
    def __init__(self, file_path, short_window=50, long_window=200, capital=100000, trade_size=1):
        """
        Initializes the SmaCross strategy.
        """
        self.file_path = file_path
        self.short_window = short_window
        self.long_window = long_window
        self.capital = capital
        self.trade_size = trade_size
        self.data = self._load_data()
        self.signals = self._generate_signals()

    def _load_data(self):
        """
        Loads the historical data from the CSV file and calculates SMAs.
        This version is robust to handle different date formats and column names with spaces.
        """
        try:
            # First, load the data from the CSV
            data = pd.read_csv(self.file_path)
            
            # Clean up column names by stripping any leading/trailing whitespace
            data.columns = data.columns.str.strip()
            
            # --- Robust Date Parsing ---
            # Explicitly convert the 'Date' column to datetime objects.
            # This allows pandas to automatically figure out the format (like 'DD-MMM-YYYY').
            data['Date'] = pd.to_datetime(data['Date'])
            
            # Rename the 'Turnover' column if it exists, ignoring errors if not
            data.rename(columns={'Turnover (₹ Cr)': 'Turnover'}, inplace=True, errors='ignore')
            
            # Calculate Short and Long SMAs
            data['short_mavg'] = data['Close'].rolling(window=self.short_window, min_periods=1).mean()
            data['long_mavg'] = data['Close'].rolling(window=self.long_window, min_periods=1).mean()
            
            return data
        except FileNotFoundError:
            print(f"Error: The file at {self.file_path} was not found.")
            return pd.DataFrame()
        except Exception as e:
            print(f"An error occurred while loading data: {e}")
            return pd.DataFrame()

    def _generate_signals(self):
        """
        Generates trading state signals based on the SMA crossover.
        -  1.0: Desired state is a long position (short SMA > long SMA).
        - -1.0: Desired state is a flat/short position (short SMA < long SMA).
        -  0.0: No signal (during the initial period).
        """
        if self.data.empty:
            return pd.DataFrame()

        signals = pd.DataFrame(index=self.data.index)
        signals['signal'] = 0.0
        
        # Generate signals only after the long_window period to ensure SMA is stable.
        signals.loc[self.long_window:, 'signal'] = np.where(
            self.data['short_mavg'][self.long_window:] > self.data['long_mavg'][self.long_window:], 
            1.0, 
            -1.0
        )
        return signals

    def run_backtest(self):
        """
        Simulates trading based on the generated signals to create a trade book.
        A trade is executed only when the signal state changes.
        """
        if self.signals.empty:
            print("No signals were generated. Cannot run backtest.")
            return []

        trade_book = []
        position = 0  # 0 represents a flat position, 1 represents a long position

        for i in range(len(self.signals)):
            signal = self.signals['signal'].iloc[i]
            
            # Skip the initial period where signals are 0.0
            if signal == 0.0:
                continue

            # --- BUY LOGIC ---
            # If we don't have a position and the signal is to go long, then BUY.
            if position == 0 and signal == 1.0:
                position = 1  # Update position to long
                trade_date = self.data['Date'].iloc[i]
                trade_price = self.data['Close'].iloc[i] # Assume trade at closing price
                
                trade = {
                    "date": trade_date.strftime('%Y-%m-%d'),
                    "signal": "BUY",
                    "order_placement_time": f"{trade_date.isoformat().split('T')[0]}T09:15:00",
                    "order_price": trade_price,
                    "order_execution_price": trade_price, # Simplified assumption for backtest
                    "order_execution_time": f"{trade_date.isoformat().split('T')[0]}T09:15:01",
                    "status": "FILLED",
                    "slippage": 0.0, # Not modeled in this simple backtest
                    "reason": "SMA Crossover"
                }
                trade_book.append(trade)

            # --- SELL LOGIC ---
            # If we have a long position and the signal is to go flat, then SELL.
            elif position == 1 and signal == -1.0:
                position = 0  # Update position to flat
                trade_date = self.data['Date'].iloc[i]
                trade_price = self.data['Close'].iloc[i]

                trade = {
                    "date": trade_date.strftime('%Y-%m-%d'),
                    "signal": "SELL",
                    "order_placement_time": f"{trade_date.isoformat().split('T')[0]}T09:15:00",
                    "order_price": trade_price,
                    "order_execution_price": trade_price,
                    "order_execution_time": f"{trade_date.isoformat().split('T')[0]}T09:15:01",
                    "status": "FILLED",
                    "slippage": 0.0,
                    "reason": "SMA Crossover"
                }
                trade_book.append(trade)
                
        return trade_book

    def get_signals(self):
        """Returns the generated trading signals."""
        return self.signals

    def get_data(self):
        """Returns the OHLC data with calculated SMAs."""
        return self.data
