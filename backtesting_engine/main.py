import json
from SMA_algo import SmaCross
import pandas as pd

def run_strategy():
    """
    This is the main function to run the SMA Crossover backtest.
    """
    print("--- Starting SMA Crossover Backtest ---")
    
    # --- Configuration ---
    # Define the path to your historical data file.
    # I have provided a sample 'NIFTY_50_data.csv' file.
    file_path = 'NIFTY_50_data.csv'
    
    # Define strategy parameters. 
    # These have been adjusted to be smaller than the number of rows in your sample data file.
    short_window = 3
    long_window = 7
    initial_capital = 100000.0
    trade_size = 10 # Number of shares per trade
    
    # --- Strategy Initialization ---
    # Create an instance of the SmaCross strategy from your SMA_algo.py file.
    strategy = SmaCross(
        file_path=file_path,
        short_window=short_window,
        long_window=long_window,
        capital=initial_capital,
        trade_size=trade_size
    )
    
    # --- Run Backtest ---
    # The run_backtest() method now generates the list of trades.
    trade_book = strategy.run_backtest()
    
    if not trade_book:
        print("No trades were generated. The backtest is complete.")
        return

    # --- Output Results ---
    print(f"\n--- Backtest Complete: {len(trade_book)} trades generated ---")
    
    # Convert the trade book to a pandas DataFrame for easy viewing.
    trade_df = pd.DataFrame(trade_book)
    print("\nRecent Trades:")
    print(trade_df.tail()) # Print the last 5 trades

    # Save the full trade book to a JSON file.
    # This file can then be used for further analysis, like your latency script.
    output_file = "trade_book.json"
    with open(output_file, 'w') as f:
        json.dump(trade_book, f, indent=4)
        
    print(f"\nFull trade book saved to '{output_file}'")

if __name__ == "__main__":
    run_strategy()
