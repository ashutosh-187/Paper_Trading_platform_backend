import json
import pandas as pd

def calculate_performance(trade_book_path, historical_data_path, initial_capital, trade_size):
    """
    Analyzes a trade book to calculate PnL and drawdown.

    Args:
        trade_book_path (str): Path to the trade book JSON file.
        historical_data_path (str): Path to the OHLC CSV data file.
        initial_capital (float): The starting capital for the backtest.
        trade_size (int): The number of shares traded in each transaction.
    """
    try:
        # Load the generated trade book
        with open(trade_book_path, 'r') as f:
            trade_book = json.load(f)
        
        # Load historical data to get the last known price for unrealized PnL
        ohlc_data = pd.read_csv(historical_data_path)
        
        # --- FIX ---
        # Clean up column names by stripping any leading/trailing whitespace
        # This prevents a KeyError if the CSV columns are like "Date " instead of "Date".
        ohlc_data.columns = ohlc_data.columns.str.strip()
        
        # Now, safely access and convert the 'Date' column
        ohlc_data['Date'] = pd.to_datetime(ohlc_data['Date'], format='%d-%b-%Y')

    except FileNotFoundError as e:
        print(f"Error loading file: {e}. Cannot perform analysis.")
        return
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error parsing data: {e}. Please check file formats.")
        return

    # --- Initialization ---
    capital = initial_capital
    position = 0  # Current number of shares held
    realized_pnl = 0.0
    portfolio_history = [{'date': 'start', 'value': initial_capital}]
    last_buy_price = 0.0

    # --- Process Each Trade ---
    for trade in trade_book:
        price = trade['order_execution_price']
        
        if trade['signal'] == 'BUY':
            # Execute buy only if we are currently flat (no position)
            if position == 0:
                position = trade_size
                capital -= price * trade_size
                last_buy_price = price
        
        elif trade['signal'] == 'SELL':
            # Execute sell only if we currently hold a position
            if position > 0:
                capital += price * trade_size
                # Calculate profit/loss for this round trip (sell price - buy price)
                pnl_for_trade = (price - last_buy_price) * trade_size
                realized_pnl += pnl_for_trade
                position = 0  # Position becomes flat after selling
        
        # Record portfolio value after the trade
        current_portfolio_value = capital + (position * price)
        portfolio_history.append({'date': trade['date'], 'value': current_portfolio_value})

    # --- Calculate Unrealized PnL (if a position is still open) ---
    unrealized_pnl = 0.0
    # Ensure there is data before trying to access the last row
    if not ohlc_data.empty:
        last_close_price = ohlc_data['Close'].iloc[-1]
        if position > 0:
            # Profit/loss based on the last known market price
            unrealized_pnl = (last_close_price - last_buy_price) * position
        final_portfolio_value = capital + (position * last_close_price)
    else:
        last_close_price = 0
        final_portfolio_value = capital
    
    # --- Calculate Max Drawdown ---
    portfolio_df = pd.DataFrame(portfolio_history)
    portfolio_df['peak'] = portfolio_df['value'].cummax()
    # Avoid division by zero if peak is 0
    portfolio_df['drawdown_pct'] = portfolio_df.apply(
        lambda row: (row['value'] - row['peak']) / row['peak'] if row['peak'] != 0 else 0, 
        axis=1
    )
    max_drawdown = portfolio_df['drawdown_pct'].min() * 100 if not portfolio_df.empty else 0

    # --- Print Performance Summary ---
    print("\n--- Performance Analysis ---")
    print(f"Initial Capital:         ${initial_capital:,.2f}")
    print(f"Final Portfolio Value:   ${final_portfolio_value:,.2f}")
    print("-" * 30)
    print(f"Realized PnL:            ${realized_pnl:,.2f}")
    print(f"Unrealized PnL:          ${unrealized_pnl:,.2f} (based on last close of ${last_close_price:,.2f})")
    print(f"Total PnL:               ${(realized_pnl + unrealized_pnl):,.2f}")
    print(f"Max Drawdown:            {max_drawdown:.2f}%")
    print("--- End of Analysis ---\n")

if __name__ == '__main__':
    # --- Configuration for Standalone Execution ---
    # This allows you to run this script directly on an existing trade_book.json
    TRADE_BOOK_FILE = 'trade_book.json'
    DATA_FILE = 'NIFTY_50_data.csv'
    INITIAL_CAPITAL = 100000.0
    TRADE_SIZE = 10  # Ensure this matches the setting in run_strategy.py

    calculate_performance(TRADE_BOOK_FILE, DATA_FILE, INITIAL_CAPITAL, TRADE_SIZE)
