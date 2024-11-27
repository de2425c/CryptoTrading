import asyncio
import json
import os
from datetime import datetime
import pytz
from websockets import connect
from termcolor import cprint

# Base WebSocket URLs
websocket_url_base = 'wss://fstream.binance.com/ws/'
trades_url = 'wss://fstream.binance.com/ws/'
liquidation_url = 'wss://fstream.binance.com/ws/!forceOrder@arr'
funding_url = 'wss://fstream.binance.com/ws/'

# File paths for CSV storage
recent_trades_file = 'recent_trades.csv'
funding_rates_file = 'funding_rates.csv'
liquidation_events_file = 'liquidation_events.csv'

# Check and create files if they don't exist
for filename, headers in [
    (recent_trades_file, 'Event Time, Symbol, Trade ID, Price, Quantity, Trade Time, Is Buyer Maker\n'),
    (funding_rates_file, 'Timestamp, Symbol, Funding Rate, Yearly Rate\n'),
    (liquidation_events_file, 'Symbol, Side, Quantity, Price, USD Size, Event Time\n')
]:
    if not os.path.isfile(filename):
        with open(filename, 'w') as f:
            f.write(headers)

# Global aggregator for trades
class TradeAggregator:
    def __init__(self):
        self.trade_buckets = {}

    async def add_trade(self, symbol, second, usd_size, is_buyer_maker):
        trade_key = (symbol, second, is_buyer_maker)
        self.trade_buckets[trade_key] = self.trade_buckets.get(trade_key, 0) + usd_size

    async def check_and_print_trades(self):
        timestamp_now = datetime.utcnow().strftime("%H:%M:%S")
        deletions = []
        for trade_key, usd_size in self.trade_buckets.items():
            symbol, second, is_buyer_maker = trade_key
            if second < timestamp_now and usd_size > 500000:
                attrs = ['bold']
                back_color = 'on_blue' if not is_buyer_maker else 'on_magenta'
                trade_type = "BUY" if not is_buyer_maker else 'SELL'
                usd_size /= 1_000_000  # Convert to millions
                cprint(f"{trade_type} {symbol} {second} ${usd_size:.2f}m", 'white', back_color, attrs=attrs)
                deletions.append(trade_key)

        for key in deletions:
            del self.trade_buckets[key]

trade_aggregator = TradeAggregator()

# Function to track recent trades
async def track_trades(uri, filename, aggregator):
    async with connect(uri) as websocket:
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                usd_size = float(data['p']) * float(data['q'])
                trade_time = datetime.fromtimestamp(data['T'] / 1000, pytz.utc)
                readable_trade_time = trade_time.strftime('%Y-%m-%d %H:%M:%S')

                await aggregator.add_trade(data['s'], readable_trade_time, usd_size, data['m'])

                # Write trade to CSV
                with open(filename, 'a') as f:
                    f.write(f"{data['E']}, {data['s']}, {data['a']}, {data['p']}, {data['q']}, {readable_trade_time}, {data['m']}\n")

            except Exception as e:
                await asyncio.sleep(5)

# Function to track funding rates
async def track_funding_rates(symbol, filename):
    uri = f"{funding_url}{symbol}@markPrice"
    async with connect(uri) as websocket:
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                timestamp = datetime.utcfromtimestamp(data['E'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                funding_rate = float(data['r'])
                yearly_rate = (funding_rate * 3 * 365) * 100

                with open(filename, 'a') as f:
                    f.write(f"{timestamp}, {data['s']}, {funding_rate}, {yearly_rate}\n")

            except Exception as e:
                await asyncio.sleep(5)

# Function to track liquidation events
async def track_liquidations(uri, filename):
    async with connect(uri) as websocket:
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)['o']
                symbol = data['s']
                side = data['S']
                quantity = float(data['z'])
                price = float(data['p'])
                usd_size = quantity * price
                timestamp = datetime.utcfromtimestamp(data['T'] / 1000).strftime('%Y-%m-%d %H:%M:%S')

                if usd_size > 3000:
                    with open(filename, 'a') as f:
                        f.write(f"{symbol}, {side}, {quantity}, {price}, {usd_size}, {timestamp}\n")

            except Exception as e:
                await asyncio.sleep(5)

# Main function to run all tasks
async def main():
    symbols = ['btcusdt', 'solusdt',]
    
    # Tasks for recent trades
    trade_tasks = [track_trades(f"{trades_url}{symbol}@aggTrade", recent_trades_file, trade_aggregator) for symbol in symbols]
    
    # Tasks for funding rates
    funding_tasks = [track_funding_rates(symbol, funding_rates_file) for symbol in symbols]
    
    # Task for liquidation events
    liquidation_task = track_liquidations(liquidation_url, liquidation_events_file)
    
    # Aggregated trade print task
    print_task = asyncio.create_task(trade_aggregator.check_and_print_trades())
    
    await asyncio.gather(*trade_tasks, *funding_tasks, liquidation_task, print_task)

# Run the unified script
asyncio.run(main())
