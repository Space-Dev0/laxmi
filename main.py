from logging import config
import sys
import os
import json
import logging
import threading
import time
import csv
import io
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Import mStock SDK
from tradingapi_a.mconnect import MConnect
from tradingapi_a.mticker import MTicker

# Import Strategy
from hma_strategy import HMAStrategy
from ema_strategy import EMAStrategy
from hma_price_strategy import HMAPriceStrategy

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot_execution.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# Global Variables
STRATEGIES = {} # Maps Token ID (int) -> Strategy Instance
M_TICKER = None
SESSION_FILE = "session_cache.json"

def load_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        log.critical(f"Config load error: {e}")
        sys.exit(1)

def save_session(access_token):
    """Saves the access token and current timestamp to a file."""
    try:
        data = {
            "access_token": access_token,
            "timestamp": datetime.now(tz=ZoneInfo("Asia/Kolkata")).isoformat()
        }
        with open(SESSION_FILE, 'w') as f:
            json.dump(data, f)
        log.info(f"Session saved to {SESSION_FILE}")
    except Exception as e:
        log.error(f"Failed to save session: {e}")

def load_session():
    """
    Loads session from file. 
    Returns access_token if file exists and is less than 12 hours old. 
    Otherwise returns None.
    """
    if not os.path.exists(SESSION_FILE):
        return None
    
    try:
        with open(SESSION_FILE, 'r') as f:
            data = json.load(f)
            
        saved_time = datetime.fromisoformat(data['timestamp'])
        age = datetime.now(tz=ZoneInfo("Asia/Kolkata")) - saved_time
        
        if age < timedelta(hours=12) and saved_time.date == datetime.now(tz=ZoneInfo("Asia/Kolkata")).date():
            log.info(f"Found valid session (Age: {age}).")
            return data['access_token']
        else:
            log.info(f"Session expired (Age: {age}).")
            return None
            
    except Exception as e:
        log.error(f"Failed to load session: {e}")
        return None

def _parse_csv_for_symbols(file_path, symbols_set, exchange):
    """Helper to parse CSV file line-by-line and find symbols."""
    token_map = {}
    remaining_symbols = symbols_set.copy()
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if not row or len(row) < 3: continue
                
                # Assuming: Token=Column 0, Symbol=Column 2, Exchange=Last Column
                curr_token = row[0]
                curr_symbol = row[2]
                curr_exch = row[-1]
                
                if curr_symbol in remaining_symbols and curr_exch == exchange:
                    token_map[curr_symbol] = {
                        'token': curr_token,
                        'exchange': curr_exch
                    }
                    remaining_symbols.remove(curr_symbol)
                    log.info(f"Mapped {curr_symbol} -> {curr_token}")
                
                if not remaining_symbols:
                    break
    except Exception as e:
        log.error(f"Error parsing local CSV: {e}")
        return {}, symbols_set
        
    return token_map, remaining_symbols

def get_token_map(mconnect, symbols, exchange):
    """
    Tries to map symbols using local cache first, then fails over to API.
    """
    file_name = "instrument_master.csv"
    symbols_set = set(symbols)
    token_map = {}
    
    # 1. Try Local File
    if os.path.exists(file_name):
        log.info("Checking local instrument master...")
        found_map, missing_symbols = _parse_csv_for_symbols(file_name, symbols_set, exchange)
        token_map.update(found_map)
        
        if not missing_symbols:
            log.info("All symbols found in local cache.")
            return token_map
        else:
            log.warning(f"Symbols not found locally: {missing_symbols}. Refreshing Master...")
    
    # 2. Download and Parse (if needed)
    log.info("Downloading Instrument Master...")
    try:
        # Stream download to file to avoid memory spike
        csv_response = mconnect.get_instruments()
        # mConnect likely returns bytes. If it's a huge request, we ideally want a stream=True request 
        # but the SDK might returns all-at-once. We write it to disk immediately.
        
        # If the SDK returns a pure bytes object properly, we just write it.
        with open(file_name, 'wb') as csvfile:
            csvfile.write(csv_response)
            
        # 3. Parse again
        found_map, missing_symbols = _parse_csv_for_symbols(file_name, symbols_set, exchange)
        token_map.update(found_map)
        
        return token_map
        
    except Exception as e:
        log.critical(f"Failed to fetch/parse Instrument Master: {e}")
        sys.exit(1)

# --- WebSocket Callbacks ---

def on_ticks(ws, ticks):
    """
    Callback when tick data is received.
    ticks is likely a list of dictionaries: [{'token': 123, 'lp': '100'}, ...]
    """

    # if ticks:
        # Example log: "Tick: {'lp': '120.5', 'Token': 14366, ...}"
        # log.info(f"Tick Received: {ticks}") 

    for tick in ticks:
        token = tick.get('instrument_token') # Note: SDK might capitalize keys, check SDK docs/logs
        
        if token and token in STRATEGIES:
            # Dispatch to appropriate strategy
            STRATEGIES[token].process_tick(tick)

def on_connect(ws, response):
    log.info("WebSocket Connected.")
    ws.send_login_after_connect()
    
    # Subscribe to all strategy tokens
    tokens_to_sub = list(STRATEGIES.keys())
    if tokens_to_sub:
        log.info(f"Subscribing to tokens: {tokens_to_sub}")
        ws.subscribe(tokens_to_sub)
        # Set Mode to Full/Quote to get OHLC data for candle logic if needed
        # Or LTP if only close price needed. Using LTP mode for speed.
        try:
            ws.set_mode(ws.MODE_LTP, tokens_to_sub)
        except:
            # Fallback if constant missing
            log.info("Using fallback mode 'ltp'")
            ws.set_mode('ltp', tokens_to_sub)

def on_close(ws, code, reason):
    log.error(f"WebSocket Closed: {code} - {reason}")

def on_error(ws, error):
    log.error(f"WebSocket Error: {error}")

def on_reconnect(ws, attempts_count):
    log.warning(f"WebSocket Reconnecting... Attempt: {attempts_count}")

def on_noreconnect(ws):
    log.critical("WebSocket Max Reconnection Attempts Reached. Exiting.")
    sys.exit(1)

def start_websocket_thread(api_key, access_token):
    global M_TICKER
    # Defined in sdk config usually, or hardcoded based on doc
    WS_URL = "wss://ws.mstock.trade" 
    
    # Run blocking connect in this thread
    # Enabling reconnection with generous limits
    # reconnect_max_tries=300 (approx 300 * 60s max delay could cover hours, but delay ramps up)
    # reconnect_max_delay=60 (max wait between retries is 60s)
    M_TICKER = MTicker(api_key, access_token, WS_URL, 
                       reconnect=True, 
                       reconnect_max_tries=300, 
                       reconnect_max_delay=60)
    
    M_TICKER.on_ticks = on_ticks
    M_TICKER.on_connect = on_connect
    M_TICKER.on_close = on_close
    M_TICKER.on_error = on_error
    M_TICKER.on_reconnect = on_reconnect
    M_TICKER.on_noreconnect = on_noreconnect
    
    # Run blocking connect in this thread
    M_TICKER.connect()


def strategy_monitor_loop():
    """Background thread loop to check EOD logic"""
    log.info("Strategy Monitor background thread started.")
    while True:
        try:
            for strat in STRATEGIES.values():
                strat.check_eod()
            time.sleep(10)
        except Exception as e:
            log.error(f"Error in monitor loop: {e}")
            time.sleep(10)


def wait_for_start_time(start_time_str):
    """
    Waits until the given start_time (HH:MM) if it's in the future (today).
    """
    if not start_time_str:
        return

    try:
        now = datetime.now(tz=ZoneInfo('Asia/Kolkata'))
        t = datetime.strptime(start_time_str, "%H:%M").time()
        target = datetime.combine(now.date(), t,tzinfo=ZoneInfo('Asia/Kolkata'))

        if now < target:
            sleep_seconds = (target - now).total_seconds()
            log.info(f"Current time {now.strftime('%H:%M')}. Waiting until {start_time_str} ({int(sleep_seconds)}s)...")
            time.sleep(sleep_seconds)
            log.info("Start time reached. Resuming...")
        else:
            log.info(f"Current time is past start time {start_time_str}. Proceeding immediately.")

    except ValueError as e:
        log.error(f"Invalid start_time format '{start_time_str}': {e}. Expected HH:MM.")
    except Exception as e:
        log.error(f"Error in wait_for_start_time: {e}")


# --- Main ---

def main():
    log.info("--- mStock HMA Bot Starting ---")
    
    print("\nSelect Strategy:")
    print("1. HMA Crossover (Heikin Ashi)")
    print("2. EMA Crossover (Heikin Ashi)")
    print("3. HMA Price Crossover (Heikin Ashi)")
    choice = input("Enter choice (1,2 or 3): ").strip()
    
    StrategyClass = None
    if choice == '1':
        StrategyClass = HMAStrategy
        log.info("Selected: HMA Strategy")
    elif choice == '2':
        StrategyClass = EMAStrategy
        log.info("Selected: EMA Strategy")
    elif choice == '3':
        StrategyClass = HMAPriceStrategy
        log.info("Selected: HMA Price Strategy")
    else:
        log.critical("Invalid choice. Exiting.")
        sys.exit(1)

    # 1. Load Config
    config = load_config()
    api_set = config['api_settings']
    strat_set = config['strategy_settings']
    
    # 2. Login
    mconnect = MConnect()
    log.info("Logging in...")
    login_resp = mconnect.login(api_set['username'], api_set['password'])
    
    # Check login status (Assuming 'status' key in json)
    if login_resp.json().get('status') != 'success':
        log.critical(f"Login failed: {login_resp.json()}")
        sys.exit(1)
        
    # 3. Session Generation (OTP/TOTP) OR Cache Restore
    cached_token = load_session()
    access_token = None
    
    if cached_token:
        # Reuse Session
        log.info("Restoring session from cache...")
        mconnect.set_access_token(cached_token)
        mconnect.set_api_key(api_set['api_key'])
        access_token = cached_token
        
    else:
        # New Session via OTP
        otp = input("Enter OTP sent to mobile: ")
        session_resp = mconnect.generate_session(api_set['api_key'], otp, "W")
        
        if session_resp.json().get('status') != 'success':
            log.critical(f"Session Generation failed: {session_resp.json()}")
            sys.exit(1)

        access_token = session_resp.json().get('data', {}).get('access_token')
        
        # Save Session
        save_session(access_token)
    
    log.info("Session Established.")
    
    # 4. Map Tokens
    token_map = get_token_map(mconnect, strat_set['symbols'], strat_set['exchange'])
    
    if len(token_map) != len(strat_set['symbols']):
        log.warning("Some symbols could not be mapped. Check spelling.")
    
    # 5. Initialize Strategies    
    for symbol in strat_set['symbols']:
        if symbol in token_map:
            try:
                # Create a copy of settings and inject the specific 'symbol'
                instance_settings = strat_set.copy()
                instance_settings['symbol'] = symbol
                
                # Now pass instance_settings which contains the 'symbol' key
                strategy = StrategyClass(mconnect, token_map, **instance_settings)
                
                # Websocket uses Token as Int for routing
                t_id = int(token_map[symbol]['token'])
                STRATEGIES[t_id] = strategy
                
                log.info(f"Strategy initialized for {symbol} (Token: {t_id})")
            except Exception as e:
                log.error(f"Failed to init strategy for {symbol}: {e}", exc_info=True)
    if not STRATEGIES:
        log.critical("No strategies running. Exiting.")
        sys.exit(1)

    # 5.5 Wait for Start Time
    if 'start_time' in strat_set:
        wait_for_start_time(strat_set['start_time'])


    # 6. Start Strategy Monitor in Background Thread
    # We move the loop here so the main thread is free for the WebSocket
    monitor_thread = threading.Thread(target=strategy_monitor_loop, daemon=True)
    monitor_thread.start()

    # 7. Start WebSocket in Main Thread (Blocking)
    # This must run on the main thread for Twisted/Signals to work
    log.info("Starting WebSocket in Main Thread...")
    try:
        start_websocket_thread(api_set['api_key'], access_token)
    except KeyboardInterrupt:
        log.info("Shutdown Signal received.")
        if M_TICKER:
            M_TICKER.close()
        sys.exit(0)

if __name__ == "__main__":
    main()