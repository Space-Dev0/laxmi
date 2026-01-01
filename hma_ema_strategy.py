import pandas as pd
import numpy as np
import datetime
import logging
import time
from zoneinfo import ZoneInfo
# Configure Logger
log = logging.getLogger(__name__)

class HMAEMAStrategy:
    def __init__(self, mconnect_obj, token_map, **settings):
        # API and Config
        self.api = mconnect_obj
        self.symbol = settings['symbol']
        self.exchange = settings['exchange']
        self.interval = settings['interval_minute']
        self.product_type = settings['product_type']
        self.order_type = settings['order_type']
        self.quantity = str(settings['quantity']) # mStock expects string for quantity
        
        # Token Info
        if self.symbol not in token_map:
            raise ValueError(f"Token not found for symbol {self.symbol}")
        
        self.token_info = token_map[self.symbol]
        self.token_id = self.token_info['token'] 
        self.int_token = int(self.token_id)      
        
        # Strategy Parameters
        # User Configuration: "hma_period" and "ema_period"
        # We need to determine which is Fast (Short) and which is Slow (Long)
        self.hma_period = int(settings.get('hma_period', 0))
        self.ema_period = int(settings.get('ema_period', 0))
        
        if self.hma_period == 0 or self.ema_period == 0:
            raise ValueError("hma_period and ema_period must be > 0")

        # Dynamic Logic:
        # Fast MA = min(hma, ema)
        # Slow MA = max(hma, ema)
        self.is_hma_faster = self.hma_period < self.ema_period
        
        self.sl_pct = settings['sl_percentage']
        self.tp_pct = settings['tp_percentage']
        self.square_off_time = settings['square_off_time']
        self.required_confirmations = settings.get('confirmation_candles', 0)
        
        # State
        self.position = "FLAT" # FLAT, LONG, SHORT
        self.data = pd.DataFrame()
        self.current_candle = None
        
        # Confirmation Logic State
        self.pending_signal = None
        self.confirmation_count = 0

        # Initial Setup
        log.info(f"[{self.symbol}] Initializing HMA-EMA Strategy (HMA:{self.hma_period}, EMA:{self.ema_period})")
        log.info(f"[{self.symbol}] fast_ma is {'HMA' if self.is_hma_faster else 'EMA'} ({min(self.hma_period, self.ema_period)})")
        self.hist_df = pd.DataFrame()
        self.intra_df = pd.DataFrame()

    def fetch_data(self, fetch_hist=True, fetch_intra=True):
        """
        Fetches data based on flags and updates internal state.
        fetch_hist: Get past days data
        fetch_intra: Get today's data
        """
        try:
            # 1. Interval Mapping
            if self.interval == 1:
                interval_str = "minute"
            else:
                interval_str = f"{self.interval}minute"

            # ---------------------------------------------------------
            # Step A: Get Historical Data (Yesterday and back)
            # ---------------------------------------------------------
            if fetch_hist:
                now = datetime.datetime.now(tz=ZoneInfo('Asia/Kolkata'))
                start_date = now - datetime.timedelta(days=2) 
                
                f_str = start_date.strftime("%Y-%m-%d")
                t_str = now.strftime("%Y-%m-%d")

                log.info(f"[{self.symbol}] Fetching Historical: {f_str} to {t_str}")
                
                try:
                    resp_hist = self.api.get_historical_chart(self.exchange, self.token_id, interval_str, f_str, t_str)
                    data_hist = resp_hist.json()
                    if data_hist.get('status') == 'success' and data_hist.get('data') and 'candles' in data_hist['data']:
                        self.hist_df = pd.DataFrame(data_hist['data']['candles'], columns=['time', 'open', 'high', 'low', 'close', 'volume'])
                        # Convert time and standardize
                        self.hist_df['time'] = pd.to_datetime(self.hist_df['time']).dt.tz_localize(None)
                except Exception as e:
                    log.warning(f"[{self.symbol}] Historical API failed (might be empty if holiday): {e}")

            # ---------------------------------------------------------
            # Step B: Get Intraday Data (Today)
            # ---------------------------------------------------------
            if fetch_intra:
                log.info(f"[{self.symbol}] Fetching Intraday (Today)")
                
                exch_map = {"NSE": "1", "NFO": "2", "CDS": "3", "BSE": "4", "BFO": "5"}
                exch_id = exch_map.get(self.exchange, "1")
                
                try:
                    resp_intra = self.api.get_intraday_chart(exch_id, self.token_id, interval_str)
                    data_intra = resp_intra.json()
                    if data_intra.get('status') == 'success' and data_intra.get('data') and 'candles' in data_intra['data']:
                        self.intra_df = pd.DataFrame(data_intra['data']['candles'], columns=['time', 'open', 'high', 'low', 'close', 'volume'])
                        # Convert time and standardize
                        self.intra_df['time'] = pd.to_datetime(self.intra_df['time']).dt.tz_localize(None)
                except Exception as e:
                    log.warning(f"[{self.symbol}] Intraday API failed (Market might be closed): {e}")

            # ---------------------------------------------------------
            # Step C: Merge, Clean, and Calc Indicators
            # ---------------------------------------------------------
            self._merge_and_calculate()

        except Exception as e:
            log.error(f"[{self.symbol}] Critical error in data fetch: {e}")

    def _merge_and_calculate(self):
        """Merges hist_df and intra_df and calculates indicators"""
        if self.hist_df.empty and self.intra_df.empty:
            log.warning(f"[{self.symbol}] No data found from either source.")
            return

        # Concatenate
        full_df = pd.concat([df.dropna(axis=1, how='all') for df in [self.hist_df, self.intra_df]], ignore_index=True)
        
        # Drop duplicates based on time
        full_df = full_df.drop_duplicates(subset='time', keep='last')
        full_df = full_df.sort_values('time').reset_index(drop=True)
        
        # Convert columns to numeric
        cols = ['open', 'high', 'low', 'close', 'volume']
        full_df[cols] = full_df[cols].apply(pd.to_numeric)

        self.data = full_df.dropna().reset_index(drop=True)
        
        # Filter out zero volume candles
        self.data = self.data[self.data['volume'] > 0].reset_index(drop=True)

        # 1. Convert to HA
        self.data = self._calculate_heikin_ashi(self.data).dropna().reset_index(drop=True)

        # 2. Calculate HMA and EMA on HA_CLOSE
        self.data['hma'] = self._calculate_hma(self.data['ha_close'], self.hma_period)
        self.data['ema'] = self._calculate_ema(self.data['ha_close'], self.ema_period)
        
        # Define Fast/Slow columns for logic
        # If HMA is faster, Fast=HMA, Slow=EMA
        if self.is_hma_faster:
            self.data['fast_ma'] = self.data['hma']
            self.data['slow_ma'] = self.data['ema']
        else:
            self.data['fast_ma'] = self.data['ema']
            self.data['slow_ma'] = self.data['hma']

        if not self.data.empty:
            last_row = self.data.iloc[-1]
            log.info(f"[{self.symbol}] Data Loaded. Candles: {len(self.data)}. Last Close: {last_row['close']} FastMA: {last_row['fast_ma']:.2f} SlowMA: {last_row['slow_ma']:.2f}")
    
    def _calculate_heikin_ashi(self, df):
        ha_df = df.copy()
        ha_df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        ha_open = [df['open'].iloc[0]]
        for i in range(1, len(df)):
            ha_open.append((ha_open[-1] + ha_df['ha_close'].iloc[i-1]) / 2)
        ha_df['ha_open'] = ha_open
        ha_df['ha_high'] = ha_df[['high', 'ha_open', 'ha_close']].max(axis=1)
        ha_df['ha_low'] = ha_df[['low', 'ha_open', 'ha_close']].min(axis=1)
        
        # Round to 2 decimal places
        cols = ['ha_open', 'ha_high', 'ha_low', 'ha_close']
        ha_df[cols] = ha_df[cols].round(2)
        
        return ha_df
    
    def process_tick(self, tick_data):
        """
        Called from Main thread dispatcher.
        """
        try:
            ltp = float(tick_data.get('last_price', 0))
            if ltp == 0: return

            tick_time = datetime.datetime.now(tz=ZoneInfo('Asia/Kolkata'))

            # --- Candle Construction Logic ---
            if self.current_candle is None:
                # Align time to nearest interval bucket
                minute_bucket = (tick_time.minute // self.interval) * self.interval
                start_time = tick_time.replace(minute=minute_bucket, second=0, microsecond=0)
                self.current_candle = {
                    'time': start_time,
                    'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp,
                    'is_closed': False
                }
            else:
                # Check if current candle interval has passed
                next_candle_time = self.current_candle['time'] + datetime.timedelta(minutes=self.interval)
                
                if tick_time >= next_candle_time:
                    # Close the candle
                    self.current_candle['is_closed'] = True
                    self._on_candle_close(self.current_candle)
                    
                    # Start new candle
                    minute_bucket = (tick_time.minute // self.interval) * self.interval
                    start_time = tick_time.replace(minute=minute_bucket, second=0, microsecond=0)
                    self.current_candle = {
                        'time': start_time,
                        'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp,
                        'is_closed': False
                    }
                else:
                    # Update current candle
                    self.current_candle['high'] = max(self.current_candle['high'], ltp)
                    self.current_candle['low'] = min(self.current_candle['low'], ltp)
                    self.current_candle['close'] = ltp

            # Optional: Check Intra-candle stoploss here (Simulated BO)
            self._monitor_open_position(ltp)

        except Exception as e:
            log.error(f"[{self.symbol}] Tick Processing Error: {e}")

    def _on_candle_close(self, raw_candle):
        # Calculate HA for this single new candle
        if self.data.empty:
            ha_open = raw_candle['open']
            ha_close = (raw_candle['open'] + raw_candle['high'] + raw_candle['low'] + raw_candle['close']) / 4
        else:
            prev_ha = self.data.iloc[-1]
            ha_open = (prev_ha['ha_open'] + prev_ha['ha_close']) / 2
            ha_close = (raw_candle['open'] + raw_candle['high'] + raw_candle['low'] + raw_candle['close']) / 4

        ha_high = max(raw_candle['high'], ha_open, ha_close)
        ha_low = min(raw_candle['low'], ha_open, ha_close)

        # Round live HA values to 2 decimals
        ha_open = round(ha_open, 2)
        ha_close = round(ha_close, 2)
        ha_high = round(ha_high, 2)
        ha_low = round(ha_low, 2)

        # Append
        new_row = {
            'time': raw_candle['time'],
            'open': raw_candle['open'], 'close': raw_candle['close'],
            'ha_open': ha_open, 'ha_high': ha_high, 'ha_low': ha_low, 'ha_close': ha_close
        }
        self.data = pd.concat([self.data, pd.DataFrame([new_row])], ignore_index=True)
        
        # --- RAM Optimization: Truncate Data ---
        keep_size = max(self.hma_period, self.ema_period) * 2 + 10
        if len(self.data) > keep_size:
            self.data = self.data.tail(keep_size).copy().reset_index(drop=True)
        # ---------------------------------------

        # Recalculate HMA/EMA
        self.data['hma'] = self._calculate_hma(self.data['ha_close'], self.hma_period)
        self.data['ema'] = self._calculate_ema(self.data['ha_close'], self.ema_period)

        # Update Fast/Slow columns used for logic checking
        if self.is_hma_faster:
            self.data['fast_ma'] = self.data['hma']
            self.data['slow_ma'] = self.data['ema']
        else:
            self.data['fast_ma'] = self.data['ema']
            self.data['slow_ma'] = self.data['hma']

        self._analyze_signal()

    def _analyze_signal(self):
        if len(self.data) < 2: return
        
        curr = self.data.iloc[-1]
        prev = self.data.iloc[-2]
        
        # Check against Fast/Slow columns
        if pd.isna(curr['fast_ma']) or pd.isna(prev['slow_ma']) or pd.isna(curr['slow_ma']) or pd.isna(prev['fast_ma']): return

        signal = None
        
        # Crossover Logic
        # Buy: Fast MA Crosses Above Slow MA
        if prev['fast_ma'] <= prev['slow_ma'] and curr['fast_ma'] > curr['slow_ma']:
            signal = "BUY"
        # Sell: Fast MA Crosses Below Slow MA
        elif prev['fast_ma'] >= prev['slow_ma'] and curr['fast_ma'] < curr['slow_ma']:
            signal = "SELL"
            
        log.info(f"[{self.symbol}] Candle Closed: {curr['time'].strftime('%H:%M')} | HA Close: {curr['ha_close']:.2f} | FastMA: {curr['fast_ma']:.2f} | SlowMA: {curr['slow_ma']:.2f} | Signal: {signal}")

        # Confirmation Logic
        if signal:
            if self.required_confirmations == 0:
                self._execute_signal(signal, curr['close'])
            else:
                log.info(f"[{self.symbol}] Signal {signal} detected. Waiting for {self.required_confirmations} confirmations.")
                self.pending_signal = signal
                self.confirmation_count = 0
                return

        # Check Pending Confirmation
        if self.pending_signal:
            is_valid = False
            if self.pending_signal == "BUY" and curr['ha_close'] > prev['ha_close']:
                is_valid = True
            elif self.pending_signal == "SELL" and curr['ha_close'] < prev['ha_close']:
                is_valid = True
            
            if is_valid:
                self.confirmation_count += 1
                log.info(f"[{self.symbol}] Confirmation {self.confirmation_count}/{self.required_confirmations} OK.")
                if self.confirmation_count >= self.required_confirmations:
                    self._execute_signal(self.pending_signal, curr['close'])
                    self.pending_signal = None
                    self.confirmation_count = 0
            else:
                log.info(f"[{self.symbol}] Signal {self.pending_signal} invalidated.")
                self.pending_signal = None
                self.confirmation_count = 0

    def _execute_signal(self, signal, price):
        """Handles Entry and Reversals with Limit Orders"""
        
        # If we have a position and get an opposite signal, Reverse
        if self.position == "LONG" and signal == "SELL":
            log.info(f"[{self.symbol}] Exiting LONG @ {price}")
            resp = self._place_order("SELL", self.quantity, price) 
            if resp and resp.get('status') == 'success':
                self.position = "FLAT"

        elif self.position == "SHORT" and signal == "BUY":
            log.info(f"[{self.symbol}] Exiting SHORT @ {price}")
            resp = self._place_order("BUY", self.quantity, price) 
            if resp and resp.get('status') == 'success':
                self.position = "FLAT"
            
        elif self.position == "FLAT":
            log.info(f"[{self.symbol}] Entry: Going {signal} @ {price}")
            resp = self._place_order(signal, self.quantity, price)
            if resp and resp.get('status') == 'success':
                self.position = "LONG" if signal == "BUY" else "SHORT"

    def _place_order(self, transaction_type, qty, price=0):
        """
        Places a Regular MIS LIMIT Order with Retry Logic.
        """
        max_retries = 3
        
        limit_price = f"{float(price):.2f}"
        
        for attempt in range(1, max_retries + 1):
            try:
                log.info(f"[{self.symbol}] Placing LIMIT Order ({transaction_type}) @ {limit_price}, Attempt {attempt}/{max_retries}...")
                
                resp = self.api.place_order(
                    _variety="regular",
                    _tradingsymbol=self.symbol,
                    _exchange=self.exchange,
                    _transaction_type=transaction_type,
                    _order_type=self.order_type,         
                    _quantity=qty,
                    _product=self.product_type,
                    _validity="DAY",
                    _price= limit_price if self.order_type == "LIMIT" else "0",          
                    _trigger_price="0",
                    _disclosed_quantity="0",
                    _tag="mStock_HMAEMA_Bot"
                )
                
                resp_json = resp.json()

                if isinstance(resp_json, list):
                    if len(resp_json) > 0:
                        resp_json = resp_json[0]
                    else:
                        log.error(f"[{self.symbol}] Received empty list response from API.")
                        return None
                
                if resp_json.get('status') == 'success':
                    log.info(f"[{self.symbol}] Order Placed Successfully: {resp_json}")
                    return resp_json
                else:
                    log.error(f"[{self.symbol}] API rejected order: {resp_json}")
                    return None 

            except Exception as e:
                error_msg = str(e)
                if "Connection aborted" in error_msg or "RemoteDisconnected" in error_msg or "ConnectionError" in error_msg:
                    log.warning(f"[{self.symbol}] Connection lost during order placement. Retrying... ({error_msg})")
                    time.sleep(1)
                else:
                    log.error(f"[{self.symbol}] Critical Error in Order Placement: {e}")
                    break
        
        log.error(f"[{self.symbol}] Failed to place order after {max_retries} attempts.")
        return None

    def _monitor_open_position(self, ltp):
        """
        Simulated Bracket Order Logic (SL/TP)
        """
        if self.position == "FLAT": return
        pass

    def check_eod(self):
        """Check End of Day Square off"""
        now_str = datetime.datetime.now(tz=ZoneInfo('Asia/Kolkata')).strftime("%H:%M")
        
        if now_str >= self.square_off_time and self.position != "FLAT":
            log.info(f"[{self.symbol}] EOD Reached. Squaring off.")
            
            exit_price = 0
            if self.current_candle:
                exit_price = self.current_candle['close']
            
            if self.position == "LONG":
                self._place_order("SELL", self.quantity, exit_price)
            elif self.position == "SHORT":
                self._place_order("BUY", self.quantity, exit_price)
                
            self.position = "FLAT"
