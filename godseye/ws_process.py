import time
import json
import threading
import redis
import websocket
import struct
import ssl
import pytz
from datetime import datetime
from datetime import time as t

# Redis client
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
last_tick_time = time.time()
IST = pytz.timezone("Asia/Kolkata")

def is_market_open():
    now_ist = datetime.now(IST).time()  # get current IST time
    market_open = t(8, 15)
    market_close = t(14, 30)
    return market_open <= now_ist <= market_close

def monitor_feed(ws):
    global last_tick_time
    while True:
        time.sleep(5)
        # If no tick for 30 seconds → kill connection
        if is_market_open() and time.time() - last_tick_time > 90:
            r.delete("ws_pending_subscriptions")
            r.delete("ws_last_subscriptions")
            ws.close()
            break

# -----------------------------------------
# Build WS URL (from Redis)
# -----------------------------------------
def build_ws_url():
    return r.get("ws_url")


# -----------------------------------------
# Market Data Parser
# -----------------------------------------
# def parse_marketdata_message(message):
#     """Parse binary message from marketdata feed."""
#     # Skip first byte (mode) – already known
#     mode = message[0]

#     if len(message) < 58:
#         #print("⚠️ Message too short")
#         return

#     # Extract fields as per spec
#     data = struct.unpack(">B B I I I I I I I I Q Q I I I I I I I I I I I I", message[:98])

#     parsed = {
#         "exchange_code": data[1],
#         "instrument_token": data[2],
#         "ltp": data[3] / 100,
#         "last_traded_time": data[4],
#         "last_quantity": data[5],
#         "trade_volume": data[6],
#         "bid_price": data[7] / 100,
#         "bid_quantity": data[8],
#         "ask_price": data[9] / 100,
#         "ask_quantity": data[10],
#         "total_buy_qty": data[11],
#         "total_sell_qty": data[12],
#         "average_trade_price": data[13] / 100,
#         "exchange_timestamp": data[14],
#         "open_price": data[15] / 100,
#         "high_price": data[16] / 100,
#         "low_price": data[17] / 100,
#         "close_price": data[18] / 100
#     }
#     print('receiving messages')
#     return parsed
def parse_compact_message(message: bytes):
    """Parse compact_marketdata message (only LTP)"""
    try:
        if len(message) < 10:  # must have at least 10 bytes: 1+4+4+1 padding
            return None

        # Skip first byte if mode exists (like in your old feed)
        exchange, token, ltp = struct.unpack(">B I I", message[1:10])
        # print(exchange,token,ltp)
        return {
            "exchange_code": exchange,
            "instrument_token": token,
            "ltp": ltp / 100  # scale to float
        }
    except Exception as e:
        return None
# -----------------------------------------
# WebSocket Event Handlers
# -----------------------------------------
def on_open(ws):
    #print("🟢 WebSocket Connected")

    # -----------------------------
    # HEARTBEAT
    # -----------------------------
    def heartbeat():
        while True:
            try:
                ws.send(json.dumps({"a": "h", "v": [], "m": ""}))
            except:
                break
            time.sleep(10)

    threading.Thread(target=heartbeat, daemon=True).start()

    def periodic_resubscribe():
        while True:
            try:
                # Read subscription set from Redis
                desired_tokens = set(r.smembers("md_subscriptions"))

                # Last confirmed subscriptions
                last_sub_set = set(r.smembers('ws_last_subscriptions'))

                # Pending subscriptions
                pending_set = set(r.smembers('ws_pending_subscriptions'))

                # Tokens to subscribe: new + pending
                to_subscribe = desired_tokens - last_sub_set

                if not to_subscribe:
                    time.sleep(0.5)
                    continue

                tokens_list = []
                for token_str in to_subscribe:
                    try:
                        ex, tk = token_str.split("_")
                        tokens_list.append([int(ex), int(tk)])
                    except Exception as e:
                        continue
                
                # Convert to sorted list of token pairs
                if tokens_list:
                    sub_msg = {
                        "a": "subscribe",
                        "v": tokens_list,
                        "m": "compact_marketdata"
                    }
                    try:
                        ws.send(json.dumps(sub_msg))
                        # Add all to pending until confirmed
                        if to_subscribe:
                            r.sadd('ws_pending_subscriptions', *to_subscribe)
                    except Exception as e:
                        pass
                        # print("❌ WS send failed, will retry:", e)

            except Exception as e:
                pass
                # print(e)

            time.sleep(0.5)

    threading.Thread(target=periodic_resubscribe, daemon=True).start()
    threading.Thread(target=monitor_feed, args=(ws,), daemon=True).start()

# def on_message(ws, message):
#     global last_tick_time
#     try:
#         if isinstance(message, bytes):
#             parsed = parse_marketdata_message(message)
#             last_tick_time = time.time()   # ✅ Update heartbeat of feed
#             #print(parsed)
#         else:
#             #print("📝 Text Message:", message)
#             return
#     except Exception as e:
#         #print("❌ Parsing Error:", e)
#         return

#     # Store LTP in redis
#     key = f"{parsed['exchange_code']}_{parsed['instrument_token']}"
#     r.set(key, parsed['ltp'])
#     r.srem("ws_pending_subscriptions", key)
#     r.sadd("ws_last_subscriptions", key)
#     #print("📌 Updated LTP:", parsed['ltp'])

def on_message(ws, message):
    global last_tick_time

    if not isinstance(message, bytes):
        return

    parsed = parse_compact_message(message)
    if not parsed:
        return

    last_tick_time = time.time()

    key = f"{parsed['exchange_code']}_{parsed['instrument_token']}"

    try:
        r.set(key, parsed['ltp'])
        r.srem("ws_pending_subscriptions", key)
        r.sadd("ws_last_subscriptions", key)
    except Exception as e:
        pass
        # print("Redis error:", e)

def on_error(ws, error):
    # print(error)
    ws.close()
    # print("❌ WS Error:", error)


def on_close(ws, code, msg):
    # print('closing')
    pass
    #print("🔴 WS Closed:", code, msg)


# -----------------------------------------
# Auto-Reconnect Loop
# -----------------------------------------
def run_forever():
    retry_delay = 3

    while True:
        # print('Retrying')
        ws_url = build_ws_url()

        if not ws_url:
            #print("⏳ Waiting for ws_url token in Redis...")
            time.sleep(5)
            continue

        #print(f"🔗 Connecting to: {ws_url}")

        try:
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

        except Exception as e:
            pass
            #print("❌ Fatal WebSocket Error:", e)

        #print(f"🔄 Reconnecting in {retry_delay} seconds...")
        time.sleep(retry_delay)

        retry_delay = min(retry_delay * 2, 30)  # exponential backoff


# -----------------------------------------
# Main
# -----------------------------------------
if __name__ == "__main__":
    #print("🚀 Starting WebSocket Feed Process")
    run_forever()
