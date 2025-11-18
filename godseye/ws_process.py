import time
import json
import threading
import redis
import websocket
import struct
import ssl

# Redis client
r = redis.Redis(host="localhost", port=6379, decode_responses=True)


# -----------------------------------------
# Build WS URL (from Redis)
# -----------------------------------------
def build_ws_url():
    return r.get("ws_url")


# -----------------------------------------
# Market Data Parser
# -----------------------------------------
def parse_marketdata_message(message):
    """Parse binary message from marketdata feed."""
    # Skip first byte (mode) – already known
    mode = message[0]

    if len(message) < 58:
        #print("⚠️ Message too short")
        return

    # Extract fields as per spec
    data = struct.unpack(">B B I I I I I I I I Q Q I I I I I I I I I I I I", message[:98])

    parsed = {
        "exchange_code": data[1],
        "instrument_token": data[2],
        "ltp": data[3] / 100,
        "last_traded_time": data[4],
        "last_quantity": data[5],
        "trade_volume": data[6],
        "bid_price": data[7] / 100,
        "bid_quantity": data[8],
        "ask_price": data[9] / 100,
        "ask_quantity": data[10],
        "total_buy_qty": data[11],
        "total_sell_qty": data[12],
        "average_trade_price": data[13] / 100,
        "exchange_timestamp": data[14],
        "open_price": data[15] / 100,
        "high_price": data[16] / 100,
        "low_price": data[17] / 100,
        "close_price": data[18] / 100
    }
    return parsed

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

    ws.last_subscriptions = set()

    def periodic_resubscribe():
        while True:
            try:
                # Read subscription set from Redis
                current = r.smembers("md_subscriptions")

                # Convert to sorted list of token pairs
                tokens = []
                for item in current:
                    ex, tk = item.split("_")
                    tokens.append([int(ex), int(tk)])

                # If there's no change in subscription, skip
                if current == ws.last_subscriptions:
                    time.sleep(0.5)
                    continue

                # Save new state
                ws.last_subscriptions = current

                # Send subscribe command
                if tokens:
                    sub_msg = {
                        "a": "subscribe",
                        "v": tokens,
                        "m": "marketdata"
                    }
                    ws.send(json.dumps(sub_msg))
                    #print("🔁 Updated Subscriptions:", sub_msg)

            except Exception as e:
                pass
                #print("❌ Resubscribe Error:", e)

            time.sleep(0.5)

    threading.Thread(target=periodic_resubscribe, daemon=True).start()

def on_message(ws, message):
    try:
        if isinstance(message, bytes):
            parsed = parse_marketdata_message(message)
            #print(parsed)
        else:
            #print("📝 Text Message:", message)
            return
    except Exception as e:
        #print("❌ Parsing Error:", e)
        return

    # Store LTP in redis
    key = f"{parsed['exchange_code']}_{parsed['instrument_token']}"
    r.set(key, parsed['ltp'])
    #print("📌 Updated LTP:", parsed['ltp'])


def on_error(ws, error):
    pass
    #print("❌ WS Error:", error)


def on_close(ws, code, msg):
    pass
    #print("🔴 WS Closed:", code, msg)


# -----------------------------------------
# Auto-Reconnect Loop
# -----------------------------------------
def run_forever():
    retry_delay = 3

    while True:
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
