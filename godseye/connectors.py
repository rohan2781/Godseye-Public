from mastertrust.master_trust_class import MasterTrustUser
import os
from django.conf import settings
import pandas as pd
from jainam.JConnect import XTSConnect as JXTSConnect
import websocket
import threading
import time
import json
import struct
import ssl
from datetime import datetime

sslopt = {"cert_reqs": ssl.CERT_NONE} ### Not for prodcution
masterclass_dict = {}
jainam_user_ids = {}
accounts_global=pd.DataFrame()
ltp_cache={}
heartbeat_msg = { "a": "h", "v": [], "m": "" }
multiplier=100
ws_connection={}
refresh_time=datetime.now()

def is_ws_connected():
    global ws_connection
    return ws_connection and ws_connection.sock and ws_connection.sock.connected

def parse_marketdata_message(message):
    try:
        # Extract fields as per spec
        data = struct.unpack(">B B I I I I I I I I Q Q I I I I I I I I I I I I", message[:98])

        parsed = {
            "exchange_code": data[1],
            "instrument_token": data[2],
            "ltp": data[3] / multiplier,
            "last_traded_time": data[4],
            "last_quantity": data[5],
            "trade_volume": data[6],
            "bid_price": data[7] / multiplier,
            "bid_quantity": data[8],
            "ask_price": data[9] / multiplier,
            "ask_quantity": data[10],
            "total_buy_qty": data[11],
            "total_sell_qty": data[12],
            "average_trade_price": data[13] / multiplier,
            "exchange_timestamp": data[14],
            "open_price": data[15] / multiplier,
            "high_price": data[16] / multiplier,
            "low_price": data[17] / multiplier,
            "close_price": data[18] / multiplier
        }
        ltp_key=str(parsed['exchange_code'])+'_'+str(parsed['instrument_token'])
        print('ltp_key',ltp_key)
        ltp_cache[ltp_key]=parsed['ltp']
        print('ltp_data:' ,ltp_cache)
        return parsed['ltp']
    except:
        return 0


def on_message(ws, message):
    # Binary data is received
    try:
        parse_marketdata_message(message)
    except:
        print('Parsing Error')


def on_error(ws, error):
    print('Error in WS connection')


def on_close(ws, close_status_code, close_msg):
    ws_connection_call()
    print('WS connection Dropped')


def on_open(ws):
    global heartbeat_msg
    # # Send subscription
    # ws.send(json.dumps(subscribe_message))
    # print("📨 Sent subscription:", subscribe_message)

    # Start heartbeat thread
    def send_heartbeat():
        while True:
            time.sleep(10)
            ws.send(json.dumps(heartbeat_msg))
            print("💓 Sent heartbeat")

    heartbeat_thread = threading.Thread(target=send_heartbeat)
    heartbeat_thread.daemon = True
    heartbeat_thread.start()


def run_ws(ws_url):
    global ws_connection
    websocket.enableTrace(False)
    ws_connection = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws_connection.run_forever(sslopt=sslopt)

def ws_connection_call():
    global masterclass_dict
    for key in masterclass_dict:
        if 'jainam' not in key.lower():
            auth_token=masterclass_dict[key].auth_token
            base_url=masterclass_dict[key].base_url.replace('https://','')
            ws_url = f"wss://{base_url}/ws/v1/feeds?token={auth_token}"
            threading.Thread(target=run_ws, args=(ws_url,), daemon=True).start()
            break

def master_connection():
    global masterclass_dict
    global accounts_global
    global jainam_user_ids
    global ws_connection
    global ltp_cache
    global refresh_time
    try:
        if masterclass_dict and not accounts_global.empty and jainam_user_ids and (datetime.now()-refresh_time)<6:
            if not is_ws_connected:
                ws_connection_call()
            return [masterclass_dict,accounts_global,jainam_user_ids,ws_connection,ltp_cache]
        file_path = os.path.join(settings.BASE_DIR, 'jainam', 'Ayush Keys.xlsx')
        accounts=pd.read_excel(file_path)
        accounts["two-fa"] = None
        accounts["auth_token"] = None
        accounts["ROC"] = 0
        accounts['Enabled'] = accounts['Enabled'].str.lower()
        accounts = accounts[accounts["Enabled"] == "yes"]
        accounts_global=accounts['Name']
        for index, row in accounts.iterrows():
            if 'jainam' in row['Name'].lower():
                API_KEY = row['App ID']
                API_SECRET = row['App Secret']
                source = "WEBAPI"
                xt = JXTSConnect(API_KEY, API_SECRET, source)
                res = xt.hostlookup_login()
                response = xt.interactive_login()
                set_interactiveToken = response["result"]["token"]
                set_iuserID = response["result"]["userID"]
                masterclass_dict[row["Name"]] = xt
                jainam_user_ids[row["Name"]] = row["User ID"]
                continue

            masterclass_dict[row["Name"]] = MasterTrustUser(
                username=row["User ID"],
                password=row["Password"],
                twofa=row["Year"],
                live=True,
                app_id=row["App ID"],
                app_secret=row["App Secret"],
            )
        ws_connection_call()
    except:
        master_connection()
    refresh_time=datetime.now()
    return [masterclass_dict,accounts_global,jainam_user_ids,ws_connection,ltp_cache]
