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
from datetime import datetime,timedelta
from django.core.cache import cache
import redis

# refresh_time=datetime.now()
# ws_connection = None
# masterclass_dict = {}
# ltp_cache={}
# accounts_global=pd.DataFrame()
# jainam_user_ids={}

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

class MasterConnectionManager:
    """
    Handles master class instances, accounts, and user IDs per worker.
    Shared data (tokens, LTP cache, user IDs) goes to Redis.
    """

    def __init__(self):
        self.masterclass_dict = {}
        self.accounts_global = []
        self.jainam_user_ids = {}
        self.refresh_time = datetime.now()
    
    def master_connection(self):
        """
        Initializes accounts and masterclass instances if needed.
        Reads shared info from Redis if available.
        """
        try:
            cached_accounts = r.get("accounts_global")
            cached_user_ids = r.get("jainam_user_ids")
            if cached_accounts and cached_user_ids:
                self.accounts_global = json.loads(cached_accounts)
                self.jainam_user_ids = json.loads(cached_user_ids)
            
            if self.masterclass_dict and self.accounts_global and self.jainam_user_ids and (datetime.now() - self.refresh_time < timedelta(hours=6)):
                return self.masterclass_dict,self.accounts_global,self.jainam_user_ids
            

            r.flushdb()  # or selectively delete keys
            self._load_accounts_from_file()
            self.refresh_time = datetime.now()
            return self.masterclass_dict, self.accounts_global, self.jainam_user_ids
        
        except Exception as e:
            # print("Error in master_connection:", e)
            raise
    
    def _load_accounts_from_file(self):
        file_path = os.path.join(settings.BASE_DIR, 'jainam', 'Ayush Keys.xlsx')
        retries=0
        while retries<2:
            try:
                accounts=pd.read_excel(file_path)
                retries=0
                break
            except:
                retries+=1
        accounts["two-fa"] = None
        accounts["auth_token"] = None
        accounts["ROC"] = 0
        accounts['Enabled'] = accounts['Enabled'].str.lower()
        accounts = accounts[accounts["Enabled"] == "yes"]
        self.accounts_global=accounts['Name'].tolist()
        for _, row in accounts.iterrows():
            if 'jainam' in row['Name'].lower():
                xt = JXTSConnect(row['App ID'], row['App Secret'], "WEBAPI")
                xt.hostlookup_login()
                res = xt.interactive_login()
                masterclass_instance = xt
                self.jainam_user_ids[row["Name"]] = row["User ID"]
            else:
                masterclass_instance = MasterTrustUser(
                    username=row["User ID"],
                    password=row["Password"],
                    twofa=row["Year"],
                    live=True,
                    app_id=row["App ID"],
                    app_secret=row["App Secret"],
                )
            self.masterclass_dict[row["Name"]] = masterclass_instance
        # Store serializable info in Redis for other workers
        for key in self.masterclass_dict:
            if 'jainam' not in key.lower():
                auth_token=self.masterclass_dict[key].auth_token
                base_url=self.masterclass_dict[key].base_url.replace('https://','')
                ws_url = f"wss://{base_url}/ws/v1/feeds?token={auth_token}"
        r.set("ws_url", ws_url)
        r.set("accounts_global", json.dumps(self.accounts_global))
        r.set("jainam_user_ids", json.dumps(self.jainam_user_ids))




# def return_ltp_cache():
#     global ltp_cache
#     return ltp_cache

# def is_ws_connected():
#     global ws_connection
#     return ws_connection and ws_connection.sock and ws_connection.sock.connected

# def parse_marketdata_message(message):
#     global ltp_cache
#     try:
#         # Extract fields as per spec
#         data = struct.unpack(">B B I I I I I I I I Q Q I I I I I I I I I I I I", message[:98])

#         parsed = {
#             "exchange_code": data[1],
#             "instrument_token": data[2],
#             "ltp": data[3] / 100,
#             "last_traded_time": data[4],
#             "last_quantity": data[5],
#             "trade_volume": data[6],
#             "bid_price": data[7] / 100,
#             "bid_quantity": data[8],
#             "ask_price": data[9] / 100,
#             "ask_quantity": data[10],
#             "total_buy_qty": data[11],
#             "total_sell_qty": data[12],
#             "average_trade_price": data[13] / 100,
#             "exchange_timestamp": data[14],
#             "open_price": data[15] / 100,
#             "high_price": data[16] / 100,
#             "low_price": data[17] / 100,
#             "close_price": data[18] / 100
#         }
#         ltp_key=str(parsed['exchange_code'])+'_'+str(parsed['instrument_token'])
#         ltp_cache[ltp_key] = parsed['ltp']
#         # #print('ltp_data:' ,ltp_cache)
#         return parsed['ltp']
#     except:
#         return 0


# def on_message(ws, message):
#     # Binary data is received
#     try:
#         parse_marketdata_message(message)
#     except:
#         pass
#         #print('Parsing Error')


# def on_error(ws, error):
#     # print('Error in connection ',error)
#     pass
#     #print('Error in WS connection')


# def on_close(ws, close_status_code, close_msg):
#     ws_connection_call()
#     #print('WS connection Dropped')


# def on_open(ws):
#     global ws_connection
#     ws_connection=ws
#     # # Send subscription
#     # ws.send(json.dumps(subscribe_message))
#     # #print("📨 Sent subscription:", subscribe_message)

#     # Start heartbeat thread
#     def send_heartbeat():
#         while True:
#             time.sleep(10)
#             ws.send(json.dumps({ "a": "h", "v": [], "m": "" }))
#             # print("💓 Sent heartbeat")

#     heartbeat_thread = threading.Thread(target=send_heartbeat)
#     heartbeat_thread.daemon = True
#     heartbeat_thread.start()


# def run_ws(ws_url):
#     global ws_connection
#     websocket.enableTrace(False)
#     ws_connection = websocket.WebSocketApp(
#         ws_url,
#         on_open=on_open,
#         on_message=on_message,
#         on_error=on_error,
#         on_close=on_close
#     )
#     ws_connection.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

# def ws_connection_call():
#     global masterclass_dict
#     for key in masterclass_dict:
#         if 'jainam' not in key.lower():
#             auth_token=masterclass_dict[key].auth_token
#             base_url=masterclass_dict[key].base_url.replace('https://','')
#             ws_url = f"wss://{base_url}/ws/v1/feeds?token={auth_token}"
            
#             threading.Thread(target=run_ws, args=(ws_url,), daemon=True).start()
#             break

# def master_connection():
#     global ws_connection
#     global masterclass_dict
#     global accounts_global
#     global jainam_user_ids
#     global ltp_cache
#     global refresh_time
#     try:
#         if masterclass_dict and not accounts_global.empty and jainam_user_ids and (datetime.now() - refresh_time < timedelta(hours=6)):
#             if not is_ws_connected():
#                 ws_connection_call()
#             return [masterclass_dict,accounts_global,jainam_user_ids,ws_connection,ltp_cache]
#         file_path = os.path.join(settings.BASE_DIR, 'jainam', 'Ayush Keys.xlsx')
#         retries=0
#         while retries<2:
#             try:
#                 accounts=pd.read_excel(file_path)
#                 retries=0
#                 break
#             except:
#                 retries+=1
#         accounts["two-fa"] = None
#         accounts["auth_token"] = None
#         accounts["ROC"] = 0
#         accounts['Enabled'] = accounts['Enabled'].str.lower()
#         accounts = accounts[accounts["Enabled"] == "yes"]
#         accounts_global=accounts['Name']
#         for index, row in accounts.iterrows():
#             if 'jainam' in row['Name'].lower():
#                 API_KEY = row['App ID']
#                 API_SECRET = row['App Secret']
#                 source = "WEBAPI"
#                 xt = JXTSConnect(API_KEY, API_SECRET, source)
#                 res = xt.hostlookup_login()
#                 response = xt.interactive_login()
#                 set_interactiveToken = response["result"]["token"]
#                 set_iuserID = response["result"]["userID"]
#                 masterclass_dict[row["Name"]] = xt
#                 jainam_user_ids[row["Name"]] = row["User ID"]
#                 continue

#             masterclass_dict[row["Name"]] = MasterTrustUser(
#                 username=row["User ID"],
#                 password=row["Password"],
#                 twofa=row["Year"],
#                 live=True,
#                 app_id=row["App ID"],
#                 app_secret=row["App Secret"],
#             )
#         ws_connection_call()
#     except:
#         master_connection()
#     return [masterclass_dict,accounts_global,jainam_user_ids,ws_connection,ltp_cache]
