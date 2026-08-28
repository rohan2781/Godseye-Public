from mastertrust.master_trust_class import MasterTrustUser
import os
from django.conf import settings
import pandas as pd
from jainam.JConnect import XTSConnect as JXTSConnect
from jainam.MConnect import XTSConnect as MXTSConnect
from jainam.mlbConnect import XTSConnect as MLBXTSConnect
import websocket
import threading
import time
import json
import struct
import ssl
from datetime import datetime,timedelta
from django.core.cache import cache
import redis
from .models import *
import pickle

# refresh_time=datetime.now()
# ws_connection = None
# masterclass_dict = {}
# ltp_cache={}
# accounts_global=pd.DataFrame()
# jainam_user_ids={}

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    
def master_connection(user):
    """
    Initializes accounts and masterclass instances if needed.
    Reads shared info from Redis if available.
    """
    cached_refresh = r.get("refresh_time_"+str(user))
    if cached_refresh and time.time() - float(cached_refresh) < 6 * 3600:
        print('from db')
    else:
        _load_accounts()
        r.set("refresh_time_"+str(user), time.time())

    masterclass_dict={}
    jainam_user_ids={}
    accounts_global=[]

    accounts = Accounts.objects.filter(users__contains=user,enabled__iexact='yes')
    for account in accounts:
        masterclass_dict[account.name]=pickle.loads(account.master_class_instance_data)
        accounts_global.append(account.name)
        jainam_user_ids[account.name]=account.userId
    return masterclass_dict, accounts_global, jainam_user_ids

    
def _load_accounts():
    masterclass_dict={}
    accounts_db = Accounts.objects.filter(enabled__iexact='yes')
    accounts=pd.DataFrame(list(accounts_db.values()))
    # accounts["two-fa"] = None
    # accounts["auth_token"] = None
    # accounts["ROC"] = 0
    # accounts['Enabled'] = accounts['Enabled'].str.lower()
    # accounts = accounts[accounts["Enabled"] == "yes"]
    for _, row in accounts.iterrows():
        if 'jainam' in row['name'].lower():
            if 'master' in row['name'].lower():
                xt = MXTSConnect(row['appId'], row['appSecret'], "WEBAPI")
            elif 'mlb' in row['name'].lower():
                xt = MLBXTSConnect(row['appId'], row['appSecret'], "WEBAPI")
            else:
                xt = JXTSConnect(row['appId'], row['appSecret'], "WEBAPI")

            xt.hostlookup_login()
            xt.interactive_login()
            # masterclass_instance = xt
            # jainam_user_ids[row["name"]] = row["userId"]
            masterclass_dict[row["name"]] = xt
        else:
            masterclass_instance = MasterTrustUser(
                username=row["userId"],
                password=row["password"],
                twofa=row["totp"],
                live=True,
                app_id=row["appId"],
                app_secret=row["appSecret"],
            )
            # if r.get('set_contract')=='0':
            #     t = threading.Thread(target=call_contracts, args=(masterclass_instance,), daemon=True)
            #     t.start()
                
            masterclass_dict[row["name"]] = masterclass_instance
        # masterclass_dict[row["name"]] = masterclass_instance
    # Store serializable info in Redis for other workers
    for key in masterclass_dict:
        try:
            if 'jainam' not in key.lower() and masterclass_dict[key].auth_token is not None:
                auth_token=masterclass_dict[key].auth_token
                base_url=masterclass_dict[key].base_url.replace('https://','')
                ws_url = f"wss://{base_url}/ws/v1/feeds?token={auth_token}"
                r.set("ws_url", ws_url)
                break
        except:
            continue

    for key in masterclass_dict:
        account = Accounts.objects.get(name=key)  # Use `get()` because you want a single object
        serialized_data = pickle.dumps(masterclass_dict[key])
        account.master_class_instance_data = serialized_data
        account.save()
        
    # r.set('masterclass_dict', json.dumps(masterclass_dict))