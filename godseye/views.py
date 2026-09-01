from django.db import close_old_connections
from django.shortcuts import render,redirect
from django.http import HttpResponse, JsonResponse
from django.contrib import messages
from .connectors import master_connection
# from godseye.connectors import master_connection,return_ltp_cache
# from .master_manager import master_manager
from datetime import datetime,date
from functools import wraps
import pandas as pd
import json
from kiteconnect import KiteConnect
from godseye.freeze_quantity import get_freeze_quantity_from_nse
import math
from threading import Thread
import datetime as dt
import re
from collections import defaultdict, deque
from django.urls import reverse
import time
from django.middleware.csrf import get_token
from django.core.cache import cache
import redis
import os
from django.conf import settings
import platform
from .models import *
import requests
import pickle
import subprocess
from django.db.models import Q
from datetime import timezone, timedelta
import csv


r = redis.Redis(host='localhost', port=6379, decode_responses=True)

instrument_cache={}
positions_data=[]

def dbdata(req):
    try:
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="TradeBook.csv"'
        writer = csv.writer(response)

        fields = [f.name for f in TradeBook._meta.fields]
        writer.writerow(fields)
        for obj in TradeBook.objects.all().values_list(*fields):
            writer.writerow(obj)
        return response
    except Exception as e:
        messages.info(req, 'Error Downloading Data')
        return redirect('home')

def restart_ws_service(req):
    if platform.system()!='Windows':
        try:
            subprocess.run(["sudo", "systemctl", "stop", "ws.service"], check=True)
            time.sleep(2)
            subprocess.run(["redis-cli", "FLUSHALL"], check=True)
            subprocess.run(["sudo", "systemctl", "restart", "redis"], check=True)
            r.set(name='ws_restart',value='1',ex=25200)
            return redirect('logout')
            # subprocess.run(["sudo", "systemctl", "start", "ws.service"], check=True)
            # r.set('ws_restart',1,25200)
        except subprocess.CalledProcessError as e:
            pass
    messages.info(req,'Error restarting websocket')
    return redirect('login')

def login_required(view_func):
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        # Retrieve the token from cookies
        token = request.COOKIES.get('x-auth-token')
        if token:
            # Decrypt and verify the token (you can customize this based on your logic)
            parts = token.split('_')
            if parts[0] == 'user':
                user_id = int(parts[1])

            try:
                cached=r.get(str(user_id))
            except:
                return redirect('/login')
            if user_id and cached=='1':
                # The token is valid, continue with the request
                user_data=Users.objects.filter(userid=user_id).first()
                request.user = user_data.username
                return view_func(request, *args, **kwargs)
            else:
                # Invalid token, redirect to login
                return redirect('/login')
        else:
            # No token found, redirect to login
            return redirect('/login')

    return _wrapped_view


def maybe_start_account_jobs(masterclass_dict):
    # try:
    #         today = date.today()
    #         # Filter rows with expiry date less than today and delete them
    #         TradeBook.objects.filter(expiry__lt=today).delete()
    # except:
    #     pass
    now = time.time()

    try:
        last_run = r.get("account_jobs_last_run")
        if last_run and now - float(last_run) < 60:  # every 300 sec
            return

        # distributed lock (multi-worker safe)
        if not r.set("account_jobs_lock", 1, nx=True, ex=55):
            return
    except:
        pass

    try:
        for key in masterclass_dict:
            start_background_job(key, masterclass_dict[key])
        
                # Get today's date
            today = date.today()
            # Filter rows with expiry date less than today and delete them
            TradeBook.objects.filter(expiry__lt=today).delete()

        r.set("account_jobs_last_run", now)
    finally:
        r.delete("account_jobs_lock")

def get_contracts():
    current_folder = os.path.dirname(os.path.abspath(__file__))
    # print(current_folder)
    flag=1
    contract_files = [f for f in os.listdir(current_folder)
                        if os.path.isfile(os.path.join(current_folder, f)) and f.startswith("contracts")]
    # List all files starting with 'contracts'
    try:
        if (contract_files[0].split('_')[2].split('.')[0]==datetime.today().strftime("%d%m%Y")):
            # print('heya')
            # file_path = os.path.join(current_folder, 'contracts_NSE_'+str(datetime.today().strftime("%d%m%Y")+'.csv'))
            # nse=pd.read_csv(file_path)
            file_path = os.path.join(current_folder, 'contracts_NFO_'+str(datetime.today().strftime("%d%m%Y")+'.csv'))
            # print(file_path)
            nfo=pd.read_csv(file_path)
            return nfo
            # r.set('nse', pickle.dumps(nse))
            # r.set('nfo', pickle.dumps(nfo))
        else:
            flag=0

    except:
        flag=0
    if flag==0:
        for f in contract_files:
            file_to_delete = os.path.join(current_folder, f)
            try:
                os.remove(file_to_delete)
                # print(f"Deleted: {file_to_delete}")
            except Exception as e:
                pass
                # print(f"Error deleting {file_to_delete}: {e}")
        # print("Refreshing global data from APIs...")
        contracts={}
        # nse_contracts = json.loads(requests.get('https://masterswift.mastertrust.co.in/api/v2/contracts.json?exchanges=NSE').text)
        # contracts['NSE'] = pd.DataFrame()
        # for x in nse_contracts:
        #     # self.contracts['NSE'] = self.contracts['NSE'].append(pd.DataFrame(nse_contracts[x]),ignore_index = True)
        #     contracts['NSE'] = pd.concat([pd.DataFrame(nse_contracts[x]) for x in nse_contracts], ignore_index=True)
        #     file_path = os.path.join(current_folder, 'contracts_NSE_'+str(datetime.today().strftime("%d%m%Y")+'.csv'))
        #     contracts['NSE'].to_csv(file_path)
        #     r.set('nse', pickle.dumps(contracts['NSE']))

        nfo_contracts = json.loads(requests.get('https://masterswift.mastertrust.co.in/api/v2/contracts.json?exchanges=NFO').text)
        contracts['NFO'] = pd.DataFrame()
        for x in nfo_contracts:
            # self.contracts['NFO'] = self.contracts['NFO'].append(pd.DataFrame(nfo_contracts[x]),ignore_index = True)
            contracts['NFO'] = pd.concat([pd.DataFrame(nfo_contracts[x]) for x in nfo_contracts], ignore_index=True)
        file_path = os.path.join(current_folder, 'contracts_NFO_'+str(datetime.today().strftime("%d%m%Y")+'.csv'))
        # print(file_path)
        contracts['NFO'].to_csv(file_path)
        return contracts['NFO']
            # r.set('nfo', pickle.dumps(contracts['NFO']))

def generate_closed_pnl(account_name,symbol):
    trades = (
        TradeBook.objects
        .filter(accountId=account_name,symbol__istartswith=symbol.upper())
        .exclude(
            Q(qty=0) | Q(price=0)
        )
        .order_by('instrument', 'tradeTime')
        .values(
            'instrument',
            'tradeTime',
            'side',          # 'BUY' / 'SELL'
            'qty',
            'price'
        )
    )
    if trades.exists():
        grouped_trades = defaultdict(list)

        for trade in trades:
            key_ie = (trade['instrument'])
            grouped_trades[key_ie].append(trade)
        
        results = []

        for (instrument), trade_list in grouped_trades.items():
            buy_queue = deque()
            sell_queue = deque()

            for trade in trade_list:
                qty = trade['qty']
                price = trade['price']

                if trade['side'] == 'BUY':

                    # First match against open sells (short positions)
                    while qty > 0 and sell_queue:
                        sell = sell_queue[0]

                        matched_qty = min(qty, sell['qty'])

                        pnl = (sell['price'] - price) * matched_qty  # short pnl logic

                        results.append({
                            'instrument': instrument,
                            'pnl': pnl
                        })

                        sell['qty'] -= matched_qty
                        qty -= matched_qty

                        if sell['qty'] == 0:
                            sell_queue.popleft()

                    # Remaining qty becomes open long
                    if qty > 0:
                        buy_queue.append({
                            'qty': qty,
                            'price': price
                        })

                elif trade['side'] == 'SELL':

                    # First match against open buys (long positions)
                    while qty > 0 and buy_queue:
                        buy = buy_queue[0]

                        matched_qty = min(qty, buy['qty'])

                        pnl = (price - buy['price']) * matched_qty  # long pnl logic

                        results.append({
                            'instrument': instrument,
                            'pnl': pnl
                        })

                        buy['qty'] -= matched_qty
                        qty -= matched_qty

                        if buy['qty'] == 0:
                            buy_queue.popleft()

                    # Remaining qty becomes open short
                    if qty > 0:
                        sell_queue.append({
                            'qty': qty,
                            'price': price
                        })
        final_pnl = defaultdict(float)
        for row in results:
            key_ie = (row['instrument'])
            final_pnl[key_ie] += float(row['pnl'])
        df_final = pd.DataFrame([
            {
                'instrument': instrument,
                'total_pnl': pnl
            }
            for (instrument), pnl in final_pnl.items()
        ],columns=['instrument','total_pnl'])

        # Optional: sort nicely
        df_final = df_final.sort_values(by=['instrument']).reset_index(drop=True)
    else:
        df_final=pd.DataFrame(columns=['instrument','total_pnl'])

    return df_final

from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))

def normalize_order(order, account_key):
    kite_instruments = get_instruments_cached("NFO")
    bfo_instruments = get_instruments_cached("BFO")
    kite_instruments['exchange_token'] = kite_instruments['exchange_token'].astype(str)
    bfo_instruments['exchange_token'] = bfo_instruments['exchange_token'].astype(str)

    if 'jainam' in account_key:
        if order['ExchangeSegment'] == 'NSEFO':
            expiry = kite_instruments.loc[
                kite_instruments['exchange_token'] == str(order['ExchangeInstrumentID']), 'expiry'
            ].values[0]
        else:
            expiry = bfo_instruments.loc[
                bfo_instruments['exchange_token'] == str(order['ExchangeInstrumentID']), 'expiry'
            ].values[0]

        # Jainam sends IST string — parse, mark as IST, strip tz for PG naive column
        trade_time = (
            datetime.strptime(order["OrderGeneratedDateTime"], "%d-%m-%Y %H:%M:%S")
            .replace(tzinfo=IST)
            .replace(tzinfo=None)
        )

        return {
            "orderId":     str(order["AppOrderID"]).strip(),
            "tradeTime":   trade_time,
            "accountId":   account_key,
            "instrument":  order["ExchangeInstrumentID"],
            "symbol":      order["TradingSymbol"],
            "side":        order["OrderSide"],
            "price":       order["OrderAverageTradedPrice"],
            "qty":         order["OrderQuantity"],
            "finalPrice":  float(order["OrderAverageTradedPrice"]) * float(order["OrderQuantity"]),
            "expiry":      expiry,
        }

    else:
        # Mastertrust (non-jainam)
        if order['exchange'] == 'NFO':
            expiry = kite_instruments.loc[
                kite_instruments['exchange_token'] == str(order['instrument_token']), 'expiry'
            ].values[0]
        else:
            expiry = bfo_instruments.loc[
                bfo_instruments['exchange_token'] == str(order['instrument_token']), 'expiry'
            ].values[0]

        # Mastertrust sends epoch (UTC) — convert to IST, strip tz for PG naive column
        trade_time = (
            datetime.fromtimestamp(int(order['order_entry_time']), tz=timezone.utc)
            .astimezone(IST)
            .replace(tzinfo=None)
        )

        return {
            "orderId":     str(order["oms_order_id"]).strip(),
            "tradeTime":   trade_time,
            "accountId":   account_key,
            "instrument":  order["instrument_token"],
            "symbol":      order["trading_symbol"],
            "side":        order["order_side"],
            "price":       order["average_price"],
            "qty":         order["quantity"],
            "finalPrice":  float(order["average_price"]) * float(order["quantity"]),
            "expiry":      expiry,
        }
    
def fetch_and_insert_orders(account_key, client):
    try:
        # Safe DB handling for threads
        close_old_connections()

        # 1️⃣ Fetch orders (API call in thread)
        if 'jainam' not in account_key.lower():
            try:
                orders = client.get_orders('completed')
                orders = orders[
                    (orders["order_status"].astype(str).str.lower() == "complete") &
                    (orders["trading_symbol"].astype(str).str.upper().str.startswith(
                        ("NIFTY", "BANKNIFTY", "SENSEX")
                    ))
                ]
            except:
                orders=pd.DataFrame()
        elif 'master' in account_key.lower():
            try:
                orders_dict = client.get_order_book('NSPSK29')
                df_orders = pd.DataFrame(orders_dict["result"])
                orders = df_orders[
                    (df_orders["OrderStatus"] == "Filled") &
                    (df_orders["TradingSymbol"].astype(str).str.upper().str.startswith(
                        ("NIFTY", "BANKNIFTY", "SENSEX")
                    ))
                ].reset_index(drop=True)
            except Exception as e:
                orders=pd.DataFrame()
        
        else:
            try:
                orders_dict = client.get_order_book()
                df_orders = pd.DataFrame(orders_dict["result"])
                orders = df_orders[
                    (df_orders["OrderStatus"] == "Filled") &
                    (df_orders["TradingSymbol"].astype(str).str.upper().str.startswith(
                        ("NIFTY", "BANKNIFTY", "SENSEX")
                    ))
                ].reset_index(drop=True)
            except Exception as e:
                orders=pd.DataFrame()

        if orders.empty:
            return
    
        # 2️⃣ Normalize all rows first
        normalized_orders = []

        for _, order in orders.iterrows():
            normalized = normalize_order(order, account_key.lower())

            # IMPORTANT: normalize orderId type
            normalized["orderId"] = str(normalized["orderId"]).strip()

            normalized_orders.append(normalized)

        # Only for DB lookup
        trade_times = {order["tradeTime"] for order in normalized_orders}
        existing_trade_times = set(
            TradeBook.objects.filter(
                accountId=account_key.lower(),
                tradeTime__in=trade_times
            ).values_list("tradeTime", flat=True)
        )
        new_rows = []

        # Iterate original normalized_orders, not a deduplicated version
        for order_data in normalized_orders:
            if order_data["tradeTime"] not in existing_trade_times:
                new_rows.append(TradeBook(**order_data))

        # 6️⃣ Bulk insert
        if new_rows:
            TradeBook.objects.bulk_create(
                new_rows,
                ignore_conflicts=True,
                batch_size=1000
            )
    except:
        pass
        
def start_background_job(account_key, client):
    t = Thread(
        target=fetch_and_insert_orders,
        args=(account_key, client),
        daemon=True
    )
    t.start()

def get_ltp_or_subscribe(exchange_code, token):

    ltp_key = f"{exchange_code}_{token}"

    # 1. Try to get cached LTP
    ltp = r.get(ltp_key)
    if ltp is not None:
        return float(ltp)

    # 2. Add subscription request to Redis SET
    r.sadd("md_subscriptions", ltp_key)

    # 3. Give WS worker a short time to fetch and push LTP
    timeout=2
    start = time.time()
    while time.time() - start < timeout:
        l = r.get(ltp_key)
        if l is not None:
            return float(l)
        time.sleep(0.05)
    # 5. Still nothing → return 0
    return 0

def get_instruments_cached(exchange):
    """Fetch and cache instruments for a given exchange."""
    today=datetime.today().date()
    if exchange in instrument_cache:
        # Check if cached today
        cache_date, df = instrument_cache[exchange]
        if cache_date == today:
            return df

    kite = KiteConnect(api_key="")
    instruments = kite.instruments(exchange)
    df = pd.DataFrame(instruments)
    instrument_cache[exchange] = (today, df)
    return df

def fetch_ltp(positions):
    # response = master_connection(req.user)
    # ws_connection = response[3]
    # ltp_cache = response[4]
    result = {}

    for row in positions:
        key = f"{row['token']}_{row['exchange']}_{row['account']}"
        
        if re.search(r'B.*F.*O', row['exchange']):
            # ltp_key = f"7_{row['token']}"
            # if ltp_key in ltp_cache:
            #     result[key] = ltp_cache[ltp_key]
            # else:
            #     ws_connection.send(json.dumps({
            #         "a": "subscribe",
            #         "v": [[7, int(row['token'])]],
            #         "m": "marketdata"
            #     }))
            ltp = get_ltp_or_subscribe(7,row['token'])
            result[key] = ltp
        else:
            # ltp_key = f"2_{row['token']}"
            # if ltp_key in ltp_cache:
            #     result[key] = ltp_cache[ltp_key]
            # else:
            #     ws_connection.send(json.dumps({
            #         "a": "subscribe",
            #         "v": [[2, int(row['token'])]],
            #         "m": "marketdata"
            #     }))
            ltp = get_ltp_or_subscribe(2,row['token'])
            result[key] = ltp

    return result

def subscribe_row(exchange, token):
    """Send subscription message based on key pattern."""
    if re.search(r'B.*F.*O', exchange):
        ltp = get_ltp_or_subscribe(7,int(token))
        # subscribe_message = {
        #     "a": "subscribe",
        #     "v": [[7, int(token)]],
        #     "m": "marketdata"
        # }
        # ws_connection.send(json.dumps(subscribe_message))
    else:
        ltp = get_ltp_or_subscribe(2,int(token))
        # subscribe_message = {
        #     "a": "subscribe",
        #     "v": [[2, int(token)]],
        #     "m": "marketdata"
        # }
        # ws_connection.send(json.dumps(subscribe_message))

def get_ltp(req):
    if req.method == "POST":
        body = json.loads(req.body)
        positions = body.get("positions", [])
        result = fetch_ltp(positions)
        return JsonResponse(result)
    return HttpResponse('Get LTP')

@login_required
def cancelsl(req):
    try:
        masterclass_dict, clients, jainam_user_ids = master_connection(req.user)
        body = json.loads(req.body)
        # Extract sl_metadata from the parsed JSON data
        metadata = body.get('sl_metadata')
        data = metadata.rsplit('_', 3)
        token_and_name = data[0]
        quantity = data[1]
        exchange = data[2]
        token, account_holder = token_and_name.split('_', 1)
        price=float(data[3])
        for key in masterclass_dict:
            if "jainam" in key.lower() and key==account_holder:
                # get order book and cancel all
                user_id = jainam_user_ids[key]
                orders=masterclass_dict[key].get_order_book(user_id)
                order_book=pd.DataFrame(orders['result'])
                if not order_book.empty:
                    order_book['ExchangeInstrumentID'] = order_book['ExchangeInstrumentID'].astype(str)
                    token = str(token)
                    filtered = order_book.loc[
                        (order_book['ExchangeInstrumentID'] == token) & 
                        (order_book['OrderType'].str.upper() == 'STOPLIMIT') & 
                        (order_book['OrderStatus'].str.contains(r'Open|New|Replaced|Pending', na=False, case=False))
                    ]
                    if not filtered.empty:
                        for row in filtered.itertuples(index=False):
                            Thread(
                                target=masterclass_dict[key].cancel_order,
                                kwargs={
                                    'appOrderID':row.AppOrderID,
                                    'orderUniqueIdentifier':row.OrderUniqueIdentifier,
                                    'clientID':user_id
                                }
                            ).start()
                    else:
                        return JsonResponse({'success': True, 'message':'Orders Not Found'})
                                
                else:
                    return JsonResponse({'success': True, 'message':'Orders Not Found'})
            elif key==account_holder:
                orders=masterclass_dict[key].get_orders()
                order_book=pd.DataFrame(orders)
                if not order_book.empty:
                    order_book['instrument_token'] = order_book['instrument_token'].astype(str)
                    token = str(token)
                    filtered = order_book.loc[
                        (order_book['instrument_token'] == token) & 
                        (order_book['order_type'].str.upper() == 'SL')
                    ]
                    if not filtered.empty:
                        for row in filtered.itertuples(index=False):
                            Thread(
                                target=masterclass_dict[key].cancel_order_id, args=(row.oms_order_id,)
                            ).start()
                    else:
                        return JsonResponse({'success': True, 'message':'Orders Not Found'})
                else:
                    return JsonResponse({'success': True, 'message':'Orders Not Found'})
        return JsonResponse({'success': True, 'message':'Orders Cancelled'})                
    except:
        return JsonResponse({'success': False, 'message':'Error Occured'})                

@login_required
def placesl(req):
    try:
        # if req.session.get("logged_in"):
        # if r.get('logged_in')=='1':
        if req.method == 'POST':
            masterclass_dict, clients, jainam_user_ids = master_connection(req.user)
            t = Thread(
                target=maybe_start_account_jobs,
                args=(masterclass_dict,),
                daemon=True
            )
            t.start()
            nifty_freeze_qty = get_freeze_quantity_from_nse("NIFTY", debug=True)
            banknifty_freeze_qty = get_freeze_quantity_from_nse("BANKNIFTY", debug=True)
            try:
                if nifty_freeze_qty is None or int(nifty_freeze_qty)<0:
                    nifty_freeze_qty=1800
                if banknifty_freeze_qty is None or int(nifty_freeze_qty)<0:
                    banknifty_freeze_qty=600
            except:
                nifty_freeze_qty=1800
                banknifty_freeze_qty=600
            sensex_freeze_qty=1000
            kite_instruments = get_instruments_cached("NFO")
            bfo_instruments = get_instruments_cached("BFO")
            kite_instruments = kite_instruments[kite_instruments["segment"] == "NFO-OPT"]
            nifty_lot_size=kite_instruments[kite_instruments['name'] == 'NIFTY']['lot_size'].iloc[0]
            banknifty_lot_size=kite_instruments[kite_instruments['name'] == 'BANKNIFTY']['lot_size'].iloc[0]
            kite_instruments["expiry"] = pd.to_datetime(kite_instruments["expiry"]).dt.date
            bfo_instruments = bfo_instruments[bfo_instruments["segment"] == "BFO-OPT"]
            bfo_instruments = bfo_instruments[bfo_instruments["name"] == "SENSEX"]
            sensex_lot_size=bfo_instruments['lot_size'].iloc[0]
            
            bfo_instruments["expiry"] = pd.to_datetime(bfo_instruments["expiry"]).dt.date
            bfo_instruments = bfo_instruments[bfo_instruments["expiry"] >= date.today()]
            body = json.loads(req.body)
            # Extract sl_metadata from the parsed JSON data
            metadata = body.get('sl_metadata')
            data = metadata.rsplit('_', 3)
            token_and_name = data[0]
            quantity = data[1]
            exchange = data[2]
            token, account_holder = token_and_name.split('_', 1)
            price=float(data[3])
            # for key in masterclass_dict:
            #     if "jainam" not in key.lower():
            all_contracts=get_contracts()
                    # break
            instrument=None
            if re.search(r'B.*F.*O',exchange):
                instrument='SENSEX'
            else:
                instrument = all_contracts[all_contracts['code'].astype(str) == str(token)].iloc[0]['symbol']
                instrument = instrument.split()[0].upper()
            order_qty=[]
            order_side="BUY" if int(quantity) < 0 else "SELL"
            price=round(round(price / 0.05) * 0.05, 2)
            if order_side=='BUY':
                limit_price=price+3
                price+=1
            else:
                limit_price=price-3
                price-=1
            quantity=abs(int(quantity))
            match instrument:
                case 'BANKNIFTY':
                    while int(quantity)>0:
                        if quantity>banknifty_freeze_qty:
                            lots=math.floor(banknifty_freeze_qty/banknifty_lot_size)
                            order_qty.append(lots*banknifty_lot_size)
                            quantity-=(lots*banknifty_lot_size)
                        else:
                            lots=math.ceil(quantity/banknifty_lot_size)
                            order_qty.append(lots*banknifty_lot_size)
                            quantity-=(lots*banknifty_lot_size)
                case 'NIFTY':
                    while int(quantity)>0:
                        if quantity>nifty_freeze_qty:
                            lots=math.floor(nifty_freeze_qty/nifty_lot_size)
                            order_qty.append(lots*nifty_lot_size)
                            quantity-=(lots*nifty_lot_size)
                        else:
                            lots=math.ceil(quantity/nifty_lot_size)
                            order_qty.append(lots*nifty_lot_size)
                            quantity-=(lots*nifty_lot_size)
                case 'SENSEX':
                    while int(quantity)>0:
                        if quantity>sensex_freeze_qty:
                            lots=math.floor(sensex_freeze_qty/sensex_lot_size)
                            order_qty.append(lots*sensex_lot_size)
                            quantity-=(lots*sensex_lot_size)
                        else:
                            lots=math.ceil(quantity/sensex_lot_size)
                            order_qty.append(lots*sensex_lot_size)
                            quantity-=(lots*sensex_lot_size)
            for key in masterclass_dict:
                ## print(key,instrument,order_qty)
                if "jainam" in key.lower() and key==account_holder:
                    exchange_segment = exchange
                    exchange_token = token
                    product_type = "NRML"
                    order_type = "STOPLIMIT"
                    order_side = order_side
                    time_in_force = "DAY"
                    disclosed_qty = 0
                    limit_price = limit_price
                    stop_price = price
                    identifier = "aabbcc"
                    user_id = jainam_user_ids[key]
                    for final_order_qty in order_qty:
                        Thread(
                            target=masterclass_dict[key].place_order, 
                            kwargs={
                                "exchangeSegment": exchange_segment,
                                "exchangeInstrumentID": exchange_token,
                                "productType": product_type,
                                "orderType": order_type,
                                "orderSide": order_side,
                                "timeInForce": time_in_force,
                                "disclosedQuantity": disclosed_qty,
                                "orderQuantity": int(final_order_qty),
                                "limitPrice": limit_price,
                                "stopPrice": stop_price,
                                "orderUniqueIdentifier": identifier,
                                "clientID": user_id,
                            }
                        ).start()
                elif key==account_holder:
                    order={
                        "instrument":token,
                        "client_id": masterclass_dict[key].username,
                        "disclosed_quantity": 0,
                        "exchange": exchange,
                        "instrument_token": token,
                        "market_protection_percentage": 100,
                        "order_side": order_side,
                        "order_type": "SL",
                        "product": "NRML",
                        "quantity": int(quantity),
                        "trigger_price": price,
                        "validity": "DAY",
                        "user_order_id": "1",
                        "price": limit_price
                    }
                    # print('Master Trust Order ',order)
                    for final_order_qty in order_qty:
                        order['quantity']=int(final_order_qty)
                        # print('Master Trust ',order)
                        # b = masterclass_dict[i[0]].place_order(order1)
                        Thread(
                            target=masterclass_dict[key].place_order, args=(order,)
                        ).start()
            order_id='12345'
            return JsonResponse({'success': True, 'order_id': order_id, 'message':'Sl Placed'})
            # return redirect("/positions")
        # else:
        #     messages.info(req,'Please Login')
        #     return redirect('/login')
    except:
        return JsonResponse({'success': False, 'message':'Error Occured'})                

@login_required        
def squareoff_group(req):
    try:
    # if req.session.get("logged_in"):
        # strike = req.POST.get("strike")
        body_data = json.loads(req.POST.get("data"))
        ## print("Body Data ",body_data)
        masterclass_dict, clients, jainam_user_ids = master_connection(req.user)
        t = Thread(
                target=maybe_start_account_jobs,
                args=(masterclass_dict,),
                daemon=True
            )
        t.start()
        # response=master_connection(req.user)
        # masterclass_dict=response[0]
        # clients=response[1]
        # jainam_user_ids=response[2]
        # ws_connection=response[3]
        # ltp_cache=response[4]
        nifty_freeze_qty = get_freeze_quantity_from_nse("NIFTY", debug=True)
        banknifty_freeze_qty = get_freeze_quantity_from_nse("BANKNIFTY", debug=True)
        try:
            if nifty_freeze_qty is None or int(nifty_freeze_qty)<0:
                nifty_freeze_qty=1800
            if banknifty_freeze_qty is None or int(nifty_freeze_qty)<0:
                banknifty_freeze_qty=600
        except:
            nifty_freeze_qty=1800
            banknifty_freeze_qty=600
        sensex_freeze_qty=1000
        kite_instruments = get_instruments_cached("NFO")
        bfo_instruments = get_instruments_cached("BFO")
        kite_instruments = kite_instruments[kite_instruments["segment"] == "NFO-OPT"]
        nifty_lot_size=kite_instruments[kite_instruments['name'] == 'NIFTY']['lot_size'].iloc[0]
        banknifty_lot_size=kite_instruments[kite_instruments['name'] == 'BANKNIFTY']['lot_size'].iloc[0]
        kite_instruments["expiry"] = pd.to_datetime(kite_instruments["expiry"]).dt.date
        bfo_instruments = bfo_instruments[bfo_instruments["segment"] == "BFO-OPT"]
        bfo_instruments = bfo_instruments[bfo_instruments["name"] == "SENSEX"]
        sensex_lot_size=bfo_instruments['lot_size'].iloc[0]
        # bfo_instruments["expiry"] = pd.to_datetime(bfo_instruments["expiry"]).dt.date
        
        bfo_instruments["expiry"] = pd.to_datetime(bfo_instruments["expiry"]).dt.date
        bfo_instruments = bfo_instruments[bfo_instruments["expiry"] >= date.today()]
        # data=id.split('_')
        # token=data[0]
        # account_holder=data[1]
        # quantity=data[2]
        # exchange=data[3]
        for id in body_data:
            if 'jainam' in id.lower():
                data = id.rsplit('_', 3)
            else:
                data = id.rsplit('_', 2)
            token_and_name = data[0]
            quantity = data[1]
            exchange = data[2]
            # now split token from name (first underscore only)
            token, account_holder = token_and_name.split('_', 1)
            # for key in masterclass_dict:
            #     if "jainam" not in key.lower():
                    # all_contracts=masterclass_dict[key].allcontracts
            all_contracts=get_contracts()
                    # break
            instrument=None
            if re.search(r'B.*F.*O',exchange):
                instrument='SENSEX'
            else:
                instrument = all_contracts[all_contracts['code'].astype(str) == str(token)].iloc[0]['symbol']
                instrument = instrument.split()[0].upper()
            iterator=0
            while True:
                if re.search(r'B.*F.*O',exchange):
                    # ltp_key = f"7_{token}"
                    # if ltp_key in ltp_cache:
                    #     ltp=ltp_cache[ltp_key]
                    # else:
                    #     ws_connection.send(json.dumps({
                    #         "a": "subscribe",
                    #         "v": [[7, int(token)]],
                    #         "m": "marketdata"
                    #     }))
                    #     try:
                    #         ltp=ltp_cache[ltp_key]
                    #     except:
                    #         ltp=0
                    ltp = get_ltp_or_subscribe(7,int(token))
                else:
                    # ltp_key = f"2_{token}"
                    # if ltp_key in ltp_cache:
                    #     ltp = ltp_cache[ltp_key]
                    # else:
                    #     ws_connection.send(json.dumps({
                    #         "a": "subscribe",
                    #         "v": [[2, int(token)]],
                    #         "m": "marketdata"
                    #     }))
                    #     try:
                    #         ltp=ltp_cache[ltp_key]
                    #     except:
                    #         ltp=0
                    ltp = get_ltp_or_subscribe(2,int(token))
                if ltp!=0 or iterator>=5:
                    break
                time.sleep(1)
                # ltp_cache=return_ltp_cache()
                iterator+=1
            if ltp==0:
                ## print('Rohan',ltp_cache)
                messages.info(req,'Square Off Failed')
                return redirect('/positions')
            order_qty=[]
            order_side="BUY" if int(quantity) < 0 else "SELL"
            if order_side=='BUY' and instrument=='NIFTY':
                ltp+=7
            elif order_side=='SELL' and instrument=='NIFTY':
                ltp-=7
            elif order_side=='BUY':
                ltp+=7
            else:
                ltp-=7
            if ltp > 1300:
                messages.info(req,"Error! Price TOO HIGH Few SquareOffs Failed")
                continue
            quantity=abs(int(quantity))
            match instrument:
                case 'BANKNIFTY':
                    while int(quantity)>0:
                        if quantity>banknifty_freeze_qty:
                            lots=math.floor(banknifty_freeze_qty/banknifty_lot_size)
                            order_qty.append(lots*banknifty_lot_size)
                            quantity-=(lots*banknifty_lot_size)
                        else:
                            lots=math.ceil(quantity/banknifty_lot_size)
                            order_qty.append(lots*banknifty_lot_size)
                            quantity-=(lots*banknifty_lot_size)
                case 'NIFTY':
                    while int(quantity)>0:
                        if quantity>nifty_freeze_qty:
                            lots=math.floor(nifty_freeze_qty/nifty_lot_size)
                            order_qty.append(lots*nifty_lot_size)
                            quantity-=(lots*nifty_lot_size)
                        else:
                            lots=math.ceil(quantity/nifty_lot_size)
                            order_qty.append(lots*nifty_lot_size)
                            quantity-=(lots*nifty_lot_size)
                case 'SENSEX':
                    while int(quantity)>0:
                        if quantity>sensex_freeze_qty:
                            lots=math.floor(sensex_freeze_qty/sensex_lot_size)
                            order_qty.append(lots*sensex_lot_size)
                            quantity-=(lots*sensex_lot_size)
                        else:
                            lots=math.ceil(quantity/sensex_lot_size)
                            order_qty.append(lots*sensex_lot_size)
                            quantity-=(lots*sensex_lot_size)
            for key in masterclass_dict:
                ## print(key,instrument,order_qty)
                if "jainam" in key.lower() and key==account_holder:
                    exchange_segment = exchange
                    exchange_token = token
                    product_type = "NRML"
                    order_type = "LIMIT"
                    order_side = order_side
                    time_in_force = "DAY"
                    disclosed_qty = 0
                    limit_price = ltp
                    stop_price = 0
                    identifier = "aabbcc"
                    user_id = jainam_user_ids[key]
                    for final_order_qty in order_qty:
                        # print('Squaring of jainaim ',final_order_qty)
                        ## print('jainam',final_order_qty)
                        Thread(
                            target=masterclass_dict[key].place_order, 
                            kwargs={
                                "exchangeSegment": exchange_segment,
                                "exchangeInstrumentID": exchange_token,
                                "productType": product_type,
                                "orderType": order_type,
                                "orderSide": order_side,
                                "timeInForce": time_in_force,
                                "disclosedQuantity": disclosed_qty,
                                "orderQuantity": int(final_order_qty),
                                "limitPrice": limit_price,
                                "stopPrice": stop_price,
                                "orderUniqueIdentifier": identifier,
                                "clientID": user_id,
                            }
                        ).start()
                elif key==account_holder:
                    order={
                        "instrument":token,
                        "client_id": masterclass_dict[key].username,
                        "disclosed_quantity": 0,
                        "exchange": exchange,
                        "instrument_token": token,
                        "market_protection_percentage": 100,
                        "order_side": order_side,
                        "order_type": "LIMIT",
                        "product": "NRML",
                        "quantity": int(quantity),
                        "trigger_price": 0,
                        "validity": "DAY",
                        "user_order_id": "1",
                        "price": ltp
                    }
                    ## print('Master Trust Order ',order)
                    for final_order_qty in order_qty:
                        order['quantity']=final_order_qty    
                        # b = masterclass_dict[i[0]].place_order(order1)
                        # print('Squaring off Master Trust ',final_order_qty)
                        Thread(
                            target=masterclass_dict[key].place_order, args=(order,)
                        ).start()
        messages.info(req,'Positions Squared-off')
        return redirect("/positions")
            
    except:
        messages.info(req,'Error Occured')
        return redirect('/positions')

@login_required
def squareoff(req,id):
    try:
        # if req.session.get("logged_in"):
        # if r.get('logged_in')=='1':
            masterclass_dict, clients, jainam_user_ids = master_connection(req.user)
            t = Thread(
                target=maybe_start_account_jobs,
                args=(masterclass_dict,),
                daemon=True
            )
            t.start()
            # response=master_connection(req.user)
            # masterclass_dict=response[0]
            # clients=response[1]
            # jainam_user_ids=response[2]
            # ws_connection=response[3]
            # ltp_cache=response[4]
            nifty_freeze_qty = get_freeze_quantity_from_nse("NIFTY", debug=True)
            banknifty_freeze_qty = get_freeze_quantity_from_nse("BANKNIFTY", debug=True)
            try:
                if nifty_freeze_qty is None or int(nifty_freeze_qty)<0:
                    nifty_freeze_qty=1800
                if banknifty_freeze_qty is None or int(nifty_freeze_qty)<0:
                    banknifty_freeze_qty=600
            except:
                nifty_freeze_qty=1800
                banknifty_freeze_qty=600
            sensex_freeze_qty=1000
            kite_instruments = get_instruments_cached("NFO")
            bfo_instruments = get_instruments_cached("BFO")
            kite_instruments = kite_instruments[kite_instruments["segment"] == "NFO-OPT"]
            nifty_lot_size=kite_instruments[kite_instruments['name'] == 'NIFTY']['lot_size'].iloc[0]
            banknifty_lot_size=kite_instruments[kite_instruments['name'] == 'BANKNIFTY']['lot_size'].iloc[0]
            kite_instruments["expiry"] = pd.to_datetime(kite_instruments["expiry"]).dt.date
            bfo_instruments = bfo_instruments[bfo_instruments["segment"] == "BFO-OPT"]
            bfo_instruments = bfo_instruments[bfo_instruments["name"] == "SENSEX"]
            sensex_lot_size=bfo_instruments['lot_size'].iloc[0]
            # bfo_instruments["expiry"] = pd.to_datetime(bfo_instruments["expiry"]).dt.date
            
            bfo_instruments["expiry"] = pd.to_datetime(bfo_instruments["expiry"]).dt.date
            bfo_instruments = bfo_instruments[bfo_instruments["expiry"] >= date.today()]
            # data=id.split('_')
            # token=data[0]
            # account_holder=data[1]
            # quantity=data[2]
            # exchange=data[3]
            if 'jainam' in id.lower():
                data = id.rsplit('_', 3)
            else:
                data = id.rsplit('_', 2)
            token_and_name = data[0]
            quantity = data[1]
            exchange = data[2]
            # now split token from name (first underscore only)
            token, account_holder = token_and_name.split('_', 1)
            # for key in masterclass_dict:
            #     if "jainam" not in key.lower():
            all_contracts=get_contracts()
                    # break
            instrument=None
            if re.search(r'B.*F.*O',exchange):
                instrument='SENSEX'
            else:
                instrument = all_contracts[all_contracts['code'].astype(str) == str(token)].iloc[0]['symbol']
                instrument = instrument.split()[0].upper()
            iterator=0
            while True:
                if re.search(r'B.*F.*O',exchange):
                    # ltp_key = f"7_{token}"
                    # if ltp_key in ltp_cache:
                    #     ltp=ltp_cache[ltp_key]
                    # else:
                    #     ws_connection.send(json.dumps({
                    #         "a": "subscribe",
                    #         "v": [[7, int(token)]],
                    #         "m": "marketdata"
                    #     }))
                    #     try:
                    #         ltp=ltp_cache[ltp_key]
                    #     except:
                    #         ltp=0
                    ltp = get_ltp_or_subscribe(7,int(token))
                else:
                    # ltp_key = f"2_{token}"
                    # if ltp_key in ltp_cache:
                    #     ltp = ltp_cache[ltp_key]
                    # else:
                    #     ws_connection.send(json.dumps({
                    #         "a": "subscribe",
                    #         "v": [[2, int(token)]],
                    #         "m": "marketdata"
                    #     }))
                    #     try:
                    #         ltp=ltp_cache[ltp_key]
                    #     except:
                    #         ltp=0
                    ltp = get_ltp_or_subscribe(2,int(token))
                if ltp!=0 or iterator>=5:
                    break
                time.sleep(1)
                # ltp_cache=return_ltp_cache()
                iterator+=1
            if ltp==0:
                ## print('Rohan',ltp_cache)
                messages.info(req,'Square Off Failed')
                return redirect('/positions')
            order_qty=[]
            order_side="BUY" if int(quantity) < 0 else "SELL"
            if order_side=='BUY' and instrument=='NIFTY':
                ltp+=7
            elif order_side=='SELL' and instrument=='NIFTY':
                ltp-=7
            elif order_side=='BUY':
                ltp+=7
            else:
                ltp-=7
            if ltp > 1300:
                messages.info(req,"Error! Price TOO HIGH")
                return redirect('/positions')
            quantity=abs(int(quantity))
            match instrument:
                case 'BANKNIFTY':
                    while int(quantity)>0:
                        if quantity>banknifty_freeze_qty:
                            lots=math.floor(banknifty_freeze_qty/banknifty_lot_size)
                            order_qty.append(lots*banknifty_lot_size)
                            quantity-=(lots*banknifty_lot_size)
                        else:
                            lots=math.ceil(quantity/banknifty_lot_size)
                            order_qty.append(lots*banknifty_lot_size)
                            quantity-=(lots*banknifty_lot_size)
                case 'NIFTY':
                    while int(quantity)>0:
                        if quantity>nifty_freeze_qty:
                            lots=math.floor(nifty_freeze_qty/nifty_lot_size)
                            order_qty.append(lots*nifty_lot_size)
                            quantity-=(lots*nifty_lot_size)
                        else:
                            lots=math.ceil(quantity/nifty_lot_size)
                            order_qty.append(lots*nifty_lot_size)
                            quantity-=(lots*nifty_lot_size)
                case 'SENSEX':
                    while int(quantity)>0:
                        if quantity>sensex_freeze_qty:
                            lots=math.floor(sensex_freeze_qty/sensex_lot_size)
                            order_qty.append(lots*sensex_lot_size)
                            quantity-=(lots*sensex_lot_size)
                        else:
                            lots=math.ceil(quantity/sensex_lot_size)
                            order_qty.append(lots*sensex_lot_size)
                            quantity-=(lots*sensex_lot_size)
            for key in masterclass_dict:
                ## print(key,instrument,order_qty)
                if "jainam" in key.lower() and key==account_holder:
                    exchange_segment = exchange
                    exchange_token = token
                    product_type = "NRML"
                    order_type = "LIMIT"
                    order_side = order_side
                    time_in_force = "DAY"
                    disclosed_qty = 0
                    limit_price = ltp
                    stop_price = 0
                    identifier = "aabbcc"
                    user_id = jainam_user_ids[key]
                    for final_order_qty in order_qty:
                        # print('jainam',final_order_qty)
                        Thread(
                            target=masterclass_dict[key].place_order, 
                            kwargs={
                                "exchangeSegment": exchange_segment,
                                "exchangeInstrumentID": exchange_token,
                                "productType": product_type,
                                "orderType": order_type,
                                "orderSide": order_side,
                                "timeInForce": time_in_force,
                                "disclosedQuantity": disclosed_qty,
                                "orderQuantity": int(final_order_qty),
                                "limitPrice": limit_price,
                                "stopPrice": stop_price,
                                "orderUniqueIdentifier": identifier,
                                "clientID": user_id,
                            }
                        ).start()
                elif key==account_holder:
                    order={
                        "instrument":token,
                        "client_id": masterclass_dict[key].username,
                        "disclosed_quantity": 0,
                        "exchange": exchange,
                        "instrument_token": token,
                        "market_protection_percentage": 100,
                        "order_side": order_side,
                        "order_type": "LIMIT",
                        "product": "NRML",
                        "quantity": int(quantity),
                        "trigger_price": 0,
                        "validity": "DAY",
                        "user_order_id": "1",
                        "price": ltp
                    }
                    ## print('Master Trust Order ',order)
                    for final_order_qty in order_qty:
                        order['quantity']=int(final_order_qty)
                        # print('Master Trust ',order)
                        # b = masterclass_dict[i[0]].place_order(order1)
                        Thread(
                            target=masterclass_dict[key].place_order, args=(order,)
                        ).start()
            messages.info(req,'Position Squared-off')
            return redirect("/positions")
        # else:
        #     messages.info(req,'Please Login')
        #     return redirect('/login')
    except:
        messages.info(req,'Error Occured')
        return redirect('/positions')

@login_required
def squareoff_calls(req):
    pass

@login_required
def squareoff_puts(req):
    pass

def find_expiry(nfo,token,instrument):
    if instrument=='SENSEX':
        nfo = nfo[nfo["exchange_token"].astype(str) == str(token)]
        return nfo.iloc[0]["expiry"].strftime("%d-%m-%Y")
    else:
        nfo = nfo[nfo["code"].astype(str).str.strip() == str(token).strip()]
        return dt.datetime.fromtimestamp(nfo.iloc[0]["expiry"]).strftime("%d-%m-%Y")

@login_required
def pnl(req): 
    try:
            global positions_data
        # if req.session.get("logged_in"):
        # if r.get('logged_in')=='1':
            if not req.headers.get('x-requested-with') == 'XMLHttpRequest':
                bfo_instruments = get_instruments_cached("BFO")
                bfo_instruments = pd.DataFrame(bfo_instruments)
                bfo_instruments = bfo_instruments[bfo_instruments["segment"] == "BFO-OPT"]
                bfo_instruments = bfo_instruments[bfo_instruments["name"] == "SENSEX"]
                masterclass_dict, clients, jainam_user_ids = master_connection(req.user)
                t = Thread(
                target=maybe_start_account_jobs,
                args=(masterclass_dict,),
                daemon=True
                )
                t.start()
                # response=master_connection(req.user)
                # masterclass_dict=response[0]
                # jainam_user_ids=response[2]
                # ws_connection=response[3]
                pnl=pd.DataFrame(
                    columns=['Account','Instrument','Expiry','PNL']
                )
                accounts=[]
                for key in masterclass_dict:
                    if 'jainam' in key.lower():
                        user_id = jainam_user_ids[key]
                        iterator=0
                        while iterator<2:
                            try:
                                positions = masterclass_dict[key].get_position_netwise(user_id)["result"][
                                    "positionList"
                                ]
                                iterator=0
                                break
                            except:
                                iterator+=1

                        ## print(f"{key} positions: {positions}")
                        for pos1 in positions:
                            if int(pos1["Quantity"]) == 0:
                                continue
                            # ## print(pos1)
                            
                            instrument = pos1["TradingSymbol"].split(" ")[0]
                            try:
                                expiry = pos1["TradingSymbol"].split(" ")[1]
                                parsed_date = datetime.strptime(expiry, "%d%b%Y")
                                expiry = parsed_date.strftime("%d-%m-%Y")
                            except Exception as e:
                                ## print(e)
                                expiry = date.today().strftime("%d-%m-%Y")
                            if re.search(r'B.*F.*O', pos1['ExchangeSegment']):
                                exchange='BFO'
                            else:
                                exchange='NFO'

                            if pos1['OpenSellQuantity'] != 0:
                                quantity = pos1['OpenSellQuantity']
                                price = pos1['SellAveragePrice']
                                side = 'SELL'
                            else:
                                quantity = pos1['OpenBuyQuantity']
                                price = pos1['BuyAveragePrice']
                                side = 'BUY'
                            t = Thread(target=subscribe_row, args=(exchange, pos1['ExchangeInstrumentId']))
                            t.start()
                            # positions_data.append(key,)
                            accounts.append((key, instrument, expiry,  side, abs(float(quantity)), abs(float(price)),pos1['ExchangeInstrumentId'],exchange))
                        continue
                    # [['trading_symbol','net_quantity','average_sell_price','average_buy_price','ltp']]
                    iterator=0
                    while iterator<2:
                        try:
                            df = masterclass_dict[key].get_positions()
                            trades=masterclass_dict[key].get_trades()
                            iterator=0
                            break
                        except:
                            iterator+=1
                    ## print(df)
                    if not df.empty:
                        for index, row in df.iterrows():
                            instrument = row["symbol"]
                            
                            # Determine which instrument list to use
                            if instrument == 'SENSEX':
                                nfo = bfo_instruments
                            else:
                                # nfo = masterclass_dict[key].contracts["NFO"]
                                nfo=get_contracts()
                            
                            # Find the expiry using your custom function
                            expiry = find_expiry(nfo, row["instrument_token"], instrument)
                            
                            try:
                                filtered_trades = trades[trades['trading_symbol'] == row["trading_symbol"]]
                            
                                # total_value = (filtered_trades['trade_quantity'] * filtered_trades['trade_price']).sum()
                                average_trade_price = filtered_trades['trade_price'].mean()

                                if row['net_quantity']>0:
                                    quantity = abs(int(row['net_quantity']))
                                    price = average_trade_price
                                    side = 'BUY'
                                else:
                                    quantity = abs(int(row['net_quantity']))
                                    price = average_trade_price
                                    side = 'SELL'
                            except:
                                # Determine trade direction (Buy or Sell) and extract quantity and price
                                if row['cf_sell_quantity'] != 0:
                                    quantity = row['cf_sell_quantity']
                                    price = row['actual_average_sell_price']
                                    side = 'SELL'
                                else:
                                    quantity = row['cf_buy_quantity']
                                    price = row['actual_average_buy_price']
                                    side = 'BUY'

                            if re.search(r'B.*F.*O', row['exchange']):
                                exchange='BFO'
                            else:
                                exchange='NFO'
                            # positions_data.append(key,)
                            # Append to the accounts list
                            t = Thread(target=subscribe_row, args=(exchange, row['instrument_token']))
                            t.start()
                            accounts.append((key, instrument, expiry, side, abs(float(quantity)), abs(float(price)),row['instrument_token'],exchange))
                
                positions_data.extend(accounts)
            
            # Calculate PNL
            # Create a dictionary to hold the grouped totals
            # Construct the payload
            payload = []
            for account, instrument, expiry, side, qty, price, token, exchange in positions_data:
                payload.append({
                    "token": token,
                    "exchange": exchange,
                    "account": account
                })

            ltp_result = fetch_ltp(payload)

            # Now compute P&L using ltp_result
            grouped_pnl = defaultdict(float)

            for account, instrument, expiry, side, qty, price, token, exchange in positions_data:
                ltp_key = f"{token}_{exchange}_{account}"
                ltp = ltp_result.get(ltp_key)
                if ltp is None:
                    pnl=0
                else:
                    pnl = (ltp - price) * qty if side == 'BUY' else (price - ltp) * qty
                key = (account, instrument, expiry)
                grouped_pnl[key] += pnl
            
            accounts = defaultdict(list)
            for (account, instrument, expiry), open_pnl in grouped_pnl.items():
                closed_pnl = 0.0
                total_pnl = open_pnl + closed_pnl
                accounts[account].append({
                    'Instrument': instrument,
                    'Expiry': expiry,
                    'Open PNL': round(open_pnl, 2),
                    'Closed PNL': round(closed_pnl, 2),
                    'Total PNL': round(total_pnl, 2)
                })

            rows = []
            for account, data in accounts.items():
                df = pd.DataFrame(data)
                df = df[['Instrument', 'Expiry', 'Open PNL', 'Closed PNL', 'Total PNL']]
                table_html = df.to_html(index=False, classes='table table-bordered table-sm', border=0)
                rows.append((account, table_html))

            # AJAX call — return only cards
            if req.headers.get("x-requested-with") == "XMLHttpRequest":
                return render(req, "partials_pnl.html", {"rows": rows})

            # Normal page load
            return render(req, "pnl.html", {"rows": rows})
            # ## print(grouped_pnl)
            # return HttpResponse('PNL')
        # else:
        #     messages.info(req,'Please Login')
        #     return redirect('/login')
    except:
        messages.info(req,'Error Occured')
        return redirect('/home')

@login_required
def positions(req):
    try:
        # if req.session.get("logged_in"):
        # if r.get('logged_in')=='1':
            csrf_token = get_token(req) 
            bfo_instruments = get_instruments_cached("BFO")
            bfo_instruments = bfo_instruments[bfo_instruments["segment"] == "BFO-OPT"]
            bfo_instruments = bfo_instruments[bfo_instruments["name"] == "SENSEX"]
            masterclass_dict, clients, jainam_user_ids = master_connection(req.user)
            t = Thread(
                target=maybe_start_account_jobs,
                args=(masterclass_dict,),
                daemon=True
            )
            t.start()
            client_list = []
            arr = []
            xts_positions = {}
            threads = []
            master_dfs={}
            grouped = defaultdict(list)
            grouped_instrument = defaultdict(list)
            for key in masterclass_dict:  
                # print(key)  
                # client_list.append(key)
                if 'jainam' in key.lower():
                    # print(key)
                    user_id = jainam_user_ids[key]
                    iterator=0
                    positions=[]
                    while iterator<2:
                        try:
                            positions = masterclass_dict[key].get_position_netwise(user_id)["result"]["positionList"]
                            try:
                                orders=masterclass_dict[key].get_order_book(user_id)
                                order_book=pd.DataFrame(orders['result'])
                            except:
                                order_book=pd.DataFrame()
                            iterator=3
                        except:
                            iterator+=1
                    df_position=pd.DataFrame(positions)
                    data = pd.DataFrame(
                        columns=[
                            "Instrument",
                            "Expiry",
                            "Strike",
                            "Type",
                            "Quantity",
                            "LTP",
                            "Token",
                            "Exchange",
                            "PNL",
                            "Quantitys",
                            "Price",
                            "Side",
                            "Squareoff",
                            "PlaceSL"
                        ]
                    )
                    if not df_position.empty and 'Quantity' in df_position.columns:
                        qty_numeric = pd.to_numeric(df_position['Quantity'], errors='coerce')
                        df_position = df_position[qty_numeric != 0]
                    if (not df_position.empty and 'TradingSymbol' in df_position.columns and df_position['TradingSymbol'].astype(str).str.contains(r'\b(SENSEX|NIFTY|BANKNIFTY)\b', case=False, na=False).any()):
                        # print('inside position',key)
                        df_position = df_position[
                            df_position['TradingSymbol'].str.contains(
                                r'\b(SENSEX|NIFTY|BANKNIFTY)\b',
                                case=False,
                                na=False
                            )
                        ]
                        # df_tradebook=generate_closed_pnl(key.lower())
                        df_position['ExchangeInstrumentId'] = df_position['ExchangeInstrumentId'].astype(int)
                        # df_tradebook['instrument'] = df_tradebook['instrument'].astype(int)
                        # df = pd.merge(
                        #     df_position,
                        #     df_tradebook,
                        #     left_on=['ExchangeInstrumentId'],
                        #     right_on=['instrument'],
                        #     how='left'
                        # )
                        df_position.fillna(0, inplace=True)
                        ## print(f"{key} positions: {positions}")
                        pos_data=df_position.to_dict(orient='records')
                        for pos1 in pos_data:
                            # if int(pos1["Quantity"]) == 0:
                            #     xts_positions[key] = data
                            #     continue
                            pos = {}
                            # ## print(pos1)
                            
                            pos["Instrument"] = pos1["TradingSymbol"].split(" ")[0]
                            try:
                                pos["Expiry"] = datetime.strptime(
                                    pos1["TradingSymbol"].split(" ")[1], "%d%b%Y"
                                ).strftime("%d-%m-%Y")
                            except Exception as e:
                                ## print(e)
                                pos["Expiry"] = 0
                            try:
                                pos["Strike"] = pos1["TradingSymbol"].split(" ")[3]
                                pos["Type"] = pos1["TradingSymbol"].split(" ")[2]
                            except Exception as e:
                                pos['Strike'] = 0
                                pos["Type"] = 'F'
                            
                            pos["Quantity"] = pos1["Quantity"]
                            pos["LTP"] = 0
                            pos["Token"]=pos1["ExchangeInstrumentId"]
                            pos["Exchange"]=pos1["ExchangeSegment"]
                            exchange=pos1["ExchangeSegment"]
                            token=pos1['ExchangeInstrumentId']
                            if re.search(r'B.*F.*O', exchange):
                                t = Thread(
                                    target=get_ltp_or_subscribe,
                                    args=(7,token),
                                    daemon=True
                                )
                                t.start()
                            else:
                                t = Thread(
                                    target=get_ltp_or_subscribe,
                                    args=(2,token),
                                    daemon=True
                                )
                                t.start()
                            ltp=0
                            if float(pos1['Quantity'])< 0:
                                quantity = abs(float(pos1['Quantity']))
                                price = pos1['ActualSellAveragePrice']
                                side = 'SELL'
                            else:
                                quantity = abs(float(pos1['Quantity']))
                                price = pos1['ActualBuyAveragePrice']
                                side = 'BUY'
                            pos['PNL']=round((float(ltp) - float(price)) * float(quantity) if side == 'BUY' else (float(price) - float(ltp)) * float(quantity),2)
                            # pos['ClosedPNL']=float(pos1['total_pnl'])
                            pos['Quantitys']=quantity
                            pos['Price']=price
                            pos['Side']=side
                            squareoff_id = f"{pos1['ExchangeInstrumentId']}_{key}_{pos1['Quantity']}_{pos1['ExchangeSegment']}_{price}"
                            group_key = (
                                str(pos["Strike"]),
                                str(pos["Type"]),
                                str(pos["Expiry"])
                            )
                            grouped[group_key].append(squareoff_id)
                            group_key = (
                                str(pos["Instrument"]),
                                str(key)
                            )
                            grouped_instrument[group_key].append(squareoff_id)
                            url = reverse('squareoff', kwargs={'id': squareoff_id})
                            pos["Squareoff"] = (
                                f'<form action="{url}" method="POST" style="display:inline;">'
                                f'<input type="hidden" name="csrfmiddlewaretoken" value="{csrf_token}"/>'
                                '<button type="submit" class="squareoff-btn">Sq Off Acc</button>'
                                '</form>'
                            )
                            if not order_book.empty:
                                try:
                                    order_book['ExchangeInstrumentID'] = order_book['ExchangeInstrumentID'].astype(str)
                                    filtered = order_book.loc[
                                        (order_book['ExchangeInstrumentID'] == str(pos1["ExchangeInstrumentId"])) & 
                                        (order_book['OrderType'].str.upper() == 'STOPLIMIT') & 
                                        (order_book['OrderStatus'].str.contains(r'Open|New|Replaced|Pending', na=False, case=False))
                                    ]

                                    app_order_id = filtered['AppOrderID'].iloc[0] if not filtered.empty else ''
                                except:
                                    app_order_id=''
                            else:
                                app_order_id=''
                            pos['PlaceSL'] = (
                                f'<div class="sl-toggle-container" data-sl-metadata="{squareoff_id}">'
                                f'<input type="hidden" class="order-id" value="{app_order_id}">'
                                f'<button class="sl-toggle-btn">'
                                f'{"Cancel SL" if app_order_id else "Place SL"}'
                                f'</button>'
                                f'</div>'
                            )
                            data.loc[len(data)] = list(pos.values())
                            data = data.sort_values(
                                by=["Instrument", "Expiry", "Type", "Strike"],
                                ascending=[True, True, False, True],
                            )
                    ## print('jainam Data: ',data)
                    xts_positions[key] = data
                    continue
                iterator=0
                df_position=pd.DataFrame()
                # print(key)
                while iterator<2:
                    try:
                        df_position = masterclass_dict[key].get_positions()
                        try:
                            orders=masterclass_dict[key].get_orders()
                            order_book=pd.DataFrame(orders)
                        except:
                            order_book=pd.DataFrame()
                        # trades=masterclass_dict[key].get_trades()
                        ## print(df)
                        iterator=0
                        break
                    except:
                        iterator+=1
                pos = pd.DataFrame(
                    columns=[
                        "Instrument",
                        "Expiry",
                        "Strike",
                        "Type",
                        "Quantity",
                        "LTP",
                        "Token",
                        "Exchange",
                        "PNL",
                        "Quantitys",
                        "Price",
                        "Side"
                    ]
                )
                if (not df_position.empty and 'trading_symbol' in df_position.columns and df_position['trading_symbol'].astype(str).str.startswith(('SENSEX', 'NIFTY', 'BANKNIFTY')).any()):
                    # print('inside position',key)
                    df_position = df_position[df_position['trading_symbol'].str.startswith(('SENSEX', 'NIFTY', 'BANKNIFTY'))]
                    # df_tradebook=generate_closed_pnl(key.lower())
                    df_position['instrument_token'] = df_position['instrument_token'].astype(int)
                    # df_tradebook['instrument'] = df_tradebook['instrument'].astype(int)
                    # df = pd.merge(
                    #     df_position,
                    #     df_tradebook,
                    #     left_on=['instrument_token'],
                    #     right_on=['instrument'],
                    #     how='left'
                    # )
                    df_position.fillna(0, inplace=True)
                    # client_list.append(key)
                    for index, row in df_position.iterrows():
                        instrument = row["symbol"]
                        # try:
                        if instrument=='SENSEX':
                            nfo=bfo_instruments
                        else:
                            # nfo = masterclass_dict[key].contracts["NFO"]
                            nfo=get_contracts()
                        expiry = find_expiry(nfo,row["instrument_token"],instrument)
                        ## print(expiry)
                        # except:
                        #     expiry=datetime.today().date()
                        # expiry = dt.datetime.strptime(expiry, "%d-%m-%Y")
                        type_ = row["trading_symbol"][-2:]
                        qty = row["net_quantity"]
                        strike = row["trading_symbol"].split(instrument)[1].split(type_)[0][-5:]
                        sell_price = row["average_sell_price"]
                        buy_price = row["average_buy_price"]
                        ltp = row["ltp"]
                        token = str(row["instrument_token"])
                        exchange=str(row['exchange'])
                        if re.search(r'B.*F.*O', exchange):
                            t = Thread(
                                target=get_ltp_or_subscribe,
                                args=(7,int(token)),
                                daemon=True
                            )
                            t.start()
                        else:
                            t = Thread(
                                target=get_ltp_or_subscribe,
                                args=(2,int(token)),
                                daemon=True
                            )
                            t.start()
                        ltp=0
                        if float(row['cf_sell_quantity'])>0 and float(row['net_quantity'])<0:
                            quantity = float(abs(row['net_quantity']))
                            price = float(row['actual_average_sell_price'])
                            side = 'SELL'
                        elif float(row['sell_quantity'])>0 and float(row['net_quantity'])<0:
                            quantity = float(abs(row['net_quantity']))
                            price = float(row['average_sell_price'])
                            side = 'SELL'
                        elif float(row['cf_buy_quantity'])>0 and float(row['net_quantity'])>0:
                            quantity = float(abs(row['net_quantity']))
                            price = float(row['actual_average_buy_price'])
                            side = 'BUY'
                        else:
                            quantity = float(abs(row['net_quantity']))
                            price = float(row['average_buy_price'])
                            side = 'BUY'

                        pnl=round((float(ltp) - float(price)) * float(quantity)  if side == 'BUY' else (float(price) - (float(ltp))) * float(quantity),2)
                        # closed_pnl=round(row['total_pnl'],2)
                        pos.loc[len(pos)] = [instrument, expiry, strike, type_, qty, ltp, token, exchange, pnl, quantity, price, side]

                    # df['Token'] = df['Token'].apply(str)
                    df = pos.copy()
                    df = df.sort_values(
                        by=["Instrument", "Expiry", "Type", "Strike"],
                        ascending=[True, True, False, True],
                    )
                    df = df[df["Quantity"] != 0]
                    ## print("***************")
                    ## print(df)
                    ## print("****************")
                    df["url"] = df.apply(
                        lambda row: reverse("squareoff",kwargs={"id": f"{row['Token']}_{key}_{row['Quantity']}_{row['Exchange']}"},),
                        axis=1
                    )
                    df["rollover_url"] = "https://goddseye.ngrok.io/rollover" + df["Token"]
                    df["Squareoff"] = df.apply(
                        lambda row: (
                            f'<form action="{row["url"]}" method="POST" style="display:inline;">'
                            f'<input type="hidden" name="csrfmiddlewaretoken" value="{csrf_token}"/>'
                            '<button type="submit" class="squareoff-btn">Sq Off Acc</button>'
                            '</form>'
                        ),
                        axis=1
                    )
                    df["Strike"] = df["Strike"].astype(str)
                    df["Type"] = df["Type"].astype(str)
                    df["Expiry"] = df["Expiry"].astype(str)

                    new_grouped = (
                        df.groupby(["Strike", "Type", "Expiry"], group_keys=False)
                        .apply(lambda g: [
                            f"{row.Token}_{key}_{row.Quantity}_{row.Exchange}"
                            for row in g.itertuples(index=False)
                        ])
                        .to_dict()
                    )
                    grouped = defaultdict(
                        list,
                        {
                            k: v for k, v in grouped.items()
                            if isinstance(v, list)
                        }
                    )
                    for group_key, items in new_grouped.items():
                        if group_key in grouped:
                            grouped[group_key].extend(items)
                        else:
                            grouped[group_key] = items

                    df["Instrument"] = df["Instrument"].astype(str)
                    new_grouped = (
                        df.groupby(["Instrument"])
                        .apply(lambda g: [
                            f"{row.Token}_{key}_{row.Quantity}_{row.Exchange}"
                            for row in g.itertuples(index=False)
                        ])
                        .to_dict()
                    )
                    for instrument, items in new_grouped.items():
                        group_key = (str(instrument), str(key))
                        if group_key in grouped_instrument:
                            grouped_instrument[group_key].extend(items)
                        else:
                            grouped_instrument[group_key] = items
                    df = df.drop(["url","rollover_url"], axis=1)

                    df['__row_attr__'] = (
                        'data-token="' + df['Token'].astype(str) + '" '
                        'data-exchange="' + df['Exchange'].astype(str) + '" '
                        'data-account="' + key + '" '+
                        'data-instrument="' + df['Instrument'].astype(str) + '" ' +
                        'data-price="' + df['Price'].astype(str) + '" '
                        'data-quantity="' + df['Quantitys'].astype(str) + '" '
                        'data-side="' + df['Side'].astype(str) + '"'
                    )
                    df = df.drop(columns=['Quantitys','Side'])
                    # Mark LTP column cell for live update
                    df['_PNL_NUM'] = pd.to_numeric(df['PNL'], errors='coerce')
                    df['LTP'] = '<span class="ltp-value">' + df['LTP'].astype(str) + '</span>'
                    df['PNL']='<span class="pnl-value">' + df['PNL'].astype(str) + '</span>'

                    if not order_book.empty:
                        try:
                            sl_map = (
                                order_book[order_book['order_type'].str.upper() == 'SL']
                                .drop_duplicates(subset='instrument_token', keep='first')
                                .set_index('instrument_token')['oms_order_id']
                            )

                            sl_map.index = sl_map.index.astype(str)
                            df['Token'] = df['Token'].astype(str)
                            df['orderid'] = df['Token'].map(sl_map)
                        except:
                            df['orderid']=None
                    else:
                        df['orderid']=None
                    df['PlaceSL'] = df.apply(
                        lambda row: (
                            f'<div class="sl-toggle-container" '
                            f'data-sl-metadata="{row["Token"]}_{key}_{row["Quantity"]}_{row["Exchange"]}_{row["Price"]}">'
                            f'<input type="hidden" class="order-id" value="{row["orderid"] if pd.notna(row["orderid"]) else ""}">'
                            f'<button class="sl-toggle-btn">'
                            f'{"Cancel SL" if pd.notna(row["orderid"]) else "Place SL"}'
                            f'</button>'
                            f'</div>'
                        ),
                        axis=1
                    )

                    for index, row in df.iterrows():
                        exchange = row['Exchange']  # or whatever column holds the key
                        token = row['Token']
                        t = Thread(target=subscribe_row, args=(exchange, token))
                        t.start()
                        threads.append(t)

                    # Hide Token and Exchange from display
                    df = df.drop(columns=['Token', 'Exchange','Price', 'orderid'])

                    # 4) IMPORTANT: Drop __row_attr__ from display BEFORE to_html
                    # row_attrs = df['__row_attr__'].tolist()  # save separately
                    # df = df.drop(columns=['__row_attr__'])

                    # master_dfs.append(df)
                    master_dfs[key]=df
                else:
                    master_dfs[key]=pos
            ## print('Group ',grouped)
            # print('MasterTrust',master_dfs.keys())
            url_strike=reverse('squareoff_group')
            for key in master_dfs:
                df = master_dfs[key].copy()
                if not df.empty:
                    def make_squareoff_form(row):
                        strike = str(row.Strike)
                        op_type = str(row.Type)
                        expiry = str(row.Expiry)
                        group_key = (strike, op_type, expiry)
                        if group_key in grouped:
                            body_json = json.dumps(grouped[group_key])
                            return (
                                f'<form action="{url_strike}" method="POST" style="display:inline;">'
                                f'<input type="hidden" name="csrfmiddlewaretoken" value="{csrf_token}"/>'
                                f'<input type="hidden" name="strike" value="{strike}"/>'
                                f'<input type="hidden" name="data" value=\'{body_json}\'/>'
                                '<button type="submit" class="squareoff-btn">Sq Off All Acc</button>'
                                '</form>'
                            )
                        return ""
                    df["SquareOff Strike"] = df.apply(make_squareoff_form, axis=1)
                    def make_squareoff_form_instrument(row,key):
                        instrument = str(row.Instrument)
                        key = str(key)
                        group_key = (instrument, key)
                        if group_key in grouped_instrument:
                            body_json = json.dumps(grouped_instrument[group_key])
                            return (
                                f'<form action="{url_strike}" method="POST" style="display:inline;">'
                                f'<input type="hidden" name="csrfmiddlewaretoken" value="{csrf_token}"/>'
                                f'<input type="hidden" name="strike" value="{strike}"/>'
                                f'<input type="hidden" name="data" value=\'{body_json}\'/>'
                                '<button type="submit" class="squareoff-btn">Position Zero</button>'
                                '</form>'
                            )
                        return ""
                    df["SquareOff Curr Acct"] = df.apply(
                        lambda row: make_squareoff_form_instrument(row, key),
                        axis=1
                    )
                    sort_order = {'CE': 1, 'PE': 0}  # custom sort order for Type
                    df['Type_order'] = df['Type'].map(sort_order)
                    df = df.sort_values(by=['Instrument','Type_order','Expiry','Strike']).drop(columns='Type_order')
                    final_rows = []
                    numeric_cols = ['_PNL_NUM']

                    for instrument, inst_df in df.groupby('Instrument', sort=False):
                        final_rows.append(inst_df)
                        closed_df=generate_closed_pnl(key.lower(),instrument)
                        total_closed_pnl = closed_df['total_pnl'].sum()
                        summary = inst_df[numeric_cols].sum()

                        summary_row = {col: '' for col in df.columns}
                        summary_row['Instrument'] = f'{instrument} TOTAL'
                        initial_pnl = round(summary['_PNL_NUM'], 2)
                        overall_pnl = round(float(initial_pnl) + float(total_closed_pnl), 2)
                        summary_row['PNL'] = (
                            f'<span class="pnl-total" data-value="{initial_pnl}">'
                            f'{initial_pnl}'
                            f'</span>'
                        )
                        # summary_row['ClosedPNL'] = 
                        summary_row['Squareoff'] = (
                            f'<div class="closed-pnl" style="text-align:center;">'
                            f'Closed PNL: {round(total_closed_pnl, 2)}'
                            f'</div>'
                        )
                        summary_row['PlaceSL'] = ''
                        summary_row['SquareOff Strike'] = (
                            f'<div class="overall-pnl" style="text-align:center;">'
                            f'Overall PNL: {overall_pnl}'
                            f'</div>'
                        )
                        summary_row["SquareOff Curr Acct"] = ''
                        summary_row['__row_attr__'] = (f'class="instrument-total" data-instrument="{instrument}" data-account="{key}"')
                        final_rows.append(pd.DataFrame([summary_row]))

                    if final_rows:
                        df = pd.concat(final_rows, ignore_index=True)
                    else:
                        df = pd.DataFrame(columns=df.columns)
                    df = df.drop(columns=['_PNL_NUM'])

                    row_attrs = df['__row_attr__'].tolist()  # save separately
                    df = df.drop(columns=['__row_attr__'])

                    # 5) Now generate HTML
                    html = df.to_html(classes="data", escape=False, index=False)

                    # 6) Inject data attributes to each <tr>
                    lines = html.splitlines()
                    final_html = []
                    row_iter = iter(row_attrs)
                    for line in lines:
                        if line.strip().startswith('<tr>'):
                            attrs = next(row_iter)
                            final_html.append(line.replace('<tr>', f'<tr {attrs}>'))
                        else:
                            final_html.append(line)

                    html = '\n'.join(final_html)                
                else:
                    # Create empty table structure
                    html = df.to_html(classes="data", escape=False, index=False)

                    # Replace empty tbody with single "No Positions" row
                    colspan = len(df.columns)

                    html = re.sub(
                        r'<tbody>\s*</tbody>',
                        f'<tbody><tr class="no-positions-row">'
                        f'<td colspan="{colspan}" style="text-align:center; font-weight:600;">'
                        f'No Positions'
                        f'</td></tr></tbody>',
                        html,
                        flags=re.S
                    )
                arr.append(html)
                client_list.append(key)
            


            # print(xts_positions.keys())
            for key in xts_positions:
                df = xts_positions[key].copy()
                if not df.empty:
                    df['__row_attr__'] = (
                        'data-token="' + df['Token'].astype(str) + '" ' +
                        'data-exchange="' + df['Exchange'].astype(str) + '" ' +
                        'data-account="' + key + '" ' +
                        'data-instrument="' + df['Instrument'].astype(str) + '" ' +
                        'data-price="' + df['Price'].astype(str) + '" ' +
                        'data-quantity="' + df['Quantitys'].astype(str) + '" ' +
                        'data-side="' + df['Side'].astype(str) + '"'
                    )
                    
                    df = df.drop(columns=['Price','Quantitys','Side'])
                    # 2) Mark LTP column cell with class="ltp-value"
                    df['_PNL_NUM'] = pd.to_numeric(df['PNL'], errors='coerce')
                    df['LTP'] = '<span class="ltp-value">' + df['LTP'].astype(str) + '</span>'
                    df['PNL']='<span class="pnl-value">' + df['PNL'].astype(str) + '</span>'
                    def make_squareoff_form(row):
                        strike = str(row.Strike)
                        op_type = str(row.Type)
                        expiry = str(row.Expiry)
                        group_key = (strike, op_type, expiry)
                        if group_key in grouped:
                            body_json = json.dumps(grouped[group_key])
                            return (
                                f'<form action="{url_strike}" method="POST" style="display:inline;">'
                                f'<input type="hidden" name="csrfmiddlewaretoken" value="{csrf_token}"/>'
                                f'<input type="hidden" name="strike" value="{strike}"/>'
                                f'<input type="hidden" name="data" value=\'{body_json}\'/>'
                                '<button type="submit" class="squareoff-btn">Sq Off All Acc</button>'
                                '</form>'
                            )
                        return ""
                    df["SquareOff Strike"] = df.apply(make_squareoff_form, axis=1)
                    def make_squareoff_form_instrument(row,key):
                        instrument = str(row.Instrument)
                        key = str(key)
                        group_key = (instrument, key)
                        if group_key in grouped_instrument:
                            body_json = json.dumps(grouped_instrument[group_key])
                            return (
                                f'<form action="{url_strike}" method="POST" style="display:inline;">'
                                f'<input type="hidden" name="csrfmiddlewaretoken" value="{csrf_token}"/>'
                                f'<input type="hidden" name="instrument" value="{instrument}"/>'
                                f'<input type="hidden" name="data" value=\'{body_json}\'/>'
                                '<button type="submit" class="squareoff-btn">Position Zero</button>'
                                '</form>'
                            )
                        return ""
                    df["SquareOff Curr Acct"] = df.apply(
                        lambda row: make_squareoff_form_instrument(row, key),
                        axis=1
                    )
                    for index, row in df.iterrows():
                        exchange = row['Exchange']  # or whatever column holds the key
                        token = row['Token']
                        t = Thread(target=subscribe_row, args=(exchange, token))
                        t.start()
                        threads.append(t)
                        
                    # 3) Hide Token and Exchange from display
                    df = df.drop(columns=['Token', 'Exchange'])

                    # 4) Save row attributes in a variable THEN drop column so it doesn't show

                    sort_order = {'CE': 1, 'PE': 0}  # custom sort order for Type
                    df['Type_order'] = df['Type'].map(sort_order)
                    df = df.sort_values(by=['Instrument','Type_order','Expiry','Strike'])
                    df=df.drop(columns='Type_order')

                    final_rows = []
                    numeric_cols = ['_PNL_NUM']

                    for instrument, inst_df in df.groupby('Instrument', sort=False):
                        final_rows.append(inst_df)
                        closed_df=generate_closed_pnl(key.lower(),instrument)
                        total_closed_pnl = closed_df['total_pnl'].sum()
                        summary = inst_df[numeric_cols].sum()

                        summary_row = {col: '' for col in df.columns}
                        summary_row['Instrument'] = f'{instrument} TOTAL'
                        initial_pnl = round(summary['_PNL_NUM'], 2)
                        overall_pnl = round(float(initial_pnl) + float(total_closed_pnl), 2)
                        summary_row['PNL'] = (
                            f'<span class="pnl-total" data-value="{initial_pnl}">'
                            f'{initial_pnl}'
                            f'</span>'
                        )
                        # summary_row['ClosedPNL'] = 
                        summary_row['Squareoff'] = (
                            f'<div class="closed-pnl" style="text-align:center;">'
                            f'Closed PNL: {round(total_closed_pnl, 2)}'
                            f'</div>'
                        )
                        summary_row['PlaceSL'] = ''
                        summary_row['SquareOff Strike'] = (
                            f'<div class="overall-pnl" style="text-align:center;">'
                            f'Overall PNL: {overall_pnl}'
                            f'</div>'
                        )
                        summary_row["SquareOff Curr Acct"] = ''
                        summary_row['__row_attr__'] = (f'class="instrument-total" data-instrument="{instrument}" data-account="{key}"')

                        final_rows.append(pd.DataFrame([summary_row]))

                    if final_rows:
                        df = pd.concat(final_rows, ignore_index=True)
                    else:
                        df = pd.DataFrame(columns=df.columns)
                    df = df.drop(columns=['_PNL_NUM'])

                    row_attrs = df['__row_attr__'].tolist()
                    df = df.drop(columns=['__row_attr__'])

                    # 5) Render table normally (no __row_attr__ visible)
                    html = df.to_html(
                        classes="data",
                        escape=False,
                        index=False,
                        table_id="positions"
                    )

                    # 6) Insert row attributes into each <tr>
                    lines = html.splitlines()
                    final_html = []
                    row_iter = iter(row_attrs)
                    for line in lines:
                        if line.strip().startswith('<tr>'):
                            attrs = next(row_iter)
                            final_html.append(line.replace('<tr>', f'<tr {attrs}>'))
                        else:
                            final_html.append(line)

                    html = '\n'.join(final_html)
                else:
                    # Create empty table structure
                    html = df.to_html(classes="data", escape=False, index=False)

                    # Replace empty tbody with single "No Positions" row
                    colspan = len(df.columns)

                    html = re.sub(
                        r'<tbody>\s*</tbody>',
                        f'<tbody><tr class="no-positions-row">'
                        f'<td colspan="{colspan}" style="text-align:center; font-weight:600;">'
                        f'No Positions'
                        f'</td></tr></tbody>',
                        html,
                        flags=re.S
                    )

                arr.append(html)
                client_list.append(key)

            
            # print('Listing')
            # print(client_list)
            rows = zip(client_list, arr)

            return render(req,"positions.html",{'header':"true",'rows':rows})
        # else:
        #     messages.info(req,'Please Login')
        #     return redirect('/login')
    except:
        messages.info(req,'Error Occured')
        return redirect('/home')

@login_required
def home(req):
    try:
            # if r.get('ws_restart')!=1:
            #     t = Thread(
            #         target=restart_ws_service,
            #         args=(req,),
            #         daemon=True
            #     )
            #     t.start()
            if platform.system()!='Windows':
                subprocess.run(["sudo", "systemctl", "start", "ws.service"], check=True)
            masterclass_dict, clients, jainam_user_ids = master_connection(req.user)
            t = Thread(
                target=maybe_start_account_jobs,
                args=(masterclass_dict,),
                daemon=True
            )
            t.start()
            # response=master_connection(req.user)
            # masterclass_dict=response[0]
            # clients=response[1]
            # jainam_user_ids=response[2]
            # ws_connection=response[3]
            # ltp_cache=response[4]
            nifty_freeze_qty = get_freeze_quantity_from_nse("NIFTY", debug=True)
            banknifty_freeze_qty = get_freeze_quantity_from_nse("BANKNIFTY", debug=True)
            try:
                if nifty_freeze_qty is None or int(nifty_freeze_qty)<0:
                    nifty_freeze_qty=1800
                if banknifty_freeze_qty is None or int(nifty_freeze_qty)<0:
                    banknifty_freeze_qty=600
            except:
                nifty_freeze_qty=1800
                banknifty_freeze_qty=600
            sensex_freeze_qty=1000
            kite_instruments = get_instruments_cached("NFO")
            bfo_instruments = get_instruments_cached("BFO")
            kite_instruments = kite_instruments[kite_instruments["segment"] == "NFO-OPT"]
            nifty_lot_size=kite_instruments[kite_instruments['name'] == 'NIFTY']['lot_size'].iloc[0]
            banknifty_lot_size=kite_instruments[kite_instruments['name'] == 'BANKNIFTY']['lot_size'].iloc[0]
            kite_instruments["expiry"] = pd.to_datetime(kite_instruments["expiry"]).dt.date
            bfo_instruments = bfo_instruments[bfo_instruments["segment"] == "BFO-OPT"]
            bfo_instruments = bfo_instruments[bfo_instruments["name"] == "SENSEX"]
            sensex_lot_size=bfo_instruments['lot_size'].iloc[0]
            bfo_instruments["expiry"] = pd.to_datetime(bfo_instruments["expiry"]).dt.date
            bfo_instruments = bfo_instruments[bfo_instruments["expiry"] >= date.today()]
            if req.method == "POST":
                # for key,value in req.POST.items():
                    
                accounts_traded = []
                for key in masterclass_dict:
                    # if key == 'Kunal':
                    # continue
                    if f"client_check_{key}" in req.POST.keys():
                        accounts_traded.append((key, req.POST.getlist(f"client_qty_{key}")[0]))
                
                legs = []
                for key in req.POST.keys():
                    if key.startswith("instrument_"):
                        leg_num = key.split("_")[1]  # e.g. "3"
                        legs.append(leg_num)

                legs = sorted(set(legs), key=int)
                orders = []
                for leg in legs:
                    instrument   = req.POST.get(f"instrument_{leg}")
                    transaction  = req.POST.get(f"transaction_{leg}")
                    opt_type     = req.POST.get(f"type_{leg}")
                    expiry       = req.POST.get(f"expiry_{leg}")
                    strike       = req.POST.get(f"strike_{leg}")
                    qty          = req.POST.get(f"qty_{leg}")
                    price        = req.POST.get(f"price_{leg}")
                    if instrument=='SENSEX':
                        temp_instruments = bfo_instruments[
                            (bfo_instruments["name"] == instrument) &
                            (bfo_instruments["expiry"] == datetime.strptime(expiry, "%Y-%m-%d").date()) &
                            (bfo_instruments["strike"] == int(strike)) &
                            (bfo_instruments["instrument_type"] == opt_type)
                        ]
                        exchange_type='BFO'
                    else:
                        temp_instruments = kite_instruments[
                            (kite_instruments["name"] == instrument) &
                            (kite_instruments["expiry"] == datetime.strptime(expiry, "%Y-%m-%d").date()) &
                            (kite_instruments["strike"] == int(strike)) &
                            (kite_instruments["instrument_type"] == opt_type)
                        ]
                        exchange_type='NFO'
                    iterator=0
                    while True:
                        exchange_token = temp_instruments['exchange_token'].iloc[0]
                        if exchange_type=='BFO':
                            ltp = get_ltp_or_subscribe(7,exchange_token)
                            # ltp_key = f"7_{exchange_token}"
                            # if ltp_key in ltp_cache:
                            #     ltp=ltp_cache[ltp_key]
                            # else:
                            #     ws_connection.send(json.dumps({
                            #             "a": "subscribe",
                            #             "v": [[7, int(exchange_token)]],
                            #             "m": "marketdata"
                            #     }))
                            #     try:
                            #         ltp=ltp_cache[ltp_key]
                            #     except:
                            #         ltp=0
                        else:
                            ltp = get_ltp_or_subscribe(2,exchange_token)
                            # ltp_key = f"2_{exchange_token}"
                            # if ltp_key in ltp_cache:
                            #     ltp = ltp_cache[ltp_key]
                            # else:
                            #     ws_connection.send(json.dumps({
                            #             "a": "subscribe",
                            #             "v": [[2, int(exchange_token)]],
                            #             "m": "marketdata"
                            #     }))
                            #     try:
                            #         ltp=ltp_cache[ltp_key]
                            #     except:
                            #         ltp=0
                        if ltp!=0 or iterator>=5:
                            break
                        if int(price)==0:
                            time.sleep(1)
                            # ltp_cache=return_ltp_cache()
                        iterator+=1

                    if transaction == "BUY":
                        ltp = round(ltp * 1.1, 1)
                    else:
                        ltp = round(ltp * 0.9, 1)

                    if ltp > 1300:
                        messages.info(req,"Error Price TOO HIGH")
                        continue
                        # return redirect('/home')
                    
                    orders.append({
                        "instrument":instrument,
                        "client_id": None,
                        "disclosed_quantity": 0,
                        "exchange": exchange_type,
                        "instrument_token": instrument,
                        "market_protection_percentage": 100,
                        "order_side": transaction,
                        "order_type": "LIMIT",
                        "product": "NRML",
                        "quantity": int(qty),
                        "trigger_price": 0,
                        "validity": "DAY",
                        "user_order_id": "1",
                        "price": float(price) if int(price)!=0 else ltp,
                        "strike":strike,
                        "expiry":expiry,
                        "exchange_token":exchange_token
                    })
                ## print(orders)
                orders.sort(key=lambda x: 0 if x['order_side'].lower() == 'buy' else 1)
                for order in orders:
                    for i in accounts_traded:
                        # order['quantity']=order['quantity']*int(i[1])
                        temp_qty=order['quantity']*int(i[1])
                        # print(order['quantity'],int(i[1]))
                        # print('Temp Qty ',temp_qty)
                        order_qty=[]
                        match order['instrument']:
                            case 'BANKNIFTY':
                                while int(temp_qty)>0:
                                    if temp_qty>banknifty_freeze_qty:
                                        lots=math.floor(banknifty_freeze_qty/banknifty_lot_size)
                                        order_qty.append(lots*banknifty_lot_size)
                                        temp_qty-=(lots*banknifty_lot_size)
                                    else:
                                        lots=math.ceil(temp_qty/banknifty_lot_size)
                                        order_qty.append(lots*banknifty_lot_size)
                                        temp_qty-=(lots*banknifty_lot_size)
                            case 'NIFTY':
                                while int(temp_qty)>0:
                                    if temp_qty>nifty_freeze_qty:
                                        lots=math.floor(nifty_freeze_qty/nifty_lot_size)
                                        order_qty.append(lots*nifty_lot_size)
                                        temp_qty-=(lots*nifty_lot_size)
                                    else:
                                        lots=math.ceil(temp_qty/nifty_lot_size)
                                        order_qty.append(lots*nifty_lot_size)
                                        temp_qty-=(lots*nifty_lot_size)
                            case 'SENSEX':
                                while int(temp_qty)>0:
                                    if temp_qty>sensex_freeze_qty:
                                        lots=math.floor(sensex_freeze_qty/sensex_lot_size)
                                        order_qty.append(lots*sensex_lot_size)
                                        temp_qty-=(lots*sensex_lot_size)
                                    else:
                                        lots=math.ceil(temp_qty/sensex_lot_size)
                                        order_qty.append(lots*sensex_lot_size)
                                        temp_qty-=(lots*sensex_lot_size)
                        if ("jainam" not in i[0].lower()):
                            # token_1 = masterclass_dict[i[0]].get_nfo_token(
                            #         order['expiry'], order['strike'], order['instrument']
                            #     )
                            order["client_id"] = masterclass_dict[i[0]].username
                            order["instrument_token"] = order["exchange_token"]
                            temp_order = order.copy()
                            temp_order.pop("expiry",None)
                            temp_order.pop("strike",None)
                            temp_order.pop("exchange_token",None)
                            for final_order_qty in order_qty:
                                temp_order['quantity']=final_order_qty
                                # print('Master Trust',temp_order)
                                # b = masterclass_dict[i[0]].place_order(order1)
                                Thread(
                                    target=masterclass_dict[i[0]].place_order, args=(temp_order,)
                                ).start()
                        else:
                            exchange_segment = "NSEFO" if order["exchange"] == "NFO" else "BSEFO"
                            exchange_token = order["exchange_token"]
                            # masterclass_dict["RACHITA"].get_nfo_token(
                            #     order['expiry'], order['strike'], order['instrument']
                            # )
                            product_type = "NRML"
                            order_type = "LIMIT"
                            order_side = order['order_side']
                            time_in_force = "DAY"
                            disclosed_qty = 0
                            limit_price = order['price']
                            stop_price = 0
                            identifier = "aabbcc"
                            user_id = jainam_user_ids[i[0]]
                            for final_order_qty in order_qty:
                                # print('jainam',order,final_order_qty)
                                # print('Jainam',final_order_qty)
                                Thread(
                                    target=masterclass_dict[i[0]].place_order, 
                                    kwargs={
                                        "exchangeSegment": exchange_segment,
                                        "exchangeInstrumentID": exchange_token,
                                        "productType": product_type,
                                        "orderType": order_type,
                                        "orderSide": order_side,
                                        "timeInForce": time_in_force,
                                        "disclosedQuantity": disclosed_qty,
                                        "orderQuantity": int(final_order_qty),
                                        "limitPrice": limit_price,
                                        "stopPrice": stop_price,
                                        "orderUniqueIdentifier": identifier,
                                        "clientID": user_id,
                                    }
                                ).start()
                messages.info(req,'Orders Placed')
                return redirect('/home')


            # if authencticated
            experies={}
            sensex_experies = sorted(list(bfo_instruments["expiry"].unique()))
            sensex_experies=sensex_experies[:4]
            sensex_experies_str = [d.strftime("%Y-%m-%d") for d in sensex_experies]
            for key in masterclass_dict:
                if 'jainam' in key.lower():
                    continue
                nfo=get_contracts()
                # nfo = masterclass_dict[key].contracts["NFO"]
                nifty_data = nfo[nfo["symbol"].str.startswith("NIFTY")]
                banknifty_data = nfo[nfo["symbol"].str.contains("BANKNIFTY")]
                nifty_expiries = sorted(list(nifty_data["expiry"].unique()))
                banknifty_expiries = sorted(list(banknifty_data["expiry"].unique()))
                today = datetime.today().date()
                nifty_expiries = [datetime.fromtimestamp(ts).date() for ts in nifty_expiries if datetime.fromtimestamp(ts).date() >= today]
                nifty_expiries=nifty_expiries[:4]
                nifty_expiries_str = [d.strftime("%Y-%m-%d") for d in nifty_expiries]
                banknifty_expiries = [datetime.fromtimestamp(ts).date() for ts in banknifty_expiries if datetime.fromtimestamp(ts).date() >= today]
                banknifty_expiries = banknifty_expiries[:1]
                banknifty_expiries_str = [d.strftime("%Y-%m-%d") for d in banknifty_expiries]
                experies ["BANKNIFTY"]= banknifty_expiries_str
                experies["NIFTY"]= nifty_expiries_str
                experies["SENSEX"]= sensex_experies_str
                break

            # for key in masterclass_dict:
            #     start_background_job(key, masterclass_dict[key])


            return render(req,'trade.html',{
                "expiries_dict": json.dumps(experies),  # must be JSON string
            'clients':clients})
    except:
        messages.info(req,'Error Occured')
        return redirect('/home')

def login(req):
    if req.method=='POST':
        username=req.POST['username'].lower()
        password=req.POST['password']
        user=Users.objects.filter(username=username,password=password)
        if user.exists():
            user=user.first()
            token=f"user_{user.userid}"
            r.set(name=str(user.userid),value='1',ex=25200)
            # return HttpResponse("Logged In")
            response= redirect('/home')
            response.set_cookie('x-auth-token', token, httponly=True, secure=True, samesite='Strict')
            return response

        else:
            messages.info(req,'Invalid Credentials')
    if not r.exists("ws_restart"):
        restart_ws_service(req)
    return render(req,'login.html')

@login_required
def logout(req):
    # try:
        # r.delete('accounts_global')
        user=Users.objects.filter(username=req.user).first()
        r.delete(str(user.userid))
        r.delete("refresh_time_"+str(req.user))
    # except:
    #     pass
        return redirect('/login')

def index(req):
    # r.flushdb()

    return redirect('/login')