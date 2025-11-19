from django.shortcuts import render,redirect
from django.http import HttpResponse, JsonResponse
from django.contrib import messages
# from godseye.connectors import master_connection,return_ltp_cache
from .master_manager import master_manager
from datetime import datetime,date
import pandas as pd
import json
from kiteconnect import KiteConnect
from godseye.freeze_quantity import get_freeze_quantity_from_nse
import math
from threading import Thread
import datetime as dt
import re
from collections import defaultdict
from django.urls import reverse
import time
from django.middleware.csrf import get_token
from django.core.cache import cache
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)


instrument_cache={}
positions_data=[]

def get_ltp_or_subscribe(exchange_code, token):

    ltp_key = f"{exchange_code}_{token}"

    # 1. Try to get cached LTP
    ltp = r.get(ltp_key)
    if ltp is not None:
        return float(ltp)

    # 2. Add subscription request to Redis SET
    sub_key = f"{exchange_code}_{token}"
    r.sadd("md_subscriptions", sub_key)

    # 3. Give WS worker a short time to fetch and push LTP
    time.sleep(0.10)

    # 4. Try again
    ltp = r.get(ltp_key)
    if ltp is not None:
        return float(ltp)

    # 5. Still nothing → return 0
    return 0


# def get_ltp(exchange_code, token):
#     return r.get(f"{exchange_code}_{token}")

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
    # response = master_connection()
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
        
def squareoff_strike(req):
    try:
        if req.session.get("logged_in"):
            strike = req.POST.get("strike")
            body_data = json.loads(req.POST.get("data"))
            ## print("Body Data ",body_data)
            masterclass_dict, clients, jainam_user_ids = master_manager.master_connection()
            # response=master_connection()
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
                data = id.rsplit('_', 2)
                token_and_name = data[0]
                quantity = data[1]
                exchange = data[2]
                # now split token from name (first underscore only)
                token, account_holder = token_and_name.split('_', 1)
                for key in masterclass_dict:
                    if "jainam" not in key.lower():
                        all_contracts=masterclass_dict[key].allcontracts
                        break
                instrument = all_contracts[all_contracts['exchange_token'].astype(str) == str(token)].iloc[0]['company_name']
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
            
        else:
            messages.info(req,'Please Login')
            return redirect('/login')
    except:
        messages.info(req,'Error Occured')
        return redirect('/positions')

def squareoff(req,id):
    try:
        if req.session.get("logged_in"):
            masterclass_dict, clients, jainam_user_ids = master_manager.master_connection()
            # response=master_connection()
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
            data = id.rsplit('_', 2)
            token_and_name = data[0]
            quantity = data[1]
            exchange = data[2]
            # now split token from name (first underscore only)
            token, account_holder = token_and_name.split('_', 1)
            for key in masterclass_dict:
                if "jainam" not in key.lower():
                    all_contracts=masterclass_dict[key].allcontracts
                    break
            instrument = all_contracts[all_contracts['exchange_token'].astype(str) == str(token)].iloc[0]['company_name']
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
        else:
            messages.info(req,'Please Login')
            return redirect('/login')
    except:
        messages.info(req,'Error Occured')
        return redirect('/positions')

def squareoff_calls(req):
    pass

def squareoff_puts(req):
    pass

def find_expiry(nfo,token,instrument):
    if instrument=='SENSEX':
        nfo = nfo[nfo["exchange_token"].astype(str) == str(token)]
        return nfo.iloc[0]["expiry"].strftime("%d-%m-%Y")
    else:
        nfo = nfo[nfo["code"] == str(token)]
        return dt.datetime.fromtimestamp(nfo.iloc[0]["expiry"]).strftime("%d-%m-%Y")

def pnl(req):
    try:
        global positions_data
        if req.session.get("logged_in"):
            if not req.headers.get('x-requested-with') == 'XMLHttpRequest':
                bfo_instruments = get_instruments_cached("BFO")
                bfo_instruments = pd.DataFrame(bfo_instruments)
                bfo_instruments = bfo_instruments[bfo_instruments["segment"] == "BFO-OPT"]
                bfo_instruments = bfo_instruments[bfo_instruments["name"] == "SENSEX"]
                masterclass_dict, clients, jainam_user_ids = master_manager.master_connection()
                # response=master_connection()
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
                                nfo = masterclass_dict[key].contracts["NFO"]
                            
                            # Find the expiry using your custom function
                            expiry = find_expiry(nfo, row["instrument_token"], instrument)
                            
                            # Determine trade direction (Buy or Sell) and extract quantity and price
                            # if row['cf_sell_quantity'] != 0:
                            #     quantity = row['cf_sell_quantity']
                            #     price = row['actual_average_sell_price']
                            #     side = 'SELL'
                            # else:
                            #     quantity = row['cf_buy_quantity']
                            #     price = row['actual_average_buy_price']
                            #     side = 'BUY'
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
        else:
            messages.info(req,'Please Login')
            return redirect('/login')
    except:
        messages.info(req,'Error Occured')
        return redirect('/home')

def positions(req):
    try:
        if req.session.get("logged_in"):
            csrf_token = get_token(req) 
            bfo_instruments = get_instruments_cached("BFO")
            bfo_instruments = bfo_instruments[bfo_instruments["segment"] == "BFO-OPT"]
            bfo_instruments = bfo_instruments[bfo_instruments["name"] == "SENSEX"]
            masterclass_dict, clients, jainam_user_ids = master_manager.master_connection()
            # response=master_connection()
            # masterclass_dict=response[0]
            # clients=response[1]
            # jainam_user_ids=response[2]
            # ws_connection=response[3]
            # ltp_cache=response[4]
            client_list = []
            arr = []
            titles = []
            xts_positions = {}
            threads = []
            master_dfs=[]
            grouped = defaultdict(list)
            for key in masterclass_dict:  
                # print(key)  
                # client_list.append(key)
                if 'jainam' in key.lower():
                    user_id = jainam_user_ids[key]
                    iterator=0
                    positions=[]
                    while iterator<2:
                        try:
                            positions = masterclass_dict[key].get_position_netwise(user_id)["result"][
                                "positionList"
                            ]
                            iterator=0
                            break
                        except:
                            iterator+=1
                    df=pd.DataFrame(positions)
                    ## print(f"{key} positions: {positions}")
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
                            "SquareOff"
                        ]
                    )
                    for pos1 in positions:
                        if int(pos1["Quantity"]) == 0:
                            continue
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
                            ltp = get_ltp_or_subscribe(7,token)
                            # ws_connection.send(json.dumps({
                            #     "a": "subscribe",
                            #     "v": [[7, int(token)]],
                            #     "m": "marketdata"
                            # }))
                            # ltp_key = f"7_{token}"
                        else:
                            ltp = get_ltp_or_subscribe(2,token)
                            # ws_connection.send(json.dumps({
                            #     "a": "subscribe",
                            #     "v": [[2, int(token)]],
                            #     "m": "marketdata"
                            # }))
                            # ltp_key = f"2_{token}"
                        # iterator=0
                        # while iterator<2:
                        #     if ltp_key in ltp_cache:
                        #         iterator=0
                        #         break
                        #     time.sleep(1)
                        #     ltp_cache=return_ltp_cache() 
                        #     iterator+=1
                        # try:
                        #     ltp=ltp_cache[ltp_key]
                        # except:
                        #     ltp=0
                        if float(pos1['OpenSellQuantity']) != 0:
                            quantity = pos1['OpenSellQuantity']
                            price = pos1['SellAveragePrice']
                            side = 'SELL'
                        else:
                            quantity = pos1['OpenBuyQuantity']
                            price = pos1['BuyAveragePrice']
                            side = 'BUY'
                        pos['PNL']=round((float(ltp) - float(price)) * float(quantity) if side == 'BUY' else (float(price) - float(ltp)) * float(quantity),2)
                        pos['Quantitys']=quantity
                        pos['Price']=price
                        pos['Side']=side
                        squareoff_id = f"{pos1['ExchangeInstrumentId']}_{key}_{pos1['Quantity']}_{pos1['ExchangeSegment']}"
                        grouped[str(pos["Strike"])].append(squareoff_id)
                        url = reverse('squareoff', kwargs={'id': squareoff_id})
                        # url = f"{{% url 'squareoff' id='{pos1['ExchangeInstrumentId']}_{key}_{pos1['Quantity']}_{pos1['ExchangeSegment']}' %}}"
                        # pos["SquareOff"] = (
                        #     '<form method="POST">'
                        #     + '<button type="submit">'
                        #     + f'<a href="{url}": target="_blank">'
                        #     + "Squareoff"
                        #     + "</a>"
                        #     + "</button>"
                        #     + "</form>"
                        # )
                        pos["SquareOff"] = (
                            f'<form action="{url}" method="POST" style="display:inline;">'
                            f'<input type="hidden" name="csrfmiddlewaretoken" value="{csrf_token}"/>'
                            '<button type="submit">Squareoff</button>'
                            '</form>'
                        )
                        # url_strike=reverse('squareoff_strike')
                        # body_json = json.dumps(grouped[pos['Strike']])  # serialize list as JSON
                        # pos["SquareOff Strike"] = (
                        #     f'<form action="{url_strike}" method="POST" style="display:inline;">'
                        #     f'<input type="hidden" name="strike" value="{strike}"/>'
                        #     f'<input type="hidden" name="data" value=\'{body_json}\'/>'
                        #     '<button type="submit">Squareoff Strikes</button>'
                        #     '</form>'
                        # )
                        data.loc[len(data)] = list(pos.values())
                        data = data.sort_values(
                            by=["Instrument", "Expiry", "Type", "Strike"],
                            ascending=[True, True, False, True],
                        )
                    ## print('jainam Data: ',data)
                    xts_positions[key] = data
                    continue
                iterator=0
                while iterator<2:
                    try:
                        df = masterclass_dict[key].get_positions()
                        trades=masterclass_dict[key].get_trades()
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
                client_list.append(key)
                if not df.empty:
                    for index, row in df.iterrows():
                        instrument = row["symbol"]
                        # try:
                        if instrument=='SENSEX':
                            nfo=bfo_instruments
                        else:
                            nfo = masterclass_dict[key].contracts["NFO"]
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
                            ltp = get_ltp_or_subscribe(7,int(token))
                            # ws_connection.send(json.dumps({
                            #     "a": "subscribe",
                            #     "v": [[7, int(token)]],
                            #     "m": "marketdata"
                            # }))
                            # ltp_key = f"7_{token}"
                        else:
                            ltp = get_ltp_or_subscribe(2,int(token))
                            # ws_connection.send(json.dumps({
                            #     "a": "subscribe",
                            #     "v": [[2, int(token)]],
                            #     "m": "marketdata"
                            # }))
                            # ltp_key = f"2_{token}"
                        # iterator=0
                        # while iterator<2:
                        #     if ltp_key in ltp_cache:
                        #         iterator=0
                        #         break
                        #     time.sleep(1)
                        #     ltp_cache=return_ltp_cache() 
                        #     iterator+=1
                        # try:
                        #     ltp=ltp_cache[ltp_key]
                        # except:
                        #     ltp=0
                        try:
                            filtered_trades = trades[trades['trading_symbol'] == row["trading_symbol"]]
                            
                            # Sum the trade_price column of the filtered trades
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
                            if float(row['cf_sell_quantity']) != 0:
                                quantity = row['cf_sell_quantity']
                                price = float(row['actual_average_sell_price'])
                                side = 'SELL'
                            else:
                                quantity = row['cf_buy_quantity']
                                price = float(row['actual_average_buy_price'])
                                side = 'BUY'
                        pnl=round((float(ltp) - float(price)) * float(quantity)  if side == 'BUY' else (float(price) - (float(ltp))) * float(quantity),2)
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
                # df["Squareoff"] = df.apply(
                #     lambda row: (
                #         '<form method="POST">'
                #         + '<button type="submit">'
                #         + f'<a href="{row["url"]}" target="_blank">'
                #         + "Squareoff"
                #         + "</a>"
                #         + "</button>"
                #         + "</form>"
                #     ),
                #     axis=1
                # )
                df["Squareoff"] = df.apply(
                    lambda row: (
                        f'<form action="{row["url"]}" method="POST" style="display:inline;">'
                        f'<input type="hidden" name="csrfmiddlewaretoken" value="{csrf_token}"/>'
                        '<button type="submit">Squareoff</button>'
                        '</form>'
                    ),
                    axis=1
                )
                new_grouped = (
                    df.groupby("Strike")
                    .apply(lambda g: [
                        f"{row.Token}_{key}_{row.Quantity}_{row.Exchange}"
                        for row in g.itertuples(index=False)
                    ])
                    .to_dict()
                )

                for strike, items in new_grouped.items():
                    grouped[strike].extend(items)

                # def make_squareoff_form(row):
                #     strike = row.strike
                #     if strike in grouped:
                #         body_json = json.dumps(grouped[strike])
                #         return (
                #             f'<form action="{url_strike}" method="POST" style="display:inline;">'
                #             f'<input type="hidden" name="strike" value="{strike}"/>'
                #             f'<input type="hidden" name="data" value=\'{body_json}\'/>'
                #             '<button type="submit">Squareoff Strikes</button>'
                #             '</form>'
                #         )
                #     return ""
                # df["SquareOff Strike"] = df.apply(make_squareoff_form, axis=1)

                df = df.drop(["url","rollover_url"], axis=1)

                # df['__row_attr__'] = (
                #     'data-token="' + df['Token'].astype(str) + '" '
                #     'data-exchange="' + df['Exchange'].astype(str) + '" '
                #     f'data-account="{key}"'
                # )

                df['__row_attr__'] = (
                    'data-token="' + df['Token'].astype(str) + '" '
                    'data-exchange="' + df['Exchange'].astype(str) + '" '
                    'data-account="' + key + '" '
                    'data-price="' + df['Price'].astype(str) + '" '
                    'data-quantity="' + df['Quantitys'].astype(str) + '" '
                    'data-side="' + df['Side'].astype(str) + '"'
                )
                df = df.drop(columns=['Price','Quantitys','Side'])
                # Mark LTP column cell for live update
                df['LTP'] = '<span class="ltp-value">' + df['LTP'].astype(str) + '</span>'
                df['PNL']='<span class="pnl-value">' + df['PNL'].astype(str) + '</span>'


                for index, row in df.iterrows():
                    exchange = row['Exchange']  # or whatever column holds the key
                    token = row['Token']
                    t = Thread(target=subscribe_row, args=(exchange, token))
                    t.start()
                    threads.append(t)

                # Hide Token and Exchange from display
                df = df.drop(columns=['Token', 'Exchange'])

                # 4) IMPORTANT: Drop __row_attr__ from display BEFORE to_html
                # row_attrs = df['__row_attr__'].tolist()  # save separately
                # df = df.drop(columns=['__row_attr__'])

                master_dfs.append(df)
            ## print('Group ',grouped)
            url_strike=reverse('squareoff_strike')
            for df in master_dfs:
                def make_squareoff_form(row):
                    strike = row.Strike
                    if strike in grouped:
                        body_json = json.dumps(grouped[strike])
                        return (
                            f'<form action="{url_strike}" method="POST" style="display:inline;">'
                            f'<input type="hidden" name="csrfmiddlewaretoken" value="{csrf_token}"/>'
                            f'<input type="hidden" name="strike" value="{strike}"/>'
                            f'<input type="hidden" name="data" value=\'{body_json}\'/>'
                            '<button type="submit">Squareoff Strikes</button>'
                            '</form>'
                        )
                    return ""
                df["SquareOff Strike"] = df.apply(make_squareoff_form, axis=1)
                sort_order = {'CE': 0, 'PE': 1}  # custom sort order for Type
                df['Type_order'] = df['Type'].map(sort_order)
                df = df.sort_values(by=['Type_order', 'Strike']).drop(columns='Type_order')
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


            # 7) Add to output
            arr.append(html)
            titles.append(df.columns.values)
                


            # remove specific item from list

            
            # for key in xts_positions:
            #     arr.append(xts_positions[key].to_html(classes="data", escape=False))
            #     titles.append(xts_positions[key].columns.values)
            #     client_list.append(key)
            # rows = zip(client_list, arr)

            for key in xts_positions:
                df = xts_positions[key].copy()

                # 1) Create row attributes BEFORE dropping columns
                # df['__row_attr__'] = (
                #     'data-token="' + df['Token'].astype(str) + '" '
                #     'data-exchange="' + df['Exchange'].astype(str) + '" '
                #     f'data-account="{key}"'
                # )
                df['__row_attr__'] = (
                    'data-token="' + df['Token'].astype(str) + '" ' +
                    'data-exchange="' + df['Exchange'].astype(str) + '" ' +
                    'data-account="' + key + '" ' +
                    'data-price="' + df['Price'].astype(str) + '" ' +
                    'data-quantity="' + df['Quantitys'].astype(str) + '" ' +
                    'data-side="' + df['Side'].astype(str) + '"'
                )
                
                df = df.drop(columns=['Price','Quantitys','Side'])
                # 2) Mark LTP column cell with class="ltp-value"
                df['LTP'] = '<span class="ltp-value">' + df['LTP'].astype(str) + '</span>'
                df['PNL']='<span class="pnl-value">' + df['PNL'].astype(str) + '</span>'
                def make_squareoff_form(row):
                    strike = row.Strike
                    if strike in grouped:
                        body_json = json.dumps(grouped[strike])
                        return (
                            f'<form action="{url_strike}" method="POST" style="display:inline;">'
                            f'<input type="hidden" name="csrfmiddlewaretoken" value="{csrf_token}"/>'
                            f'<input type="hidden" name="strike" value="{strike}"/>'
                            f'<input type="hidden" name="data" value=\'{body_json}\'/>'
                            '<button type="submit">Squareoff Strikes</button>'
                            '</form>'
                        )
                    return ""
                df["SquareOff Strike"] = df.apply(make_squareoff_form, axis=1)
                for index, row in df.iterrows():
                    exchange = row['Exchange']  # or whatever column holds the key
                    token = row['Token']
                    t = Thread(target=subscribe_row, args=(exchange, token))
                    t.start()
                    threads.append(t)
                    
                # 3) Hide Token and Exchange from display
                df = df.drop(columns=['Token', 'Exchange'])

                # 4) Save row attributes in a variable THEN drop column so it doesn't show

                sort_order = {'CE': 0, 'PE': 1}  # custom sort order for Type
                df['Type_order'] = df['Type'].map(sort_order)
                df = df.sort_values(by=['Type_order', 'Strike'])
                df=df.drop(columns='Type_order')
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

                arr.append(html)
                client_list.append(key)

            rows = zip(client_list, arr)

            return render(req,"positions.html",{'header':"true",'rows':rows})
        else:
            messages.info(req,'Please Login')
            return redirect('/login')
    except:
        messages.info(req,'Error Occured')
        return redirect('/home')

def home(req):
    try:
        if req.session.get("logged_in"):
            masterclass_dict, clients, jainam_user_ids = master_manager.master_connection()
            # response=master_connection()
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
                        return redirect('/home')
                    
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
                nfo = masterclass_dict[key].contracts["NFO"]
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

            return render(req,'trade.html',{
                "expiries_dict": json.dumps(experies),  # must be JSON string
            'clients':clients})
        else:
            messages.info(req,'Please Login')
            return redirect('/login')
    except:
        messages.info(req,'Error Occured')
        return redirect('/home')

def login(req):
    r.flushdb()  # or selectively delete keys
    req.session['logged_in']=False
    if req.method=='POST':
        username=req.POST['username'].lower()
        password=req.POST['password']
        if username=='ganesha' and password=='ayusshmittaal':
            req.session["logged_in"] = True
            req.session.set_expiry(25200)  # expires in 5 minutes (300 seconds)
            return redirect('/home')
        else:
            messages.info(req,'Invalid Credentials')
    
    return render(req,'login.html')

def logout(req):
    r.flushdb()  # or selectively delete keys
    req.session['logged_in']=False
    return redirect('/login')

def index(req):
    req.session['logged_in']=False
    return redirect('/login')