import requests
import json
import pandas as pd
# from requests_oauthlib import OAuth2Session
import os,sys
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import random
from mastertrust.helper import initialize
from requests_oauthlib import OAuth2Session
import zipfile
import io



os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'


class MasterTrustUser:
    API_ENDPOINTS ={
        'Test_URL':'http://uat.tradelab.in',
        'Live_URL':'https://masterswift-beta.mastertrust.co.in',
        'login':'/api/v1/user/login',
        'twofa' : '/api/v1/user/twofa',
        'orders': '/api/v1/orders',
        'positions': '/api/v1/positions',
        'trades': '/api/v1/trades',
        'profile': '/api/v1/user/profile',
        'place_order' :'/api/v1/orders',
        'cancel_order':'/api/v1/orders/',
        'contract_all':'/api/v1/contract/Compact?info=download'
    }

    def __init__(self,username,password,twofa,live=False,app_id=None,app_secret=None):
        self.username = username#username.upper()
        self.password = password
        self.twofa = twofa
        self.device = 'WEB'
        self.app_id = app_id
        self.app_secret = app_secret
        self.auth_token = None
        self.redirect_uri = 'http://127.0.0.1/5000'
        self.web_url = 'https://masterswift-beta.mastertrust.co.in'
        self.authorization_base_url = f'{self.web_url}/api/v1/user/profile?cliend_id={self.username}/'
        self.token_url = f'{self.web_url}/oauth2/token'
        self.scope = ['orders','holdings']
        #print(self.token_url)
        live = True
        if live == False:
            self.base_url = MasterTrustUser.API_ENDPOINTS['Test_URL']
        else:
            self.base_url = MasterTrustUser.API_ENDPOINTS['Live_URL']
        # self.initiate_login()
#         self.login_app()
        self.token_,self.auth_token = initialize(self.username,self.password,self.twofa)
        self.contracts = dict()
        self.exchanges = ['NSE','NFO','MCX','BFO']
        # self.get_master_contracts()
        self.allcontracts=pd.DataFrame()
        # self.get_all_contracts()

    def get_all_contracts(self):
        # Step 1: Fetch the ZIP file from the API
        response = requests.get(self.return_url('contract_all'))

        # Step 2: Check the response
        try:
            # Step 3: Load the ZIP file into memory
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # #print contents to verify the file name
                #print("ZIP contains:", z.namelist())

                # Step 4: Extract the CSV file (assuming it's named 'compactscrip.csv')
                with z.open("CompactScrip.csv") as csv_file:
                    df = pd.read_csv(csv_file)
                    self.allcontracts=df

        except:
            pass
            #print(f"Failed")

    def get_driver(self,Name=None):
        # path = os.path.dirname(os.path.abspath(__file__))
        chrome_options = webdriver.ChromeOptions()
        if Name is None or Name == "chrome":
            #chrome_options.add_argument("--headless")
            driver = webdriver.Chrome(options=chrome_options)
            driver = webdriver.Chrome()
            driver.maximize_window()
        elif Name == "PhantomJS":
            driver = webdriver.PhantomJS()
            driver.set_window_size(1124, 850)
        else:
            #chrome_options.add_argument("--headless")
            driver = webdriver.Chrome(options=chrome_options)
            driver.maximize_window()
        return driver

    def login_app(self):
        #print(self.authorization_base_url)
        try:

            oauth = OAuth2Session(self.app_id, redirect_uri=self.redirect_uri, scope=self.scope)
            authorization_url, _state = oauth.authorization_url(self.authorization_base_url,
                                                                access_type="authorization_code")

            #print('authorization_url')
            #print(authorization_url)
            try:
                self.driver = self.get_driver()

            except:
                #print(sys.exc_info())
                self.driver = self.get_driver(Name='PhantomJS')


            self.driver.get(authorization_url)
            time.sleep(2)
            input_fields = self.driver.find_elements_by_tag_name('input')
            self.driver.save_screenshot('Temp.png')
            #print("Sending Username")
            input_fields[0].send_keys(self.username)
            #print("Sending Password")
            input_fields[1].send_keys(self.password)
            input_fields[1].send_keys(Keys.ENTER)
            time.sleep(1)
            two_fa = self.driver.find_elements_by_tag_name('yyyy')
            try:
                two_fa[0].send_keys(str(self.twofa))
                two_fa[0].send_keys(Keys.ENTER)
                time.sleep(1)
                #self.driver.find_element_by_class_name('btn').click()
                current_url = self.driver.current_url
            except Exception as e:
                pass
                #print(e)
            #print('#printing current_url')
            #print(current_url)
            token = oauth.fetch_token(self.token_url, authorization_response=current_url,
                                      client_secret=self.app_secret)
            access_token = token['access_token']
            self.auth_token = access_token
            #print('Login App: ',access_token)
            self.driver.close()
        except Exception as e:
            pass
            #print('Error: ',e)
            #print(sys.exc_info())


    def return_url(self,name):
        return self.base_url+MasterTrustUser.API_ENDPOINTS[name]

    def initiate_login(self):
        headers = {'x-device-type':'WEB'}
        data = {
            'device': 'WEB',
            'login_id': self.username,
            'password':self.password
        }
        # #print(data)
        login_request = requests.post(url=self.return_url('login'),headers=headers,data=data)
        login_response = login_request.text
        json_data = json.loads(login_response)
        # #print(json_data)
        login_response = login_response.replace('false','False')
        login_response = login_response.replace('true','True')
        login_response = eval(login_response)
        # #print(login_response)
        if login_response['status'] == 'success':
            twofa_token = login_response['data']['twofa_token']
            question_ids = [x['question_id'] for x in login_response['data']['questions']]
        else:
            raise ValueError("Error Occured In Login. Check Username,Password and Twofa"+str(login_response))

        data = {
            'login_id': self.username.upper(),
            'twofa_token':twofa_token,
            'twofa':[{
            'question_id': x,
            'answer': self.twofa} for x in question_ids]
        }
        # #print(data)
        twofa_request = requests.post(url=self.return_url('twofa'),json=data)
        twofa_response = twofa_request.text
        twofa_response = twofa_response.replace('true','True')
        twofa_response = twofa_response.replace('false', 'False')
        twofa_response = eval(twofa_response)
        if twofa_response['status'] == 'success':
            self.auth_token = twofa_response['data']['auth_token']
            #print("Login Successful")
            return
        else:
            raise ValueError(
                "Error Occured In Two Fa. Check Twofa " + str(twofa_response))

    def get_master_contracts(self):
        nse_contracts = json.loads(requests.get('https://masterswift.mastertrust.co.in/api/v2/contracts.json?exchanges=NSE').text)
        self.contracts['NSE'] = pd.DataFrame()
        for x in nse_contracts:
            # self.contracts['NSE'] = self.contracts['NSE'].append(pd.DataFrame(nse_contracts[x]),ignore_index = True)
            self.contracts['NSE'] = pd.concat([pd.DataFrame(nse_contracts[x]) for x in nse_contracts], ignore_index=True)


        nfo_contracts = json.loads(requests.get('https://masterswift.mastertrust.co.in/api/v2/contracts.json?exchanges=NFO').text)
        self.contracts['NFO'] = pd.DataFrame()
        for x in nfo_contracts:
            # self.contracts['NFO'] = self.contracts['NFO'].append(pd.DataFrame(nfo_contracts[x]),ignore_index = True)
            self.contracts['NFO'] = pd.concat([pd.DataFrame(nfo_contracts[x]) for x in nfo_contracts], ignore_index=True)

        #print(self.contracts['NFO'].columns)

        mcx_contracts = json.loads(requests.get('https://masterswift.mastertrust.co.in/api/v2/contracts.json?exchanges=MCX').text)
        self.contracts['MCX'] = pd.DataFrame()
        for x in mcx_contracts:
            # self.contracts['MCX'] = self.contracts['MCX'].append(pd.DataFrame(mcx_contracts[x]),ignore_index = True)
            self.contracts['MCX'] = pd.concat([pd.DataFrame(mcx_contracts[x]) for x in mcx_contracts], ignore_index=True)

        
    def get_authorization_header(self):
        return {'Accept': 'application/json',
                #'Authorization': f'Bearer {self.auth_token}'
                'x-authorization-token': f'{self.auth_token}'
                }

    def get_profile(self):
        #print("Getting Profile")
        data = {'client_id': self.username}
        # #print(data)
        res = requests.get(params=data, url=self.return_url('profile'),
                           headers=self.get_authorization_header())
        #print(res.text)
        #time.sleep(25*60)
        #self.get_profile()
        return json.loads(res.text)

    def logged_in(self):
        data = {'client_id': self.username}
        # #print(data)
        res = requests.get(params=data, url=self.return_url('profile'),
                           headers=self.get_authorization_header())
        # #print(res.text)
        res = json.loads(res.text)
        if res['status'] != 'success':
            self.initiate_login()
        else:
            #print("Already Logged in.")
            pass
        return

    def get_nfo_token(self,expiry,strike,instrument):
        df = self.contracts['NFO']
        #print(df)
        #print(expiry)
        df = df[df['expiry'] == expiry]
        #print(df)
        df = df[df['symbol'].apply(lambda x: instrument in x)]
        
        df = df[df['trading_symbol'].apply(lambda x: strike in x)]
        return int(df['code'].iloc[0])


    def get_token_from_symbol(self,symbol,exchange):
        if exchange in ['NSE','NFO','MCX']:
            res = self.contracts[exchange].loc[self.contracts[exchange]['symbol'] == symbol,'code']
            if len(res) == 1 :
                return int(res)
            else:
                raise ValueError("Error. Issue with getting Symbol or Too many Results"+str(res))

    def get_orders(self,type = 'pending'):
        data = {'client_id':self.username,'type':type}
        res = requests.get(params = data,url = self.return_url('orders'),headers=self.get_authorization_header())
        res = json.loads(res.text)
        if res['status'] == 'success':
            
            return pd.DataFrame(res['data']['orders'])
        else:
            
            raise ValueError("Error in getting orders")
        

    def get_trades(self):
        data = {'client_id':self.username}
        res = requests.get(params = data,url = self.return_url('trades'),headers=self.get_authorization_header())
        res = json.loads(res.text)
        if res['status'] == 'success':
            return pd.DataFrame(res['data']['trades'])
        else:
            
            raise ValueError("Error in getting orders")

    def get_positions(self):
        data = {'client_id': self.username,'type':'historical'}
        # #print("Data",data )
        # #print("Header :  ", self.get_authorization_header())
        res = requests.get(params=data, url=self.return_url('positions'),
                           headers=self.get_authorization_header())
        # #print(res.headers)
        # #print(res.text)
        res = json.loads(res.text)
        # #print(res)
        if res['status'] == 'success':
            #print(res)
            return pd.DataFrame(res['data'])
        else:
            #print(res)
            raise ValueError("Error in getting Positions")

    def place_order(self,order):
        # {
        #     "client_id": "SUPERGOD",
        #     "disclosed_quantity": 1,
        #     "exchange": "NSE",
        #     "instrument_token": 3045,
        #     "market_protection_percentage": 10,
        #     "order_side": "BUY",
        #     "order_type": "MARKET",
        #     "price": 341,
        #     "product": "NRML",
        #     "quantity": 1,
        #     "trigger_price": 0,
        #     "validity": "DAY",
        #     "user_order_id": "1012910"
        # }
        order['client_id'] = self.username
        order['quantity'] = abs(order['quantity'])
        order_req = requests.post(self.return_url('place_order'),headers=self.get_authorization_header(),data=order)    

        #print(json.loads(order_req.text))
        return json.loads(order_req.text)


    def cancel_order_id(self,order_id):
        try:
            url=self.return_url('cancel_order')+str(order_id)+'?client_id='+str(self.username)
            headers=self.get_authorization_header()
            payload={}
            response = requests.request("DELETE", url, headers=headers, data=payload)
            return json.loads(response.text)
        except:
            return 

    def cancel_order(self,order_id):
        df = self.get_orders()
        df = df[df['oms_order_id']== str(order_id)]
        if len(df) > 0:
            data = df.to_dict('index')
            for each_item in data:
                order = data[each_item]
                order_can_req = requests.put(self.return_url('cancel_order')+str(order_id),headers=self.get_authorization_header(),data=order)
                return json.loads(order_can_req.text)

    def close_positions(self,symbol,exchange):
        df = self.get_orders()
        instrument_token = self.get_token_from_symbol(symbol=symbol,exchange=exchange)
        if len(df)>0:
            df  = df[df['instrument_token']==str(instrument_token)]
            df_dict  = df.to_dict('index')
            for x in df_dict:
                self.cancel_order(order_id=df_dict[x]['oms_order_id'])
        positions_df = self.get_positions()
        if len(positions_df) > 0:
            positions_df = positions_df[positions_df['product']=='MIS']
            positions_df = positions_df[positions_df['instrument_token']==instrument_token]
            positions_dict = positions_df.to_dict('index')
            for each_position in positions_dict:
                if positions_dict[each_position]['net_quantity'] > 0:
                    order = {
                        "client_id": self.username,
                        "disclosed_quantity": positions_dict[each_position]['net_quantity'],
                        "exchange": positions_dict[each_position]['exchange'],
                        "instrument_token": instrument_token,
                        "market_protection_percentage": 10,
                        "order_side": "SELL",
                        "order_type": "MARKET",
                        "price": 0,
                        "product": positions_dict[each_position]['product'],
                        "quantity":positions_dict[each_position]['net_quantity'] ,
                        "trigger_price": 0,
                        "validity": "DAY",
                        "user_order_id": str(random.randint(100000,9999999))
                    }
                    self.place_order(order)
                elif positions_dict[each_position]['net_quantity'] < 0:
                    order = {
                        "client_id": self.username,
                        "disclosed_quantity": positions_dict[each_position]['net_quantity']*-1,
                        "exchange": positions_dict[each_position]['exchange'],
                        "instrument_token": instrument_token,
                        "market_protection_percentage": 10,
                        "order_side": "BUY",
                        "order_type": "MARKET",
                        "price": 0,
                        "product": positions_dict[each_position]['product'],
                        "quantity":positions_dict[each_position]['net_quantity']*-1 ,
                        "trigger_price": 0,
                        "validity": "DAY",
                        "user_order_id": str(random.randint(100000,9999999))
                    }
                    self.place_order(order)




if __name__ == '__main__':
    #os.chdir(os.path.dirname(os.path.dirname(__file__)))

    # x  = MasterTrustUser(username='INV55-CHND',password='abc123',twofa='2000')
    # x  = MasterTrustUser(username='7maa384',password='poi@098',twofa='1982')
    x = MasterTrustUser(username='6LAA07740', password='chicu@24428', twofa='1992', live=True,app_id='APIUSER',app_secret='aas5ccov7n8n3pr55z586nav6dde19qb571mn03x0utqxxs2nh5avtaaqiacdzqenpw3xwjh5486ac31')
    # x = MasterTrustUser(username='7MPF404', password='bbb@222', twofa='1981', live=True)
    # x = MasterTrustUser(username='7MPF386', password='ppp@111', twofa='1969', live=True)
    # x = MasterTrustUser(username='7MJI01', password='05JUNE1991@', twofa='1994', live=True)
    # #print(x.logged_in())
    order = {
        "client_id": "SUPERGOD",
        "disclosed_quantity": 1,
        "exchange": "NSE",
        "instrument_token": 3045,
        "market_protection_percentage": 10,
        "order_side": "BUY",
        "order_type": "MARKET",
        # "price": 341,
        "product": "MIS",
        "quantity": 1,
        "trigger_price": 0,
        "validity": "DAY",
        "user_order_id": "1012910"
    }
    #x.place_order(order=order)
    #x.close_positions('SBIN','NSE')
    x.get_profile()
    #print("Positions:",x.get_positions())