import requests
import json
import csv
import onetimepass as otp

def get_closed_positions():
    with open('closed_positions.json','r') as f:
        data = f.read()

    data = json.loads(data)
    # #print(data)
    return data


def get_open_positions():

    with open('open_positions.txt') as json_file:
        data = json.load(json_file)
   
    # #print(data)
    return data


def write_csv(list,output):
    with open(output, "a", newline='') as fp:
        wr = csv.writer(fp, dialect='excel')
        wr.writerow(list)

def initialize(user_id,password,year):
    headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    # Already added when you pass json=
    # 'Content-Type': 'application/json',
    'Origin': 'https://masterswift-beta.mastertrust.co.in',
    'Referer': 'https://masterswift-beta.mastertrust.co.in/dashboard',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'x-device-type': 'web',
}

    json_data = {
        'login_id': user_id,
        'password': password,
    }
    #print(json_data)
    response = requests.post('https://masterswift-beta.mastertrust.co.in/api/v3/user/login', headers=headers, json=json_data)
    #print(response.json())
    twofa = response.json()['data']['twofa']['twofa_token']

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en-IN;q=0.9,en-GB;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        # Already added when you pass json=
        # 'Content-Type': 'application/json',
        'DNT': '1',
        'Origin': 'https://masterswift-beta.mastertrust.co.in',
        'Referer': 'https://masterswift-beta.mastertrust.co.in/marketwatches',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'x-authorization-token': 'null',
        'x-device-type': 'web',
    }
    totp = str(otp.get_totp(year))
    if len(totp) == 5:
        totp = '0'+totp
    elif len(totp) == 4:
        totp = '00'+totp
    else:
        #print(totp)
        pass
    json_data = {
        'login_id': user_id,
        'twofa_token':twofa ,
        'totp': totp,
    }
    # #print(json_data)
    # return
    response = requests.post('https://masterswift-beta.mastertrust.co.in/api/v3/user/validatetotp', headers=headers, json=json_data)
    #print('Auth Token ',user_id,': ',response.json()['data']['auth_token'])
    return twofa,response.json()['data']['auth_token']
def initialize2(user_id, password, year):
    year = str(year)
    url = "https://masterswift-beta.mastertrust.co.in/api/v1/user/login?device=WEB&"
    url_login = url + "login_id=" + user_id + "&password=" + password
    headers = {"x-device-type": "WEB"}
    data = {}
    response = requests.post(url=url_login, headers=headers, data=data)
    token = response.json()['data']['twofa_token']
    url = "https://masterswift-beta.mastertrust.co.in/api/v1/user/twofa?"
    url_login = url + "login_id=" + user_id + "&password=" + password
    headers = {
        "x-device-type": "WEB",
        "Host": "masterswift-beta.mastertrust.co.in",
        "Content-Type": "application/json"
    }

    data = {
        "login_id": user_id,
        "twofa": [
            {
                "question_id": 1,
                "answer": year
            }], "twofa_token": token}
    data['login_id'] = user_id
    data['twofa'][0]['answer'] = year
    data['twofa_token'] = token
    data = json.dumps(data)
    response = requests.post(url=url_login, headers=headers, data=data)

    return token, response.json()['data']['auth_token']