# Description: Script para enviar el valor del indice de miedo y codicia de CNN
import requests
from bs4 import BeautifulSoup
import http.client
import ssl
import json
import shelve
import datetime
import time
import os


def send_message(msj, phone_number):
    try:
        conn = http.client.HTTPSConnection("api.ultramsg.com", context=ssl._create_unverified_context())
        phone_number = "50252009468"
        payload = "token=2v27xloy7ejs271p&to=%2B" + phone_number + "&body=" + msj
        payload = payload.encode('utf8').decode('iso-8859-1')
        headers = {'content-type': "application/x-www-form-urlencoded"}
        conn.request("POST", "/instance46411/messages/chat", payload, headers)
        res = conn.getresponse()
        data = res.read()
        # print(data.decode("utf-8"))
    except Exception as e:
        print(f"Error: {e}")


def sendFnG():
    try:
        url = 'https://production.dataviz.cnn.io/index/fearandgreed/graphdata'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}

        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')

        if len(soup.text) > 30:
            json_data = json.loads(soup.text)
            score = round(json_data['fear_and_greed']['score'])
            rating = json_data['fear_and_greed']['rating']

            with shelve.open('mydata.db') as db:
                value = db.get('key', -1)
                if value != score:
                    db['key'] = score
                    send_message(str(score) + "  " + rating, "50252009468")
        else:
            send_message("Error " + "\n" + soup.text[:75], "50252009468")
    except Exception as e:
        print(f"Error: {e}")


def is_weekday_and_office_hours():
    try:
        now = datetime.datetime.now()
        weekday = now.weekday()  # Monday is 0, Sunday is 6
        hour = now.hour
        if 7 <= hour < 8:
            # Check if the file exists
            if os.path.exists('mydata.db'):
                # Delete the file
                os.remove('mydata.db')
        if 0 <= weekday < 5:  # Monday to Friday
            if 7 <= hour < 17:  # 7am to 5pm
                return True
        exit()
    except Exception as e:
        print(f"Error: {e}")
    return False


while True:
    if is_weekday_and_office_hours():
        sendFnG()
        # Wait for an hour before checking again
    time.sleep(3600)  # 3600 seconds = 1 hour
