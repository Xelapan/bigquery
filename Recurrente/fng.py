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
import utils.bdds as bdds
import utils.sendwp as sendwp

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
                    sendwp(str(score) + "  " + rating, "50252009468")
                else:                    
                    sendwp("Same Score: " + str(score) + "  " + rating, "50252009468")
        else:
            sendwp("Error " + "\n" + soup.text[:75], "50252009468")
    except Exception as e:
        print(f"Error: {e}")


def is_weekday_and_office_hours():
    try:
        now = datetime.datetime.now()
        weekday = now.weekday()  # Monday is 0, Sunday is 6
        hour = now.hour
        if 4 <= hour < 8:
            # Check if the file exists
            if os.path.exists('mydata.db'):
                # Delete the file
                os.remove('mydata.db')
        if 0 <= weekday < 5:  # Monday to Friday
            if 7 <= hour < 18:  # 7am to 5pm
                return True
        exit()
    except Exception as e:
        print(f"Error: {e}")
    return False


if is_weekday_and_office_hours():
    sendFnG()
