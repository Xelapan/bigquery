import http.client
import ssl

def send_message(msj, phone_number):
    try:
        conn = http.client.HTTPSConnection("api.ultramsg.com", context=ssl._create_unverified_context())
        payload = "token=2v27xloy7ejs271p&to=%2B" + phone_number + "&body=" + msj
        payload = payload.encode('utf8').decode('iso-8859-1')
        headers = {'content-type': "application/x-www-form-urlencoded"}
        conn.request("POST", "/instance46411/messages/chat", payload, headers)
        res = conn.getresponse()
        data = res.read()
        print(data.decode("utf-8"))
    except Exception as e:
        print(f"Error: {e}")

