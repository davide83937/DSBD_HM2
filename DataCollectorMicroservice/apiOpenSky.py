from datetime import datetime
import requests


CLIENT_ID = "davidepanto@gmail.com-api-client"
CLIENT_SECRET = "ewpHTQ27KoTGv4vMoCyLT8QrIt4sLr3z"
AIRPORT = "LICC"


def get_token(client_id, client_secret):
    """Ottieni token Bearer da OpenSky"""
    url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(url, data=data, headers=headers)
    resp.raise_for_status()
    return resp.json()["access_token"]



def get_info_flight(token, airport, begin_ts, end_ts, type):
    """Recupera arrivi dall'API OpenSky per un intervallo"""
    url = f"https://opensky-network.org/api/flights/{type}?airport={airport}&begin={begin_ts}&end={end_ts}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json()
    elif resp.status_code == 404:
        return []
    else:
        raise Exception(f"Errore {resp.status_code}: {resp.text}")



def get_data(start_str):
   #icao_code =icao_code.strip().upper()
   start_str = start_str.strip()
   end_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
   print("----------------------------")
   date_format = "%Y-%m-%d %H:%M:%S"
   try:
       start_time = int(datetime.strptime(start_str, date_format).timestamp())
       end_time = int(datetime.strptime(end_str, date_format).timestamp())
       print(end_time)
       return start_time, end_time
   except ValueError:
      print("\nERRORE: Formato data/ora non valido. Controlla il formato YYYY-MM-DD HH:MM:SS.")
      exit()

