import time

from flask import Flask
from DataCollectorMicroservice import app
import threading
import DatabaseManager as db

CLIENT_ID = "davidepanto@gmail.com-api-client"
CLIENT_SECRET = "ewpHTQ27KoTGv4vMoCyLT8QrIt4sLr3z"

def background_cancelling_flights():
    while True:
        db.delete_old_flights()
        time.sleep(43200)

def backgroung_downloading_flights():
    while True:
        db.download_flights(CLIENT_ID, CLIENT_SECRET)
        time.sleep(43200)

def start_downloading_flights():
    worker = threading.Thread(target=backgroung_downloading_flights)
    worker.daemon = True
    worker.start()



def start_cancelling_task():
    worker = threading.Thread(target=background_cancelling_flights)
    worker.daemon = True
    worker.start()


appl = Flask(__name__)
appl.register_blueprint(app)

if __name__ == "__main__":
    start_cancelling_task()
    start_downloading_flights()
    appl.run(host='0.0.0.0', port=5005, debug=True, use_reloader=False)
