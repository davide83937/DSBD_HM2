import grpc

import DatabaseManager as db
import service_pb2
from flask import request
from flask import Blueprint
import grpc_manager

app = Blueprint('app', __name__)

CLIENT_ID = "davidepanto@gmail.com-api-client"
CLIENT_SECRET = "ewpHTQ27KoTGv4vMoCyLT8QrIt4sLr3z"
ARRIVAL = "arrival"
DEPARTURE = "departure"

@app.route("/send_interest", methods=["POST"])
def sendInterest():
    try:
        data = request.json
        email = data["email"]
        token = data["token"]
        stub = grpc_manager.get_stub()
        response = stub.checkUser(service_pb2.UserCheckMessage(email=email, token=token))
        if response.status == 0:
            airport = data["airport_code"]
            mode = data["mode"]
            response = db.insertInterests(email, airport, mode)
            if response == 0:
                return {"message": "Inserimento inserito"}
            else:
                return {"message": "Inserimento non inserito"}
        else:
            return {"message": "Utente non loggato"}
    except grpc.RpcError as e:
        e = e.code()
        if e == grpc.StatusCode.UNAVAILABLE:
              return {"message": f"Il canale grpc è spento o irraggiungibile: {e}"}, 503
    except KeyError as e:
        campo_mancante = e.args[0]
        return {"error": f"Manca il campo obbligatorio: {campo_mancante}"}, 400

@app.route("/delete_interest", methods=["POST"])
def delete_interest():
    try:
        data = request.json
        email = data["email"]
        token = data["token"]
        stub = grpc_manager.get_stub()
        response = stub.checkUser(service_pb2.UserCheckMessage(email=email, token=token))
        if response.status == 0:
            airport = data["airport_code"]
            mode = data["mode"]
            response = db.deleteInterest(email, airport, mode)
            if response == 0:
                return {"message": "Eliminazione riuscita"}, 200
            else:
                return {"message": "Eliminazione non riuscita"}, 404
        else:
            return {"message": "Utente non loggato"}, 409
    except grpc.RpcError as e:
        e = e.code()
        if e == grpc.StatusCode.UNAVAILABLE:
              return {"message": f"Il canale grpc è spento o irraggiungibile: {e}"}, 503
    except KeyError as e:
        campo_mancante = e.args[0]
        return {"error": f"Manca il campo obbligatorio: {campo_mancante}"}, 400


@app.route("/get_info", methods=["POST"])
def get_info():
    try:
        data = request.json
        email = data["email"]
        token = data["token"]
        stub = grpc_manager.get_stub()
        response = stub.checkUser(service_pb2.UserCheckMessage(email=email, token=token))
        if response.status == 0:
            airport = data["airport_code"]
            mode = data["mode"]
            response = db.get_flight_by_airport(airport, mode)
            lista_voli_json = []
            for riga in response:
                lista_voli_json.append({
                    "partenza": riga[0],
                    "ora_arrivo": riga[1],
                    "ora_partenza": riga[2],
                    "arrivo": str(riga[3]),
                    "codice": str(riga[4])
                })
            return {
                "count": len(lista_voli_json),
                "voli": lista_voli_json
            }, 200
        else:
            return {"message": "Utente non autorizzato o non loggato"}, 401
    except grpc.RpcError as e:
        e = e.code()
        if e == grpc.StatusCode.UNAVAILABLE:
              return {"message": f"Il canale grpc è spento o irraggiungibile: {e}"}, 503
    except KeyError as e:
        campo_mancante = e.args[0]
        return {"error": f"Manca il campo obbligatorio: {campo_mancante}"}, 400

@app.route("/get_last", methods=["POST"])
def get_last_one():
    try:
        data = request.json
        email = data["email"]
        token = data["token"]
        stub = grpc_manager.get_stub()
        response = stub.checkUser(service_pb2.UserCheckMessage(email=email, token=token))
        if response.status == 0:
            airport = data["airport_code"]
            last_arrival, last_departure = db.get_last_one(airport)
            response = [last_arrival, last_departure]
            lista_voli_json = []
            for riga in response:
                if riga is None:
                    continue
                lista_voli_json.append({
                    "partenza": riga[0],
                    "ora_arrivo": riga[1],
                    "ora_partenza": riga[2],
                    "arrivo": str(riga[3]),
                    "codice": str(riga[4])
                })
            return {
                "count": len(lista_voli_json),
                "voli": lista_voli_json
            }, 200
        else:
            return {"message": "Utente non autorizzato o non loggato"}, 401
    except grpc.RpcError as e:
         e = e.code()
         if e == grpc.StatusCode.UNAVAILABLE:
              return {"message": f"Il canale grpc è spento o irraggiungibile: {e}"}, 503
    except KeyError as e:
        campo_mancante = e.args[0]
        return {"error": f"Manca il campo obbligatorio: {campo_mancante}"}, 400

@app.route("/get_avgs", methods=["POST"])
def get_avgs():
    data = request.json
    email = data["email"]
    token = data["token"]
    try:
       stub = grpc_manager.get_stub()
       response = stub.checkUser(service_pb2.UserCheckMessage(email=email, token=token))
       if response.status == 0:
            airport = data["airport_code"]
            n_days = data["n_days"]
            arrival_avg, departure_avg = db.get_average_flights(airport, n_days)
            return {
                "media arrivi": arrival_avg,
                "media partenze": departure_avg
            }, 200
       else:
            return {"message": "Utente non autorizzato o non loggato"}, 401
    except grpc.RpcError as e:
        e = e.code()
        if e == grpc.StatusCode.UNAVAILABLE:
              return {"message": f"Il canale grpc è spento o irraggiungibile: {e}"}, 503
    except KeyError as e:
        campo_mancante = e.args[0]
        return {"error": f"Manca il campo obbligatorio: {campo_mancante}"}, 400




