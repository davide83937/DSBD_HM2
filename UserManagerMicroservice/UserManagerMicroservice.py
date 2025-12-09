import hashlib
import grpc
from flask import request
from flask import Blueprint
import DbManager as db
import grpc_methods
import service_pb2
import redis_script as rs

app = Blueprint('app', __name__)

def sha512_hash(s: str) -> str:
    return hashlib.sha512(s.encode()).hexdigest()

def sha256_hash(s: str) -> str:
    return hashlib.sha3_256(s.encode()).hexdigest()

@app.route("/login", methods=["POST"])
def login():
    try:
        data = request.json
        email = data["email"]
        password = data["password"]
        password = sha512_hash(password)
        response = db.check_user(email)
        if response == 1:
            response = db.login(email, password, True)
            if response == 0:
                return {"message": "Login effettuato con successo"}, 200
            elif response == 2:
                return {"message": "Utente gia loggato"}, 407
            elif response == -1:
                return {"message": "Qualcosa è andato storto"}, 404
            elif response == 1:
                return {"message": "Qualcosa è andato storto"}, 409
        else:
            return {"message": "L'utente non esiste"}, 408
    except grpc.RpcError as e:
       e = e.code()
       if e == grpc.StatusCode.UNAVAILABLE:
         return {"message": f"Il canale grpc è spento o irraggiungibile: {e}"}, 503
    except KeyError as e:
        campo_mancante = e.args[0]
        return {"error": f"Manca il campo obbligatorio: {campo_mancante}"}, 400




@app.route("/registrazione", methods=["POST"])
def registrazione():
    try:
        data = request.json
        email = data["email"]
        username = data["username"]
        password = data["password"]
        password = sha512_hash(password)
        hash_mail = sha256_hash(email)
        response = rs.check_request(hash_mail)
        if response == 0:
            return {"message": "Registrazione andata a buon fine"}, 200
        response = db.check_user(email)
        if response == 0:
            response = db.registrazione(email, username, password)
            if response == 0:
                rs.insert_request(hash_mail, username)
                return {"message": "Registrazione andata a buon fine"}, 200
            else:
                return {"message": "Qualcosa è andato storto"}, 404
        else:
            return {"message": "Utente già registrato"}, 409
    except grpc.RpcError as e:
       e = e.code()
       if e == grpc.StatusCode.UNAVAILABLE:
         return {"message": f"Il canale grpc è spento o irraggiungibile: {e}"}, 503
    except KeyError as e:
        campo_mancante = e.args[0]
        return {"error": f"Manca il campo obbligatorio: {campo_mancante}"}, 400


@app.route("/cancellazione", methods=["POST"])
def cancellazione():
    try:
        data = request.json
        email = data["email"]
        password = data["password"]
        password = sha512_hash(password)
        response = db.check_user(email)
        if response == 1:
            response = db.login(email, password, False)
            if response == 0:
               response = db.cancellazione(email)
               if response == 0:
                   db.cancellazione_sessione(email)
                   stub = grpc_methods.get_stub()
                   stub.delete_interestes_by_email(service_pb2.UserCheckMessage(email=email))
                   return {"message": "cancellazione andata a buon fine"}, 200
               else:
                   return {"message": "qualcosa è andato storto"}, 404
            else:
                return {"message": "password errata"}, 405
        else:
            return {"message": "utente non esistente"}, 409
    except grpc.RpcError as e:
       e = e.code()
       if e == grpc.StatusCode.UNAVAILABLE:
         return {"message": f"Il canale grpc è spento o irraggiungibile: {e}"}, 503
    except KeyError as e:
        campo_mancante = e.args[0]
        return {"error": f"Manca il campo obbligatorio: {campo_mancante}"}, 400



