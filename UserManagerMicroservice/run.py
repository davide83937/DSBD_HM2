from flask import Flask
from UserManagerMicroservice import app


appl = Flask(__name__)
appl.register_blueprint(app)

if __name__ == "__main__":
    appl.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)

