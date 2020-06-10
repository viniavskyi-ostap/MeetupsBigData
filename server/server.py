from flask import Flask
from server.stream_api import stream_route


app = Flask(__name__)
app.register_blueprint(stream_route, url_prefix='/stream')

if __name__ == '__main__':
    app.run(debug=True)