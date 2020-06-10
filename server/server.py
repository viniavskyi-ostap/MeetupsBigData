from flask import Flask
from server.stream_api import stream_route
from server.batch_api import batch_route


app = Flask(__name__)
app.register_blueprint(stream_route, url_prefix='/stream')
app.register_blueprint(batch_route, url_prefix='/batch')

if __name__ == '__main__':
    app.run(debug=True)
