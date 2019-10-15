#
# Simple Flask app
#
from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, world!'

from flask import request

@app.route('/do_something/<int:my_param>', methods=['POST'])
def do_something(my_param):
    return(f"PARAMETER {my_param}\nPOST DATA: {request.json}\n")

@app.route('/get_most_recent_stats_params/<int:my_param>', methods=['POST'])
def get_most_recent_stats_params(my_param):
    return(f"PARAMETER {my_param}\nPOST DATA: {request.json}\n")



from flask import request, abort, make_response, jsonify

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found. Bad luck!'}), 404)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')


