#
# Simple Flask app
#
from flask import Flask
from flask import request, abort, make_response, jsonify
import requests
import urllib
from collections import OrderedDict
import json

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, world!'

from flask import request

@app.route('/do_something/<int:my_param>', methods=['POST'])
def do_something(my_param):
    return(f"PARAMETER {my_param}\nPOST DATA: {request.json}\n")

@app.route('/get_most_recent_stats_params', methods=['GET'])
def get_most_recent_stats_params():
    clickhouse_host="localhost"
    clickhouse_port="8123"
    table=request.args.get('table', default = 'system.one', type = str)
    fields=request.args.get('fields', default = '*', type = str)
    query = "select {} from {} format JSON".format(fields,table)
    response = requests.get('http://{}:{}/?query={}'.format(clickhouse_host, clickhouse_port, urllib.parse.quote(query)))
    if response.status_code != 200: return response.text
    parsed = json.loads(response.text)
    data = parsed["data"]
    result=""
    for l in data:
        result=result+"\n" +json.dumps(l,indent=4, sort_keys=True)
    return result



@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found. Bad luck!'}), 404)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')


