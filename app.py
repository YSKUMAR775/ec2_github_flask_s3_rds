from flask import Flask, request, jsonify
from module import fil1

app = Flask(__name__)


@app.route('/csv_post', methods=['Post'])
def fn_1():
    info = request.get_json()
    data = fil1.sn_1(info)
    return jsonify(data)


@app.route('/csv_get', methods=['Get'])
def fn_2():
    data = fil1.sn_2()
    return jsonify(data)


if __name__ == '__main__':
    app.run(host='0.0.0.0')
