#!/usr/bin/env python3
import json
import logging
import sqlite3

from flask import Flask, current_app, g, jsonify, request
from flask_mqtt import Mqtt
from threading import Timer

app = Flask(__name__)
app.config['MQTT_BROKER_URL'] = '127.0.0.1'
app.config['MQTT_BROKER_PORT'] = 1883
app.config['MQTT_REFRESH_TIME'] = 1.0
app.config['DATABASE'] = './mesh-home.sqlite'
# refresh time in seconds
mqtt = Mqtt(app)
logging.basicConfig(level=logging.DEBUG)


def log(msg):
    app.logger.info(msg)


def make_dicts(cursor, row):
    return dict((cursor.description[idx][0], value)
                for idx, value in enumerate(row))


def get_db():
    if 'db' not in g:
        log(current_app.config['DATABASE'])
        g.db = sqlite3.connect("./mesh-home.sqlite",
                               detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = make_dicts

    return g.db


def get_mqtt_db():
    db = sqlite3.connect("./mesh-home.sqlite", detect_types=sqlite3.PARSE_DECLTYPES)
    db.row_factory = make_dicts
    return db


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


@app.route('/emitMockEvent/<nodeId>/<internalId>')
def emitMockEvent(nodeId, internalId):
    value = request.args.get('value', default=1)
    msg = {"nodeId": nodeId,
           "objectId": internalId,
           "type": "event",
           "objectType": "switch",
           "value": int(value)}
    mqtt.publish("painlessMesh/from/" + str(nodeId), json.dumps(msg))
    return jsonify(success=True)


@app.route('/emitMockTemp/<nodeId>/<internalId>')
def emitTemp(nodeId, internalId):
    temp = request.args.get('value', default=25)
    msg = {"nodeId": nodeId,
           "objectId": internalId,
           "type": "event",
           "objectType": "tempSensor",
           "value": int(temp)}
    mqtt.publish("painlessMesh/from/" + nodeId, json.dumps(msg))
    return jsonify(success=True)


@app.route('/emitMockHumidity/<nodeId>/<internalId>')
def emitTemp(nodeId, internalId):
    hum = request.args.get('value', default=30)
    msg = {"nodeId": nodeId,
           "objectId": internalId,
           "type": "event",
           "objectType": "humiditySensor",
           "value": int(hum)}
    mqtt.publish("painlessMesh/from/" + nodeId, json.dumps(msg))
    return jsonify(success=True)


@mqtt.on_connect()
def on_connect(client, userdata, flags, rc):
    log("Connected with result code " + str(rc))
    mqtt.subscribe("painlessMesh/to/#")


@mqtt.on_message()
def on_message(client, userdata, msg):
    db = get_mqtt_db()
    try:
        log(">>New mqtt message")
        jsonString = msg.payload.decode("utf-8")
        data = json.loads(jsonString)
        log(data)
        msgType = data["type"]
        if msgType == "change":
            msg = {"nodeId": int(data["nodeId"]),
                   "objectId": int(data["internalId"]),
                   "type": "read-value",
                   "value": data["value"]}
            mqtt.publish("painlessMesh/from/" + str(data["nodeId"]), json.dumps(msg))

    finally:
        db.close()


if __name__ == '__main__':
    mqtt.init_app(app)
    app.run(host='192.168.0.20', port=4666, use_reloader=False, debug=False)
