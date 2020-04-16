#!/usr/bin/env python3
from flask import Flask, current_app, g, jsonify, make_response, request
from flask_mqtt import Mqtt
import json
import logging
import sqlite3
import sys

app = Flask(__name__)
app.config['MQTT_BROKER_URL'] = '127.0.0.1'
app.config['MQTT_BROKER_PORT'] = 1883
app.config['MQTT_REFRESH_TIME'] = 1.0
app.config['DATABASE'] = './mesh-home.sqlite'
# refresh time in seconds
mqtt = Mqtt(app)
logging.basicConfig(level=logging.DEBUG)


def make_dicts(cursor, row):
    return dict((cursor.description[idx][0], value)
                for idx, value in enumerate(row))


def get_db():
    if 'db' not in g:
        app.logger.info(current_app.config['DATABASE'])
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


@app.route('/objects')
def getObjects():
    cur = get_db().cursor()
    cur.execute("SELECT * FROM objects")

    return json.dumps(cur.fetchall())


@app.route('/object/<objectId>')
def getObject(objectId):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM objects where object_id = ?", objectId)
    return json.dumps(cur.fetchone())


@app.route('/change/<objectId>/<value>')
def changeState(objectId, value):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM objects where object_id = ?", objectId)
    obj = cur.fetchone()
    msg = {"nodeId": int(obj["node_id"]),
           "objectId": int(obj["internal_id"]),
           "type": "change",
           "value": int(value)}
    mqtt.publish("painlessMesh/to/" + obj["node_id"], json.dumps(msg))
    return jsonify(success=True)


@app.route('/reads/<objectId>')
def getReads(objectId):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM reads where object_id = ?", objectId)
    obj = cur.fetchall()
    return json.dumps(obj)


@app.route('/events/<objectId>')
def getEvents(objectId):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM events where object_id = ?", objectId)
    obj = cur.fetchall()
    return json.dumps(obj)


@app.route('/update/<objectId>')
def updateObject(objectId):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM objects where object_id = ?", objectId)
    obj = cur.fetchone()
    name = request.args.get('name', default=obj["name"])
    description = request.args.get('description', default=obj["description"])
    cur.execute("Update objects set name = ?, description = ? where object_id = ? ", (name, description, objectId))
    get_db().commit()
    return jsonify(success=True)


@mqtt.on_connect()
def on_connect(client, userdata, flags, rc):
    app.logger.info("Connected with result code " + str(rc))
    mqtt.subscribe("painlessMesh/from/#")


def saveValue(data):
    db = get_mqtt_db()
    cur = db.cursor()
    cur.execute(
        'Update reads set value = ? where object_id = (SELECT object_id from objects where node_id =? and internal_id = ? )',
        (data["value"], data["nodeId"], data["objectId"]))
    db.commit()
    db.close()


def checkAndAddToDatabase(data):
    db = get_mqtt_db()
    cur = db.cursor()
    cur.execute("SELECT count(*) as cnt from objects where node_id = ? and internal_id = ?",
                (data["nodeId"], data["objectId"]))
    if cur.fetchone()["cnt"] == 0:
        cur.execute("Insert into objects (node_id, internal_id, type) values (?, ?, ?)",
                    (data["nodeId"], data["objectId"], data.get("objectType", "switch")))
        db.commit()
    db.close()


def saveRead(data):
    db = get_mqtt_db()
    cur = db.cursor()
    cur.execute("SELECT object_id from objects where node_id =? and internal_id = ?",
                (data["nodeId"], data["objectId"]))
    obj = cur.fetchone()
    cur.execute(
        'Insert into reads (object_id, value) values (?,?)',
        (obj["object_id"], data["value"]))
    db.commit()
    db.close()

def saveEvent(data):
    db = get_mqtt_db()
    cur = db.cursor()
    cur.execute("SELECT object_id from objects where node_id =? and internal_id = ?",
                (data["nodeId"], data["objectId"]))
    obj = cur.fetchone()
    cur.execute(
        'Insert into events (object_id, value) values (?,?)',
        (obj["object_id"], data["value"]))
    db.commit()
    db.close()


@mqtt.on_message()
def on_message(client, userdata, msg):
    try:
        app.logger.info("Message")
        jsonString = msg.payload.decode("utf-8")
        data = json.loads(jsonString)
        app.logger.info(data)
        checkAndAddToDatabase(data)
        nodeId = data["nodeId"]
        internalId = data["objectId"]

        type = data["type"]
        if type == "read-value":
            app.logger.info(f'readed value {data["value"]}')
            saveValue(data)
        if type == "new-value":
            app.logger.info(f'readed value {data["value"]}')
            saveRead(data)
        if type == "event":
            app.logger.info(f'readed value {data["value"]}')
            saveEvent(data)
        elif type == "hello":
            app.logger.info(f'hello value {data["value"]}')

    except KeyError as e:
        app.logger.error(e)


if __name__ == '__main__':
    mqtt.init_app(app)
    app.run(host='192.168.0.20', port=4455, use_reloader=False, debug=False)
