#!/usr/bin/env python3
from flask import Flask, current_app, g, jsonify, request
from flask_mqtt import Mqtt
import json
import logging
import sqlite3

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




@app.route('/addOwnObject')
def addOwnObject():
    cur = get_db().cursor()
    objectType = request.args.get('type', default='switch')
    name = request.args.get('name', default='Sztuczny obiekt')
    description = request.args.get('description', default='Sztuczny obiekt')
    cur.execute("SELECT min(node_id) as nodeId from objects where node_id < 0 ")
    res = cur.fetchone()
    lastNodeId = res.get("nodeId")
    if lastNodeId is None:
        lastNodeId = 0
    newNodeId = lastNodeId - 1
    cur.execute("Insert into objects (node_id, internal_id, type, name, description) values (?, 1, ?, ?, ?)",
                (newNodeId, objectType, name, description))
    get_db().commit()

    cur.execute("SELECT * FROM objects where node_id  = ?", (newNodeId, ))
    obj = cur.fetchone()

    return jsonify(objectId=obj["object_id"])


@app.route('/change/<objectId>/<value>')
def changeState(objectId, value, db=None):
    if db is None:
        db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM objects where object_id = ?", (objectId, ))
    obj = cur.fetchone()
    msg = {"nodeId": int(obj["node_id"]),
           "objectId": int(obj["internal_id"]),
           "type": "change",
           "value": int(value)}
    mqtt.publish("painlessMesh/to/" + str(obj["node_id"]), json.dumps(msg))
    return jsonify(success=True)


@app.route('/read/<objectId>')
def readState(objectId, value):
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

@app.route('/emitEvent/<objectId>')
def emitEvent(objectId):
    processEvent(objectId, get_db())
    return jsonify(success=True)


@app.route('/update/<objectId>')
def updateObject(objectId):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM objects where object_id = ?", objectId)
    obj = cur.fetchone()
    name = request.args.get('name', default=obj["name"])
    description = request.args.get('description', default=obj["description"])
    objType = request.args.get('type', default=obj["type"])
    cur.execute("Update objects set name = ?, description = ?, type = ? where object_id = ? ",
                (name, description, objType, objectId))
    get_db().commit()
    return jsonify(success=True)


@mqtt.on_connect()
def on_connect(client, userdata, flags, rc):
    log("Connected with result code " + str(rc))
    mqtt.subscribe("painlessMesh/from/#")


def saveValue(data, db):
    log(">>saveValue")
    db = get_mqtt_db()
    cur = db.cursor()
    cur.execute(
        'Update objects set value = ? where node_id =? and internal_id = ? ',
        (data["value"], data["nodeId"], data["objectId"]))
    db.commit()
    log("saveValue<<")


def checkAndAddToDatabase(data, db):
    cur = db.cursor()
    cur.execute("SELECT count(*) as cnt from objects where node_id = ? and internal_id = ?",
                (data["nodeId"], data["objectId"]))
    if cur.fetchone()["cnt"] == 0:
        cur.execute("Insert into objects (node_id, internal_id, type) values (?, ?, ?)",
                    (data["nodeId"], data["objectId"], data.get("objectType", "switch")))
        db.commit()
        log(">>new Object add<<")


def saveRead(data, db):
    cur = db.cursor()
    cur.execute("SELECT object_id from objects where node_id =? and internal_id = ?",
                (data["nodeId"], data["objectId"]))
    obj = cur.fetchone()
    cur.execute(
        'Insert into reads (object_id, value) values (?,?)',
        (obj["object_id"], data["value"]))
    db.commit()


def eventHandler(data, db):
    cur = db.cursor()
    cur.execute("SELECT object_id from objects where node_id =? and internal_id = ?",
                (data["nodeId"], data["objectId"]))
    obj = cur.fetchone()
    objId = obj["object_id"]
    cur.execute(
        'Insert into events (object_id, value) values (?,?)',
        (objId, data["value"]))

    processEvent(objId, db)
    db.commit()


def processEvent(objectId, db):
    cur = db.cursor()
    cur.execute("Select * from scenarios where from_object_id = ?", (objectId, ))
    steps = cur.fetchall()
    for step in steps:
        targetObjectId = step["to_object"]
        value = step["new_value"]
        changeState(targetObjectId, value, db)


@mqtt.on_message()
def on_message(client, userdata, msg):
    db = get_mqtt_db()
    try:
        log(">>New mqtt message")
        jsonString = msg.payload.decode("utf-8")
        data = json.loads(jsonString)
        log(data)
        checkAndAddToDatabase(data, db)

        type = data["type"]
        if type == "read-value":
            saveValue(data, db)
        elif type == "new-value":
            saveRead(data, db)
        elif type == "event":
            eventHandler(data, db)
        elif type == "hello":
            checkAndAddToDatabase(data, db)
    finally:
        db.close()


if __name__ == '__main__':
    mqtt.init_app(app)
    app.run(host='192.168.0.20', port=4455, use_reloader=False, debug=False)
