#!/usr/bin/env python3
from flask import Flask, current_app, g, jsonify, request
from flask_mqtt import Mqtt
import json
import logging
import sqlite3

from model import MessageType, MsgFields, ObjectType

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
    name = request.args.get('name', default='Sztuczny obiekt')
    description = request.args.get('description', default='Sztuczny obiekt')
    cur.execute("SELECT min(node_id) as nodeId from objects where node_id < 0 ")
    res = cur.fetchone()
    lastNodeId = res.get("nodeId")
    if lastNodeId is None:
        lastNodeId = 0
    newNodeId = lastNodeId - 1
    cur.execute("Insert into objects (node_id, internal_id, type, name, description) values (?, 1, ?, ?, ?)",
                (newNodeId, ObjectType.AppEvent, name, description))
    get_db().commit()

    cur.execute("SELECT * FROM objects where node_id  = ?", (newNodeId,))
    obj = cur.fetchone()

    return jsonify(objectId=obj["object_id"])


@app.route('/change/<objectId>/<value>')
def changeState(objectId, value, db=None):
    if db is None:
        db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM objects where object_id = ?", (objectId,))
    obj = cur.fetchone()
    msg = {MsgFields.NodeId: int(obj["node_id"]),
           MsgFields.ObjectId: int(obj["internal_id"]),
           MsgFields.Type: "change",
           MsgFields.Value: int(value)}
    mqtt.publish("painlessMesh/to/" + str(obj["node_id"]), json.dumps(msg))
    return jsonify(success=True)


@app.route('/read/<objectId>')
def readState(objectId, value):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM objects where object_id = ?", objectId)
    obj = cur.fetchone()
    msg = {MsgFields.NodeId: int(obj["node_id"]),
           MsgFields.ObjectId: int(obj["internal_id"]),
           MsgFields.Type: "change",
           MsgFields.Value: int(value)}
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
        (data[MsgFields.Value], data[MsgFields.NodeId], data[MsgFields.ObjectId]))
    db.commit()
    log("saveValue<<")


def checkAndAddToDatabase(data, db):
    cur = db.cursor()
    cur.execute("SELECT count(*) as cnt from objects where node_id = ? and internal_id = ?",
                (data[MsgFields.NodeId], data[MsgFields.ObjectId]))
    if cur.fetchone()["cnt"] == 0:
        cur.execute("Insert into objects (node_id, internal_id, type) values (?, ?, ?)",
                    (data[MsgFields.NodeId], data[MsgFields.ObjectId], data.get(MsgFields.ObjectType, "switch")))
        db.commit()
        log(">>new Object add<<")


def saveRead(data, db):
    cur = db.cursor()
    cur.execute("SELECT object_id from objects where node_id =? and internal_id = ?",
                (data[MsgFields.NodeId], data[MsgFields.ObjectId]))
    obj = cur.fetchone()
    cur.execute(
        'Insert into reads (object_id, value) values (?,?)',
        (obj["object_id"], data[MsgFields.Value]))
    db.commit()


def eventHandler(data, db):
    cur = db.cursor()
    cur.execute("SELECT object_id from objects where node_id =? and internal_id = ?",
                (data[MsgFields.NodeId], data[MsgFields.ObjectId]))
    obj = cur.fetchone()
    objId = obj["object_id"]
    cur.execute(
        'Insert into events (object_id, value) values (?,?)',
        (objId, data[MsgFields.Value]))

    processEvent(objId, db)
    db.commit()


def processEvent(objectId, db):
    cur = db.cursor()
    cur.execute("Select * from Scenarios where object_Id = ?", (objectId,))
    scenarios = cur.fetchall()
    for scenario in scenarios:
        if True:  # TODO dodać sprawdzanie warunku
            cur.execute("Select * from steps where scenario_id = ?", (scenario["scenario_id"],))
            steps = cur.fetchall()
            for step in steps:
                targetObjectId = step["to_object"]
                value = step["new_value"]
                changeState(targetObjectId, value, db)
                # TODO dodać obsługę opoznieniemnia


@mqtt.on_message()
def on_message(client, userdata, msg):
    db = get_mqtt_db()
    try:
        log(">>New mqtt message")
        jsonString = msg.payload.decode("utf-8")
        data = json.loads(jsonString)
        log(data)
        checkAndAddToDatabase(data, db)

        msgType = data[MsgFields.Type]
        if msgType == MessageType.ReadValue:
            saveValue(data, db)
        elif msgType == MessageType.NewValue:
            saveRead(data, db)
        elif msgType == MessageType.Event:
            eventHandler(data, db)
    finally:
        db.close()


if __name__ == '__main__':
    mqtt.init_app(app)
    app.run(host='192.168.0.20', port=4455, use_reloader=False, debug=False)
