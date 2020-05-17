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


@app.route('/objects')
def getObjects():
    cur = get_db().cursor()
    cur.execute("SELECT * FROM objects")

    return json.dumps(cur.fetchall())


@app.route('/scenarios')
def getScenarios():
    cur = get_db().cursor()
    cur.execute("SELECT * FROM objects")

    return json.dumps(cur.fetchall())


@app.route('/scenarios/<objectId>')
def getScenariosForObject(objectId):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM Scenarios where objectId = ?", objectId)

    return json.dumps(cur.fetchall())


@app.route('/steps/<scenarioId>')
def getSteps(scenarioId):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM steps where scenarioId = ?", scenarioId)

    return json.dumps(cur.fetchall())


@app.route('/object/<objectId>')
def getObject(objectId):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM objects where objectId = ?", objectId)
    return json.dumps(cur.fetchone())


@app.route('/addOwnObject')
def addOwnObject():
    cur = get_db().cursor()
    objectType = request.args.get('type', default='switch')
    name = request.args.get('name', default='Sztuczny obiekt')
    description = request.args.get('description', default='Sztuczny obiekt')
    cur.execute("SELECT min(nodeId) as nodeId from objects where nodeId < 0 ")
    res = cur.fetchone()
    lastNodeId = res.get("nodeId")
    if lastNodeId is None:
        lastNodeId = 0
    newNodeId = lastNodeId - 1
    cur.execute("Insert into objects (nodeId, internalId, type, name, description) values (?, 1, ?, ?, ?)",
                (newNodeId, objectType, name, description))
    get_db().commit()

    cur.execute("SELECT * FROM objects where nodeId  = ?", (newNodeId,))
    obj = cur.fetchone()

    return jsonify(objectId=obj["objectId"])


@app.route('/deleteScenario/<scenarioId>')
def deleteScenario(scenarioId):
    cur = get_db().cursor()
    cur.execute("DELETE FROM steps where scenarioId = ?", scenarioId)
    cur.execute("DELETE FROM Scenarios where scenarioId = ?", scenarioId)

    return jsonify(success=True)


@app.route('/addScenario', methods=['POST'])
def addScenario():
    cur = get_db().cursor()
    data = request.get_json()
    objectId = data['objectId']
    condition = data['condition']
    conditionValue = data['conditionValue']
    cur.execute("Insert into Scenarios (objectId, condition, conditionValue) values (?, ?, ?)",
                (objectId, condition, conditionValue))
    get_db().commit()

    return jsonify(scenarioId=cur.lastrowid)


@app.route('/addStep', methods=['POST'])
def addStep():
    cur = get_db().cursor()
    data = request.get_json()
    toObject = data['toObject']
    scenarioId = data['scenarioId']
    newValue = data['newValue']
    delay = data['delay']
    onTime = data['onTime']
    cur.execute("Insert into steps (scenarioId, toObject, newValue, delay, onTime) values (?, ?, ?, ?, ?)",
                (scenarioId, toObject, newValue, delay, onTime))
    get_db().commit()

    return jsonify(stepId=cur.lastrowid)


@app.route('/deleteStep/<stepId>')
def deleteScenario(stepId):
    cur = get_db().cursor()
    cur.execute("DELETE FROM steps where id = ?", stepId)

    return jsonify(success=True)


@app.route('/change/<objectId>/<value>')
def changeState(objectId, value, db=None):
    if db is None:
        db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM objects where objectId = ?", (objectId,))
    obj = cur.fetchone()
    msg = {"nodeId": int(obj["nodeId"]),
           "objectId": int(obj["internalId"]),
           "type": "change",
           "value": int(value)}
    mqtt.publish("painlessMesh/to/" + str(obj["nodeId"]), json.dumps(msg))
    return jsonify(success=True)


@app.route('/read/<objectId>')
def readState(objectId, value):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM objects where objectId = ?", objectId)
    obj = cur.fetchone()
    msg = {"nodeId": int(obj["nodeId"]),
           "objectId": int(obj["internalId"]),
           "type": "change",
           "value": int(value)}
    mqtt.publish("painlessMesh/to/" + obj["nodeId"], json.dumps(msg))
    return jsonify(success=True)


@app.route('/reads/<objectId>')
def getReads(objectId):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM reads where objectId = ?", objectId)
    obj = cur.fetchall()
    return json.dumps(obj)


@app.route('/events/<objectId>')
def getEvents(objectId):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM events where objectId = ?", objectId)
    obj = cur.fetchall()
    return json.dumps(obj)


@app.route('/emitEvent/<objectId>')
def emitEvent(objectId):
    value = request.args.get('value', default=1)
    processEvent(objectId, value, get_db())
    return jsonify(success=True)


@app.route('/update/<objectId>')
def updateObject(objectId):
    cur = get_db().cursor()
    cur.execute("SELECT * FROM objects where objectId = ?", objectId)
    obj = cur.fetchone()
    name = request.args.get('name', default=obj["name"])
    description = request.args.get('description', default=obj["description"])
    objType = request.args.get('type', default=obj["type"])
    cur.execute("Update objects set name = ?, description = ?, type = ? where objectId = ? ",
                (name, description, objType, objectId))
    get_db().commit()
    return jsonify(success=True)


@mqtt.on_connect()
def on_connect(client, userdata, flags, rc):
    log("Connected with result code " + str(rc))
    mqtt.subscribe("painlessMesh/from/#")


def saveValue(data, db):
    log(">>saveValue")
    cur = db.cursor()
    cur.execute(
        'Update objects set value = ? where nodeId =? and internalId = ? ',
        (data["value"], data["nodeId"], data["objectId"]))
    db.commit()
    log("saveValue<<")


def checkAndAddToDatabase(data, db):
    cur = db.cursor()
    cur.execute("SELECT count(*) as cnt from objects where nodeId = ? and internalId = ?",
                (data["nodeId"], data["objectId"]))
    if cur.fetchone()["cnt"] == 0:
        cur.execute("Insert into objects (nodeId, internalId, type) values (?, ?, ?)",
                    (data["nodeId"], data["objectId"], data.get("objectType", "switch")))
        db.commit()
        log(">>new Object add<<")


def saveRead(data, db):
    cur = db.cursor()
    cur.execute("SELECT objectId from objects where nodeId =? and internalId = ?",
                (data["nodeId"], data["objectId"]))
    obj = cur.fetchone()
    cur.execute(
        'Insert into reads (objectId, value) values (?,?)',
        (obj["objectId"], data["value"]))
    db.commit()


def eventHandler(data, db):
    cur = db.cursor()
    cur.execute("SELECT objectId from objects where nodeId =? and internalId = ?",
                (data["nodeId"], data["objectId"]))
    obj = cur.fetchone()
    objId = obj["objectId"]
    value = data["value"]
    cur.execute(
        'Insert into events (objectId, value) values (?,?)',
        (objId, value))
    log("event hander")
    processEvent(objId, value, db)
    db.commit()


def processEvent(objectId, value, db):
    cur = db.cursor()
    cur.execute("Select * from Scenarios where objectId = ?", (objectId,))
    scenarios = cur.fetchall()
    for scenario in scenarios:
        if checkScenarioCondition(scenario, value):
            cur.execute("Select * from steps where scenarioId = ?", (scenario["scenarioId"],))
            steps = cur.fetchall()
            for step in steps:
                delay = step.get("delay")
                if delay is not None:
                    t = Timer(delay, executeStep, [step])
                    t.start()
                else:
                    executeStep(step)


def executeStep(step):
    db = get_mqtt_db()
    targetObjectId = step["toObject"]
    cur = get_db().cursor()
    cur.execute("SELECT * FROM objects where objectId = ?", targetObjectId)
    obj = cur.fetchone()
    previousValue = obj.get("value", default=0)
    value = step["newValue"]
    changeState(targetObjectId, value, db)
    onTime = step.get("onTime")
    if onTime is not None:
        t = Timer(onTime, switchOff, [targetObjectId, previousValue])
        t.start()


def switchOff(objId, previousValue):
    db = get_mqtt_db()
    changeState(objId, previousValue, db)


def checkScenarioCondition(scenario, value):
    condition = scenario.get("condition")
    conditionValue = scenario.get("conditionValue")
    if condition is not None and conditionValue is not None:
        if condition == "=":
            return value == conditionValue
        if condition == "<=":
            return value <= conditionValue
        if condition == ">=":
            return value >= conditionValue
        if condition == "<":
            return value < conditionValue
        if condition == ">":
            return value > conditionValue
    return True


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
