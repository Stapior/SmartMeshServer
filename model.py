import enum


class MessageType(enum.Enum):
    Read = 'read'
    ReadValue = 'read-value'
    NewValue = "new-value"
    Event = 'event'


class MsgFields(enum.Enum):
    ObjectId = "objectId"
    NodeId = "nodeId"
    ObjectType = "objectType"
    Value = "value"
    Type = "type"


class ObjectType(enum.Enum):
    Relay = "relay"  # value 0/1
    DimmableSwitch = "dimmableSwitch"  # value 0...1024
    AppEvent = "appEvent"  # trigger scenario from app
    Switch = "switch"  # trigger scenario
    RemoteSwitch = "remote-switch"  # trigger scenario
    TempSensor = "tempSensor"  # read temperature values, value in *C
    HumiditySensor = "humiditySensor"  # read humidity values, value in %
    LuxSensor = "luxSensor"  # read light intensity, value in in 1 ... ~10000