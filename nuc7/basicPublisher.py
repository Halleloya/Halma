#!/usr/bin/env python3

import paho.mqtt.client as mqtt
import time
# This is the Publisher

def on_publish(client, userdata, mid):
    print ("publish callback!")
    pass
def on_disconnect(client, userdata, rc):
    print ("disconnect callback!")

client = mqtt.Client()
client.connect("111.111.111.1", 1883, 60)

client.on_publish = on_publish
client.on_disconnect = on_disconnect

ret = client.publish("topic/gpio", "Hello world!", qos=2, retain=False);
print (ret)

#ret.wait_for_publish():

if ret.is_published():
    print ("is published")

try:
    client.loop_forever()
except KeyboardInterrupt:
    client.disconnect()
