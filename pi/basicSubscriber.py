#!/usr/bin/env python3

import paho.mqtt.client as mqtt
#import logging
#logging.basicConfig(level=logging.DEBUG)
# This is the subscriber

def on_connect (client, userdata, flags, rc):
    print ("Connected with result code " + str(rc))
    #client.subscribe("$SYS/broker/publish/#")
    client.subscribe( "topic/gpio", qos=2) # can subscribe a list of tuple

def on_message(client, userdata, msg):
    rcv_msg =  msg.payload.decode()
    print (msg.topic + ": " +  rcv_msg)
    # client.disconnect()

def on_log(client, userdata, level, buf):
    print ('log:' , buf)

#def on_subscribe()
#def on_unsubscribe()
#message_callback_add(sub, callback) # handle callback with filter

client = mqtt.Client("lyhao")
client.reinitialise("lyhao")
client.reconnect_delay_set(min_delay=1, max_delay=120)
'''
logger = logging.getLogger(__name__)
client.enable_logger(logger)
'''
# something else can be done: inflight message, ongoing message queuesize.
# client.max_inflight_messages_set(1)
client.connect( "111.111.111.1", 1883, 60)

#publish (topic, payload=None, qos=0, retain=False)

client.on_connect = on_connect
client.on_message = on_message
#client.on_subscribe = on_subscribe
client.on_log = on_log
#client.on_subscribe = on_subscribe
#client.unsubscribe("topic/gpio")
#on_unsubscribe()
client.loop_forever()
