import logging
import asyncio
import os
import yaml
from hbmqtt.broker import Broker
import paho.mqtt.client as mqtt
from multiprocessing import Process


logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
'''
handler = logging.FileHandler("log.txt")
handler.setLevel(logging.INFO)
'''

with open("config.yml", 'r') as cfgfile:
    config = yaml.load(cfgfile, Loader=yaml.FullLoader)

@asyncio.coroutine
def broker_coro():
    broker = Broker(config)
    yield from broker.start()

@asyncio.coroutine
def broker_lisn():
    while True:
        print ('Lisn Function', logger)
        print ('222222222222222222222222222222')
        break # will do somthing...

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("topic/gpio") #topic/test")

def on_message(client, userdata, msg):
    if msg.payload.decode() != None:
        rcv_msg = msg.payload.decode()
        print ('received:'+rcv_msg)
        client_trans = mqtt.Client()
        client_trans.connect("111.111.111.1", 1883, 60)
        client_trans.publish("topic/gpio", rcv_msg);
        client_trans.disconnect();
        #client.disconnect()
    
def start_broker():
    formatter = "[%(asctime)s] :: %(levelname)s :: %(name)s :: %(message)s"
    logging.basicConfig(level=logging.INFO, format=formatter)
    asyncio.get_event_loop().run_until_complete(broker_coro())
    #asyncio.get_event_loop().run_until_complete(broker_lisn())
    asyncio.get_event_loop().run_forever()

def start_client():
    client = mqtt.Client('nuc6')
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        client.connect("111.111.111.2",1883,60)
        client.loop_forever()
    except KeyboardInterrupt:
        client.disconnect()

def main():
    Process (target = start_broker).start()
    Process (target = start_client).start()

    Process (target = broker_lisn).start()

if __name__ == '__main__':
    main() 
