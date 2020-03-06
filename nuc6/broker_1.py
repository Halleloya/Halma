import logging
import asyncio
import os
import yaml
from hbmqtt.broker import Broker
import paho.mqtt.client as mqtt
from multiprocessing import Process


logger = logging.getLogger(__name__)

with open("config.yml", 'r') as cfgfile:
    config = yaml.load(cfgfile, Loader=yaml.FullLoader)

@asyncio.coroutine
def broker_coro():
    broker = Broker(config)
    yield from broker.start()

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
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    asyncio.get_event_loop().run_until_complete(broker_coro())
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


if __name__ == '__main__':
    Process (target = start_broker).start()
    print ("66666666666666666666666666666666666666") 
    Process (target = start_client).start()
    print ("77777777777777777777777777777777777777")
    
