'''
This is the Publisher
'''

#!/usr/bin/env python3

import sys
import paho.mqtt.client as mqtt
import time
import json

def on_publish(client, userdata, mid):
    print ("publish callback!")
    #pass

def on_disconnect(client, userdata, rc):
    print ("disconnect callback!")


def main():

    client = mqtt.Client('id-pub-0001')
    client.connect("111.111.111.1", 1883, 60)
    client.loop_start()
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    value = int(sys.argv[1])
    msg = {"pin":17, "value":value}
    ret = client.publish("topic/gpio", json.dumps(msg), qos=2, retain=False);
    print (ret)

    #ret.wait_for_publish()
    if ret.is_published():
        print ("is published")
    
    '''
    try:
        client.loop()
    except KeyboardInterrupt:
        client.disconnect()
    '''
    client.loop_stop()
    client.disconnect()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print ('input 0 to turn off, 1 to turn on.')
        sys.exit()
    
    main()
