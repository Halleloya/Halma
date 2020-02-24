import paho.mqtt.client as mqtt

# This is the Subscriber

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
    
if __name__ == '__main__':
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        client.connect("111.111.111.2",1883,60)
        client.loop_forever()
    except KeyboardInterrupt:
        client.disconnect()
