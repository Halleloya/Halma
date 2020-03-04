# -*- coding: utf-8 -*-  

import paho.mqtt.client as mqtt
import RPi.GPIO as GPIO
import json
  
# BCM GPIO
pins = [17,18,27,22,23,24,25,4]
def gpio_setup():
    GPIO.setmode(GPIO.BCM)
    for pin in pins:
        GPIO.setup(pin, GPIO.OUT)
        GPIO.output(pin, GPIO.LOW)
         
def gpio_destroy():
    for pin in pins:
        GPIO.output(pin, GPIO.LOW)
        GPIO.setup(pin, GPIO.IN)
         
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("topic/gpio")
  
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload.decode()))
    gpio = json.loads(str(msg.payload.decode()))
    print (gpio)
  
    if gpio['pin'] in pins:
        if gpio['value'] == 0:
            GPIO.output(gpio['pin'], GPIO.LOW)
            print ('Turned off!')
        else:
            GPIO.output(gpio['pin'], GPIO.HIGH)
            print ('Turned on!')
  
if __name__ == '__main__':
    client = mqtt.Client('id-sub-0001')
    client.on_connect = on_connect
    client.on_message = on_message
    gpio_setup()
     
    try:
        client.connect("111.111.111.1", 1883, 60)
        client.loop_forever()
    except KeyboardInterrupt:
        client.disconnect()
        gpio_destroy()
