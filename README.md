# Halma

## Demo

> run `mosquitto -c conf` on both nuc6 and nuc7, thus we set up two proxys. Or, we might want to log it using `mosquitto -c conf &> log`.

> run `python3 subscriber_1.py` on pi to subscribe gpio topic of nuc6.

> run `python3 basicBroker.py` on nuc6 to subscribe gpio topic of its neighbor nuc7.

> run `python3 basicPublisher.py [0/1]` on any machine (e.g. pi) to publish message to nuc7.

> We would be able to see the light on or off with the control messages. 


## Progress:

I am trying to get rid of mosquitto, using hbmqtt instead.
