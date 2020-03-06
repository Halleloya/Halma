import logging
import asyncio
import os
import yaml
from hbmqtt.broker import Broker

logger = logging.getLogger(__name__)

with open("config.yml", 'r') as cfgfile:
    config = yaml.load(cfgfile, Loader=yaml.FullLoader)

@asyncio.coroutine
def broker_coro():
    broker = Broker(config)
    yield from broker.start()


if __name__ == '__main__':
    formatter = "[%(asctime)s] :: %(levelname)s :: %(name)s :: %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    asyncio.get_event_loop().run_until_complete(broker_coro())
    asyncio.get_event_loop().run_forever()
