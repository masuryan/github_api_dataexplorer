from datetime import datetime
import os
import requests
import json
import time
import logging
import configparser
from azure.eventhub import EventHubProducerClient, EventData

config_data = configparser.ConfigParser()
config_data.read("config-prod.ini")
# Logging
logging.basicConfig(filename='GIT_Events.log', level=logging.INFO)


def info(message):
    # logging.info("{} | {}".format("INFO", message))
    print("{} | {}".format("INFO", message))


def error(message, err):
    # logging.error("{} | {} | {}".format("ERROR", message, err))
    print("{} | {} | {}".format("ERROR", message, err))
# end of Logging


class SlidingCache:
    def __init__(self, max_size=500):
        self.prev = set()
        self.current = set()
        self.max_size = max_size

    def add(self, item):
        if item not in self:
            if len(self.current) > self.max_size:
                self.prev = self.current
                self.current = set()

            self.current.add(item)

    def __contains__(self, item):
        return item in self.current or item in self.prev

# Make Sure you have the configuration file (.ini) has the proper settings


connection_str = config_data["EventHub"]["Connectionstring"]
eventhub_name = config_data["EventHub"]["EventhubName"]
token = config_data["GitHub"]["Token"]
ENDPOINT = config_data["GitHub"]["ENDPOINT"]
client = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name)
headers = {"Authorization": "token {}".format(token)}
serializer = json.dumps
item_seperator = "\n"
seconds_per_request = round(
        1.0 / (5000 / 60 / 60), 2
    )
cache = SlidingCache()
events = 0
loop = True


while loop:
    count = 0
    loop_start_time = time.time()
    event_data_batch = client.create_batch()
    try:

        resp = requests.get(ENDPOINT, headers=headers)
        resp.raise_for_status()
        data = sorted(resp.json(), key=lambda x: x["id"])

        for d in data:
            if d["id"] not in cache:
                try:

                    _item = "Id:" + d["id"]
                    event_data_batch.add(EventData(json.dumps(d)))
                    print(EventData(json.dumps(_item)))
                except Exception as e:
                    error("EventHubError", repr(e))  # EventDataBatch object reaches max_size.
            cache.add(d.get("id"))
        with client:
            client.send_batch(event_data_batch)
        cycle_took = time.time() - loop_start_time

        delay = seconds_per_request - cycle_took
        info("CYCLE DONE | took {}, waiting for {}".format(cycle_took, max(delay, 0)))
        if delay > 0:
            time.sleep(delay)

    except requests.HTTPError as e:
        if e.errno in [429, 403]:
            time_to_wait = int(
                float(e.response.get("X-RateLimit-Reset", 60)) - datetime.now().utcnow().timestamp()
            )
            info("waiting for {}".format(time_to_wait))
            if time_to_wait > 0:
                time.sleep(time_to_wait)
        error("HTTP EXCEPTION", repr(e))

    except Exception as e:
        error("UNEXPECTED ERROR", repr(e))

os.kill(os.getpid(), 9)
