from darwindb import Client

from darwindb.postgres import Connection as PostgresConnection
from darwindb.postgres import ScheduleMessageStore, TrainStatusMessageStore

import json
import os
import time

class Listener:
    def __init__(self, client):
        self.connection = PostgresConnection("172.17.0.2", "postgres", "postgres", "mysecretpassword")
        
        self.schedule_store = ScheduleMessageStore(self.connection)
        self.train_status_store = TrainStatusMessageStore(self.connection)

        self.client = client
        
    def on_connected(self, headers, body):
        self.connection.connect()
        self.schedule_store.create_tables()
        self.train_status_store.create_tables()
    
    def on_message(self, headers, message):
        m = json.loads(message.decode("utf-8"))

        if "schedule_messages" in m and len(m["schedule_messages"]) > 0:
            #print("Message includes schedules:")
            pass
        for s in m["schedule_messages"]:
            #print ("    "+s["rid"])
            self.schedule_store.save_schedule_message(s)

        for s in m["train_status_messages"]:
            self.train_status_store.save_train_status_message(s)

        # Now we have finished processing, ack the message.
        self.client.ack(headers["ack"])

c = Client()
l = Listener(c)

c.connect(os.environ["STOMP_HOST"], 61613, "admin", "pass", "Consumer.Example.VirtualTopic.PushPortJson", l)

while True:
    time.sleep(1)


