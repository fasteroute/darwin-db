from darwindb import Client

from darwindb.postgres import Connection as PostgresConnection
from darwindb.postgres import ScheduleMessageStore, TrainStatusMessageStore

import json
import os
import time

class Listener:
    def __init__(self, client):
        self.connection = PostgresConnection(host=os.environ["POSTGRES_HOST"],
                                             dbname=os.environ["POSTGRES_DB"],
                                             user=os.environ["POSTGRES_USER"],
                                             password=os.environ["POSTGRES_PASS"])
        
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
        self.client.ack(headers)

c = Client()
l = Listener(c)

c.connect(server=os.environ["STOMP_HOST"],
          port=int(os.environ["STOMP_PORT"]),
          user=os.environ["STOMP_USER"],
          password=os.environ["STOMP_PASS"],
          queue=os.environ["STOMP_QUEUE"],
          listener=l)

while True:
    time.sleep(1)


