from darwindb import Client

from darwindb.postgres import Connection as PostgresConnection
from darwindb.postgres import ScheduleMessageStore, TrainStatusMessageStore

import json
import time

class Listener:
    def __init__(self):
        self.connection = PostgresConnection("172.17.0.2", "postgres", "postgres", "mysecretpassword")
        
        self.schedule_store = ScheduleMessageStore(self.connection)
        self.train_status_store = TrainStatusMessageStore(self.connection)
        
    def on_connected(self, headers, body):
        self.connection.connect()
        self.schedule_store.create_tables()
        self.train_status_store.create_tables()
    
    def on_message(self, header, message):
        m = json.loads(message.decode("utf-8"))

        if "schedule_messages" in m and len(m["schedule_messages"]) > 0:
            #print("Message includes schedules:")
            pass
        for s in m["schedule_messages"]:
            #print ("    "+s["rid"])
            self.schedule_store.save_schedule_message(s)

        for s in m["train_status_messages"]:
            self.train_status_store.save_train_status_message(s)

l = Listener()

c = Client()
c.connect("172.17.0.1", 61613, "admin", "pass", "Consumer.Example.VirtualTopic.PushPortJson", l)

while True:
    time.sleep(1)


