from darwindb import Client

from darwindb.stores import PostgresConnection, PostgresStore

import json
import os
import time

class Listener:
    def __init__(self, client):
        print("Setting up listener")
        self.connection = PostgresConnection(host=os.environ["POSTGRES_HOST"],
                                             dbname=os.environ["POSTGRES_DB"],
                                             user=os.environ["POSTGRES_USER"],
                                             password=os.environ["POSTGRES_PASS"])
        print("Connected to Postgres. Now instantiating Postgres Store")
        self.store = PostgresStore(self.connection)
        self.client = client
        
    def on_connected(self, headers, body):
        print("On Connected")
        self.connection.connect()
        self.store.create_tables()
    
    def on_message(self, headers, message):
        m = json.loads(message.decode("utf-8"))

        snapshot = False
        if m["message_type"] == "snapshot":
            snapshot = True

        for s in m["schedule_messages"]:
            self.store.save_schedule_message(s, snapshot)

        for s in m["association_messages"]:
            self.store.save_association_message(s, snapshot)

        for d in m["deactivated_messages"]:
            self.store.save_deactivated_message(d, snapshot)

        for s in m["train_status_messages"]:
            self.store.save_train_status_message(s, snapshot)

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


