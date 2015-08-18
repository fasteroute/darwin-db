from stomp.connect import StompConnection12

import logging
log = logging.getLogger("darwindb")

##### Code for STOMP debugging
#import logging
#console = logging.StreamHandler()
#console.setFormatter(logging.Formatter('[%(asctime)s] %(name)-12s %(levelname)-8s %(message)s'))
#logging.getLogger().addHandler(console)
#logging.getLogger().setLevel(logging.DEBUG)
#LOGGER = logging.getLogger('stomp')
#####

def has_method(_class, _method):
    return callable(getattr(_class, _method, None))

class Client:
    def connect(self, server, port, user, password, queue, listener):
        log.debug("StompClient.connect()")
        
        self.cb = listener

        self.conn = StompConnection12([(server, port)], auto_decode=False)
        self.conn.set_listener('', self)
        self.conn.start()
        self.conn.connect(user, password)
        self.conn.subscribe(queue, ack='client', id='1')

    def on_error(self, headers, message):
        log.debug("StompClient.onError(headers={}, message={})".format(headers, message))
        
        if has_method(self.cb, "on_error"):
            self.cb.on_error(headers, message)

    def on_connecting(self, host_and_port):
        log.debug("StompClient.onConnecting(host_and_port={})".format(host_and_port))

        if has_method(self.cb, "on_connecting"):
            self.cb.on_connecting(host_and_port)

    def on_connected(self, headers, body):
        log.debug("StompClient.onConnected(headers={}, body={})".format(headers, body))

        if has_method(self.cb, "on_connected"):
            self.cb.on_connected(headers, body)

    def on_disconnected(self):
        log.debug("StompClient.onDisconnected()")

        if has_method(self.cb, "on_disconnected"):
            self.cb.on_disconnected()

    def on_message(self, headers, message):
        log.debug("StompClient.onMessage(headers={}, body=<truncated>)".format(headers))

        if has_method(self.cb, "on_message"):
            self.cb.on_message(headers, message)

    def ack(self, message_id):
        self.conn.ack(message_id)


