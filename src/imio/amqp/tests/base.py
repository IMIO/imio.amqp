# -*- coding: utf-8 -*-

from pika.exceptions import ChannelClosedByBroker

import pickle
import pika


URL = "amqp://guest:guest@127.0.0.1:5672/%2F"


class RabbitMQManager(object):
    """Base class to interract with RabbitMQ"""

    def __init__(self):
        self.declared_exchanges = []
        self.declared_queues = []

    def cleanup(self):
        for exchange in self.declared_exchanges:
            self.delete_exchange(exchange)
        for queue in self.declared_queues:
            self.delete_queue(queue)
        self.declared_exchanges = []
        self.declared_queues = []

    def _call(self, method, *args, **kwargs):
        """Call a channel method on its own connection, so that a broker error
        (e.g. a missing queue closes the channel) does not break the next calls"""
        connection = pika.BlockingConnection(pika.URLParameters(URL))
        try:
            return getattr(connection.channel(), method)(*args, **kwargs)
        finally:
            if connection.is_open:
                connection.close()

    def publish_message(self, exchange, routing_key, message):
        self._call("basic_publish", exchange, routing_key, pickle.dumps(message))

    def declare_exchange(self, name, type, durable=False):
        self._call("exchange_declare", name, exchange_type=type, durable=durable)
        self.declared_exchanges.append(name)

    def declare_queue(self, name, durable=False):
        self._call("queue_declare", name, durable=durable)
        self.declared_queues.append(name)

    def declare_bind(self, exchange, queue, routing_key=None):
        self._call("queue_bind", queue, exchange, routing_key=routing_key)

    def delete_exchange(self, name):
        """Delete an exchange, RabbitMQ accepts a missing one"""
        self._call("exchange_delete", name)

    def delete_queue(self, name):
        """Delete a queue, RabbitMQ accepts a missing one"""
        self._call("queue_delete", name)

    def queue_exists(self, name):
        try:
            self._call("queue_declare", name, passive=True)
        except ChannelClosedByBroker:
            return False
        return True

    def messages_number(self, queue):
        """Return the number of messages in a queue"""
        try:
            result = self._call("queue_declare", queue, passive=True)
        except ChannelClosedByBroker:
            return 0
        return result.method.message_count
