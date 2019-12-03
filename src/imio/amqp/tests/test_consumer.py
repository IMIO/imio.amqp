# -*- coding: utf-8 -*-

from imio.amqp.consumer import BaseConsumer
from imio.amqp.consumer import BaseSingleMessageConsumer
from imio.amqp.event import ConnectionOpenedEvent
from imio.amqp.event import add_subscriber
from imio.amqp.event import _subscribers
from imio.amqp.tests.base import RabbitMQManager
from pika.exceptions import ChannelClosed

import cPickle
import time
import unittest


class TestConsumer(BaseConsumer):
    exchange = "imio.amqp.test"
    queue = "imio.amqp.conqueue"
    routing_key = "AA"

    def treat_message(self, message):
        if not hasattr(self, "_messages"):
            self._messages = []
        self._messages.append(message)


class TestSingleMessageConsumer(BaseSingleMessageConsumer):
    exchange = "imio.amqp.test"
    queue = "imio.amqp.conqueue"
    routing_key = "AA"


def after_connection_open(event):
    event.context._connection.add_timeout(4, event.context.stop)


class TestBaseConsumer(unittest.TestCase):
    def setUp(self):
        self._amqp = RabbitMQManager()
        self._amqp.declare_exchange("imio.amqp.test", "direct", durable="true")
        self._amqp.declare_queue("imio.amqp.conqueue", durable="true")
        self._amqp.declare_bind(
            "imio.amqp.test", "imio.amqp.conqueue", routing_key="AA"
        )

        connection = (
            "amqp://guest:guest@127.0.0.1:5672/%2F?"
            "connection_attempts=3&heartbeat_interval=3600"
        )
        self._consumer = TestConsumer(connection, logging=False)
        add_subscriber(ConnectionOpenedEvent, after_connection_open)

    def tearDown(self):
        _subscribers[:] = []
        self._amqp.cleanup()

    def test_consuming(self):
        self._amqp.publish_message("imio.amqp.test", "AA", "foo")
        self._amqp.publish_message("imio.amqp.test", "AA", "bar")
        self.assertIsNone(getattr(self._consumer, "_messages", None))
        self.assertRaises(ChannelClosed, self._consumer.start)
        self.assertEqual(["foo", "bar"], self._consumer._messages)


class TestBaseSingleMessageConsumer(unittest.TestCase):
    def setUp(self):
        self._amqp = RabbitMQManager()
        self._amqp.declare_exchange("imio.amqp.test", "direct", durable="true")
        self._amqp.declare_queue("imio.amqp.conqueue", durable="true")
        self._amqp.declare_bind(
            "imio.amqp.test", "imio.amqp.conqueue", routing_key="AA"
        )

        connection = (
            "amqp://guest:guest@127.0.0.1:5672/%2F?"
            "connection_attempts=3&heartbeat_interval=3600"
        )
        self._consumer = TestSingleMessageConsumer(connection, logging=False)

    def tearDown(self):
        self._amqp.cleanup()

    def test_consumer(self):
        self._amqp.publish_message("imio.amqp.test", "AA", "foo")
        self._amqp.publish_message("imio.amqp.test", "AA", "bar")
        self._consumer.start()
        message = cPickle.loads(self._consumer.get_message())
        self._consumer.acknowledge_message()
        self.assertEqual("foo", message)
        time.sleep(1.5)
        self.assertEqual(1, self._amqp.messages_number("imio.amqp.conqueue"))
