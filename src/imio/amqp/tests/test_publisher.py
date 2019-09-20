# -*- coding: utf-8 -*-

from imio.amqp.event import ConnectionOpenedEvent
from imio.amqp.event import add_subscriber
from imio.amqp.event import remove_subscriber
from imio.amqp.publisher import BasePublisher
from imio.amqp.publisher import BaseSingleMessagePublisher
from imio.amqp.tests.base import RabbitMQManager

import unittest


class TestPublisher(BasePublisher):
    exchange = "imio.amqp.test"
    batch_interval = 4

    @property
    def stop_timeout(self):
        return getattr(self, "_stop_timeout", 4)

    @stop_timeout.setter
    def stop_timeout(self, value):
        self._stop_timeout = value

    def add_messages(self):
        return ["B", "B", "B"]

    def get_routing_key(self, message):
        return {"A": "AA", "B": "BB"}.get(message, "AA")


class TestSingleMessagePublisher(BaseSingleMessagePublisher):
    exchange = "imio.amqp.test"

    def get_routing_key(self, message):
        return "CC"


def after_connection_open(event):
    event.context._connection.add_timeout(
        event.context.stop_timeout, event.context.stop
    )


class TestBasePublisher(unittest.TestCase):
    def setUp(self):
        self._amqp = RabbitMQManager()
        self._amqp.declare_exchange("imio.amqp.test", "direct", durable="true")

        connection = (
            "amqp://guest:guest@127.0.0.1:5672/%2F?"
            "connection_attempts=3&heartbeat_interval=3600"
        )
        self._publisher = TestPublisher(connection, logging=False)
        add_subscriber(ConnectionOpenedEvent, after_connection_open)

    def tearDown(self):
        remove_subscriber(ConnectionOpenedEvent, after_connection_open)
        self._amqp.delete_queue("imio.amqp.pub1queue")
        self._amqp.delete_queue("imio.amqp.pub2queue")
        self._amqp.cleanup()

    def test_setup_queue(self):
        self._publisher.setup_queue("imio.amqp.pub1queue", "AA")
        self._publisher.setup_queue("imio.amqp.pub2queue", "BB")
        self._publisher.start()
        self.assertIn("imio.amqp.pub1queue", self._amqp.queues)
        self.assertIn("imio.amqp.pub2queue", self._amqp.queues)

    def test_single_publisher(self):
        self._publisher.setup_queue("imio.amqp.pub1queue", "AA")
        self._publisher._messages = ["A", "A"]
        self._publisher.stop_timeout = 1
        self._publisher.start()
        self.assertEqual(2, self._amqp.messages_number("imio.amqp.pub1queue"))
        self.assertEqual(0, len(self._publisher._published_messages.keys()))

    def test_multiple_publisher(self):
        self._publisher.setup_queue("imio.amqp.pub1queue", "AA")
        self._publisher.setup_queue("imio.amqp.pub2queue", "BB")
        self._publisher._messages = ["A", "B"]
        self._publisher.stop_timeout = 1
        self._publisher.start()
        self.assertEqual(1, self._amqp.messages_number("imio.amqp.pub1queue"))
        self.assertEqual(1, self._amqp.messages_number("imio.amqp.pub2queue"))

    def test_add_message(self):
        self._publisher.setup_queue("imio.amqp.pub2queue", "BB")
        self._publisher.stop_timeout = 6
        self._publisher.start()
        self.assertEqual(3, self._amqp.messages_number("imio.amqp.pub2queue"))


class TestBaseSingleMessagePublisher(unittest.TestCase):
    def setUp(self):
        self._amqp = RabbitMQManager()
        self._amqp.declare_exchange("imio.amqp.test", "direct", durable="true")

        connection = (
            "amqp://guest:guest@127.0.0.1:5672/%2F?"
            "connection_attempts=3&heartbeat_interval=3600"
        )
        self._publisher = TestSingleMessagePublisher(connection, logging=False)

    def tearDown(self):
        self._amqp.cleanup()

    def test_publish(self):
        self._publisher.setup_queue("imio.amqp.pub1queue", "CC")
        self._publisher.add_message("C")
        self._publisher.start()
        self.assertEqual(1, self._amqp.messages_number("imio.amqp.pub1queue"))
