# -*- coding: utf-8 -*-

from imio.amqp.event import ConnectionOpenedEvent
from imio.amqp.event import notify
from logging.handlers import TimedRotatingFileHandler
from pika.exceptions import AMQPConnectionError

import logging
import os
import pika


class AMQPConnector(object):
    queue = None
    exchange = "imiodocument"
    exchange_type = "direct"
    routing_key = "key"
    logger_name = None
    log_file = None
    log_path = "."
    connection_cls = pika.SelectConnection
    exchange_durable = True
    queue_durable = True
    queue_auto_delete = False

    def __init__(self, amqp_url, logging=True):
        self._url = amqp_url

        self._connection = None
        self._channel = None
        self._closing = False

        if logging is True:
            self._set_logger()

    @property
    def url_parameters(self):
        return pika.URLParameters(self._url)

    def setup_queue(self, queue_name, routing_key):
        """Setup the queue"""
        self.queue = queue_name
        self.routing_key = routing_key

    def setup_exchange(self, exchange_name, exchange_type):
        """Setup the exchange"""
        self.exchange = exchange_name
        self.exchange_type = exchange_type

    def _set_logger(self):
        """Set logging"""
        self._logger = logging.getLogger(self.logger_name)
        self._logger.setLevel(logging.DEBUG)
        if self.log_file and self.log_path:
            fh = TimedRotatingFileHandler(
                os.path.join(self.log_path, self.log_file), "midnight", 1
            )
        else:
            fh = logging.StreamHandler()
        fh.suffix = "%Y-%m-%d-%H-%M"
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s " "- %(message)s"
        )
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        if not self._logger.handlers:
            self._logger.addHandler(fh)

    def _log(self, message, type="info"):
        """Log a message if a logger has been defined"""
        if hasattr(self, "_logger"):
            getattr(self._logger, type)(message)

    def connect(self):
        """Open and return the connection to RabbitMQ"""
        self._log("Connecting to {0!s}".format(self._url))
        return self.connection_cls(self.url_parameters, self.on_connection_open)

    def close_connection(self):
        """Close the connection to RabbitMQ"""
        self._log("Closing connection")
        self._connection.close()

    def on_connection_closed(self, connection, reply_code, reply_text):
        """Called when the connection to RabbitMQ is closed unexpectedly"""
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._log(
                "Connection closed, try to reopening: "
                "({0!s}) {1!s}".format(reply_code, reply_text),
                type="warning",
            )
            raise AMQPConnectionError

    def on_connection_closed_0_9_5(self, method_frame):
        """Called when the connection to RabbitMQ is closed unexpectedly
        (Pika 0.9.5)"""
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            reply_code = method_frame.reply_code
            reply_text = method_frame.reply_text
            self._log(
                "Connection closed, try to reopening: "
                "({0!s}) {1!s}".format(reply_code, reply_text),
                type="warning",
            )
            raise AMQPConnectionError

    def on_connection_open(self, connection):
        """Called when the connection to RabbitMQ is established"""
        self._log("Connection opened")
        notify(ConnectionOpenedEvent(self))
        if pika.__version__ == "0.9.5":
            self._connection.add_on_close_callback(self.on_connection_closed_0_9_5)
        else:
            self._connection.add_on_close_callback(self.on_connection_closed)
        self.open_channel()

    def reconnect(self):
        """Called by IOLoop timer if the connection is closed"""
        self._connection.ioloop.stop()
        self._connection = self.connect()
        self._connection.ioloop.start()

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Called when RabbitMQ unexpectedly closes the channel"""
        self._log(
            "Channel was closed: ({0!s}) {1!s}".format(reply_code, reply_text),
            type="warning",
        )
        if not self._closing:
            self._connection.close()

    def on_channel_closed_0_9_5(self, reply_code, reply_text):
        """Called when RabbitMQ unexpectedly closes the channel (Pika 0.9.5)"""
        self._log(
            "Channel was closed: ({0!s}) {1!s}".format(reply_code, reply_text),
            type="warning",
        )
        if not self._closing:
            self._connection.close()

    def on_channel_open(self, channel):
        """Called when the channed has been opened"""
        self._log("Channel opened")
        self._channel = channel
        if pika.__version__ == "0.9.5":
            self._channel.add_on_close_callback(self.on_channel_closed_0_9_5)
        else:
            self._channel.add_on_close_callback(self.on_channel_closed)
        parameters = {
            "callback": self.on_exchange_declared,
            "exchange": self.exchange,
            "durable": self.exchange_durable,
        }
        parameters[self.keywords("exchange_type")] = self.exchange_type
        self._channel.exchange_declare(**parameters)

    @staticmethod
    def keywords(key):
        if pika.__version__ == "0.9.5":
            return {"exchange_type": "type"}.get(key)
        return key

    def on_exchange_declared(self, response_frame):
        """Called when RabbitMQ has finished the exchange declare"""
        self._channel.queue_declare(
            callback=self.on_queue_declared,
            queue=self.queue,
            durable=self.queue_durable,
            auto_delete=self.queue_auto_delete,
        )

    def on_queue_declared(self, method_frame):
        """Called when a queue has been configured"""
        self._channel.queue_bind(
            callback=self.on_bind,
            queue=self.queue,
            exchange=self.exchange,
            routing_key=self.routing_key,
        )

    def on_bind(self, response_frame):
        """Called when the queue is ready to received or consumed messages"""
        raise NotImplementedError("on_bind method must be implemented")

    def start_publishing(self):
        """Hook for publishing the messages"""

    def start_consuming(self):
        """Hook for consuming the messages"""

    def close_channel(self):
        """Close the channel with RabbitMQ"""
        self._log("Closing the channel")
        if self._channel:
            self._channel.close()

    def open_channel(self):
        """Open a new channel with RabbitMQ"""
        self._log("Creating a new channel")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def start(self):
        """Start the process"""
        self._connection = self.connect()
        self._connection.ioloop.start()
