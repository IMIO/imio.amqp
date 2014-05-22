# -*- coding: utf-8 -*-

import logging
import os
import pika

from logging.handlers import TimedRotatingFileHandler


class AMQPConnector(object):
    queue = None
    exchange = 'imiodocument'
    exchange_type = 'direct'
    routing_key = 'key'
    logger_name = None
    log_file = None

    def __init__(self, amqp_url, logging=True):
        self._url = amqp_url

        self._connection = None
        self._channel = None
        self._closing = False

        if logging is True:
            self._set_logger()

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
        fh = TimedRotatingFileHandler(os.path.join('.', self.log_file),
                                      'midnight', 1)
        fh.suffix = "%Y-%m-%d-%H-%M"
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s '
                                      '- %(message)s')
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        self._logger.addHandler(fh)

    def connect(self):
        """Open and return the connection to RabbitMQ"""
        self._logger.info('Connecting to {0!s}'.format(self._url))
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self.on_connection_open)

    def close_connection(self):
        """Close the connection to RabbitMQ"""
        self._logger.info('Closing connection')
        self._connection.close()

    def on_connection_closed(self, connection, reply_code, reply_text):
        """Called when the connection to RabbitMQ is closed unexpectedly"""
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._logger.warning('Connection closed, reopening in 5 seconds: '
                                 '({0!s}) {1!s}'.format(reply_code,
                                                        reply_text))
            self._connection.add_timeout(5, self.reconnect)

    def on_connection_open(self, connection):
        """Called when the connection to RabbitMQ is established"""
        self._logger.info('Connection opened')
        self._connection.add_on_close_callback(self.on_connection_closed)
        self.open_channel()

    def reconnect(self):
        """Called by IOLoop timer if the connection is closed"""
        self._connection.ioloop.stop()
        self._connection = self.connect()
        self._connection.ioloop.start()

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Called when RabbitMQ unexpectedly closes the channel"""
        self._logger.warning('Channel was closed: ({0!s}) {1!s}'.format(
            reply_code, reply_text))
        if not self._closing:
            self._connection.close()

    def on_channel_open(self, channel):
        """Called when the channed has been opened"""
        self._logger.info('Channel opened')
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self._channel.exchange_declare(self.on_exchange_declared,
                                       self.exchange,
                                       self.exchange_type,
                                       durable=True)

    def on_exchange_declared(self, response_frame):
        """Called when RabbitMQ has finished the exchange declare"""
        self._channel.queue_declare(self.on_queue_declared, self.queue,
                                    durable=True)

    def on_queue_declared(self, method_frame):
        """Called when a queue has been configured"""
        self._channel.queue_bind(self.on_bind, self.queue, self.exchange,
                                 self.routing_key)

    def on_bind(self, response_frame):
        """Called when the queue is ready to received or consumed messages"""
        raise NotImplementedError('on_bind method must be implemented')

    def start_publishing(self):
        """Hook for publishing the messages"""

    def start_consuming(self):
        """Hook for consuming the messages"""

    def close_channel(self):
        """Close the channel with RabbitMQ"""
        self._logger.info('Closing the channel')
        if self._channel:
            self._channel.close()

    def open_channel(self):
        """Open a new channel with RabbitMQ"""
        self._logger.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def start(self):
        """Start the process"""
        self._connection = self.connect()
        self._connection.ioloop.start()