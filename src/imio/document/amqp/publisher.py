# -*- coding: utf-8 -*-
import cPickle
import logging
import os
import pika

from logging.handlers import TimedRotatingFileHandler


class BasePublisher(object):
    queue = None
    exchange = 'imiodocument'
    exchange_type = 'direct'
    routing_key = 'key'
    publish_interval = 1
    batch_interval = 15  # 15 seconds
    logger_name = None
    log_file = None

    def __init__(self, amqp_url):
        self._url = amqp_url

        self._connection = None
        self._channel = None
        self._closing = False
        self._messages = []
        self._message_number = 0

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

    def add_messages(self):
        """Method called to verify if there is new messages to publish"""
        raise NotImplementedError('messages_batch method must be implemented')

    def mark_message(self, message):
        """Method called when a message has been published"""
        raise NotImplementedError('mark_message method must be implemented')

    def connect(self):
        """Open an return the connection to RabbitMQ"""
        self._logger.info('Connecting to %s' % self._url)
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
                                 '(%s) %s' % (reply_code, reply_text))
            self._connection.add_timeout(5, self.reconnect)

    def on_connection_open(self, connection):
        """Called when the connection to RabbitMQ is established"""
        self._logger.info('Connection opened')
        self.open_channel()

    def reconnect(self):
        """Called by IOLoop timer if the connection is closed"""
        self._connection.ioloop.stop()
        self._connection = self.connect()
        self._connection.ioloop.start()

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Called when RabbitMQ unexpectedly closes the channel"""
        self._logger.warning('Channel was closed: (%s) %s' % (reply_code,
                                                              reply_text))
        if not self._closing:
            self._connection.close()

    def on_channel_open(self, channel):
        """Called when the channed has been opened"""
        self._logger.info('Channel opened')
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self._channel.exchange_declare(self.on_exchange_declared,
                                       self.exchange,
                                       self.exchange_type)

    def on_exchange_declared(self, response_frame):
        """Called when RabbitMQ has finished the exchange declare"""
        self._channel.queue_bind(self.on_bind, self.queue, self.exchange,
                                 self.routing_key)

    def on_bind(self, response_frame):
        """Called when the queue is ready to received messages"""
        self.start_publishing()

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
        """Start the publishing process"""
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Stop the publishing process"""
        self._logger.info('Stopping')
        self._closing = True
        self.close_channel()
        self.close_connection()
        # Allow the publisher to cleanly disconnect from RabbitMQ
        self._connection.ioloop.start()
        self._logger.info('Stopped')

    def _publish(self):
        """Publish a message"""
        if self._closing is True:
            return
        if len(self._messages) == 0:
            self._connection.add_timeout(self.batch_interval,
                                         self._add_messages)
            return

        message = self._messages.pop(0)
        body = cPickle.dumps(message)
        self._message_number += 1
        self._channel.basic_publish(self.exchange, self.routing_key, body)
        self.mark_message(message)
        self._logger.info('Published message #%d' % self._message_number)
        self.schedule_next_message()

    def _add_messages(self):
        self._messages.extend(self.add_messages())
        self.schedule_next_message()

    def start_publishing(self):
        """Begin the publishing of messages"""
        self._logger.info('Begin the publishing of messages')
        self.add_messages()
        self.schedule_next_message()

    def schedule_next_message(self):
        """Schedule the next message to be delivered"""
        if self._closing is True:
            return
        self._connection.add_timeout(self.publish_interval,
                                     self._publish)
