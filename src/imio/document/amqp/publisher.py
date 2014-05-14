# -*- coding: utf-8 -*-
import cPickle

from imio.document.amqp.base import AMQPConnector


class BasePublisher(AMQPConnector):
    publish_interval = 1
    batch_interval = 15  # 15 seconds

    def __init__(self, amqp_url):
        super(BasePublisher, self).__init__(amqp_url)

        self._messages = []
        self._message_number = 0

    def mark_message(self, message):
        """Method called when a message has been published or consumed"""
        raise NotImplementedError('mark_message method must be implemented')

    def add_messages(self):
        """Method called to verify if there is new messages to publish"""
        raise NotImplementedError('messages_batch method must be implemented')

    def on_bind(self, response_frame):
        """Called when the queue is ready to received messages"""
        self.start_publishing()

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

    def stop(self):
        """Stop the publishing process"""
        self._logger.info('Stopping')
        self._closing = True
        self.close_channel()
        self.close_connection()
        # Allow the publisher to cleanly disconnect from RabbitMQ
        self._connection.ioloop.start()
        self._logger.info('Stopped')
