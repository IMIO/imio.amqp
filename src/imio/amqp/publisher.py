# -*- coding: utf-8 -*-
import cPickle

from imio.amqp.base import AMQPConnector


class BasePublisher(AMQPConnector):
    publish_interval = 1
    batch_interval = 15  # 15 seconds

    def __init__(self, *args, **kwargs):
        super(BasePublisher, self).__init__(*args, **kwargs)

        self._messages = []
        self._message_number = 0
        self._queues = []
        self._declared_queues = 0
        self._binded_queues = 0

    def setup_queue(self, queue_name, routing_key):
        """Setup a queue. This method can be called multiple times to setup
        multiple publishing queues"""
        self._queues.append((queue_name, routing_key))

    def mark_message(self, message):
        """Method called when a message has been published"""
        pass

    def add_messages(self):
        """Method called to verify if there is new messages to publish"""
        raise NotImplementedError('add_messages method must be implemented')

    def add_message(self, message):
        """Add a message that will be published"""
        self._messages.append(message)

    def transform_message(self, message):
        """Method called before a message will be published"""
        return message

    def get_routing_key(self, message):
        if len(self._queues) == 1:
            return self._queues[0][1]
        raise NotImplementedError('If multiple queues are defined, the '
                                  'get_routing_key method must be overrided')

    def on_exchange_declared(self, response_frame):
        """Called when RabbitMQ has finished the exchange declare"""
        if len(self._queues) == 0:
            self.setup_queue(self.queue, self.routing_key)
        for queue, routing_key in self._queues:
            self._channel.queue_declare(self.on_queue_declared, queue,
                                        durable=True)

    def on_queue_declared(self, method_frame):
        """Called when a queue has been configured"""
        self._declared_queues += 1
        if self._declared_queues == len(self._queues):
            for queue, routing_key in self._queues:
                self._channel.queue_bind(self.on_bind, queue, self.exchange,
                                         routing_key)

    def _publish(self):
        """Publish a message from the message list"""
        if self._closing is True:
            return
        message = self._messages.pop(0)
        self.publish(message)
        self.schedule_next_message()

    def publish(self, message):
        """Publish a message"""
        routing_key = self.get_routing_key(message)
        body = cPickle.dumps(self.transform_message(message))
        self._message_number += 1
        self._channel.basic_publish(self.exchange, routing_key, body)
        self.mark_message(message)
        self._log('Published message #%d' % self._message_number)

    def _add_messages(self):
        self._messages.extend(self.add_messages())
        self.schedule_next_message()

    def start_publishing(self):
        """Begin the publishing of messages"""
        self._log('Begin the publishing of messages')
        self.schedule_next_message()

    def schedule_next_message(self):
        """Schedule the next message to be delivered"""
        if self._closing is True:
            return
        if len(self._messages) == 0:
            self._connection.add_timeout(self.batch_interval,
                                         self._add_messages)
            return
        self._connection.add_timeout(self.publish_interval,
                                     self._publish)

    def stop(self):
        """Stop the publishing process"""
        self._log('Stopping')
        self._closing = True
        self.close_channel()
        self.close_connection()
        # Allow the publisher to cleanly disconnect from RabbitMQ
        self._connection.ioloop.start()
        self._log('Stopped')

    def on_bind(self, response_frame):
        """Called when the queue is ready to received messages"""
        self._binded_queues += 1
        if self._binded_queues == len(self._queues):
            self.start_publishing()
