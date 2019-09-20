# -*- coding: utf-8 -*-

import cPickle
import subprocess


class RabbitMQManager(object):
    """Base class to interract with RabbitMQ"""

    def __init__(self):
        self.declared_vhosts = []
        self.declared_exchanges = []
        self.declared_queues = []
        self.declared_users = []

    def cleanup(self):
        for vhost in self.declared_vhosts:
            self.delete_vhost(vhost)
        for exchange in self.declared_exchanges:
            self.delete_exchange(exchange)
        for queue in self.declared_queues:
            self.delete_queue(queue)
        for user in self.declared_users:
            self.delete_user(user)

    def _exec(self, *args):
        c_args = ["rabbitmqadmin"]
        c_args.extend(list(args))
        proc = subprocess.Popen(c_args, stdout=subprocess.PIPE)
        return proc.communicate()[0]

    def publish_message(self, exchange, routing_key, message):
        message = cPickle.dumps(message)
        self._exec(
            "publish",
            "exchange={0}".format(exchange),
            "routing_key={0}".format(routing_key),
            "payload={0}".format(message),
        )

    def declare_vhost(self, name):
        self._exec("declare", "vhost", "name={0}".format(name))
        self.declared_vhosts.append(name)

    def declare_exchange(self, name, type, durable="false"):
        self._exec(
            "declare",
            "exchange",
            "name={0}".format(name),
            "type={0}".format(type),
            "durable={0}".format(durable),
        )
        self.declared_exchanges.append(name)

    def declare_queue(self, name, durable=False):
        self._exec(
            "declare", "queue", "name={0}".format(name), "durable={0}".format(durable)
        )
        self.declared_queues.append(name)

    def declare_bind(self, exchange, queue, routing_key=None):
        params = [
            "declare",
            "binding",
            "source={0}".format(exchange),
            "destination={0}".format(queue),
        ]
        if routing_key is not None:
            params.append("routing_key={0}".format(routing_key))
        self._exec(*params)

    def declare_user(self, name, password):
        self._exec(
            "declare",
            "user",
            "name={0}".format(name),
            "password={0}".format(password),
            "tags=",
        )
        self.declared_users.append(name)

    def declare_permission(self, vhost, user, configure=".*", write=".*", read=".*"):
        self._exec(
            "declare",
            "permission",
            "vhost={0}".format(vhost),
            "user={0}".format(user),
            "configure={0}".format(configure),
            "write={0}".format(write),
            "read={0}".format(read),
        )

    def delete_vhost(self, name):
        if name in self.vhosts:
            self._exec("delete", "vhost", "name={0}".format(name))
        if name in self.declared_vhosts:
            self.declared_vhosts.remove(name)

    def delete_exchange(self, name):
        if name in self.exchanges:
            self._exec("delete", "exchange", "name={0}".format(name))
        if name in self.declared_exchanges:
            self.declared_exchanges.remove(name)

    def delete_queue(self, name):
        if name in self.queues:
            self._exec("delete", "queue", "name={0}".format(name))
        if name in self.declared_queues:
            self.declared_queues.remove(name)

    def delete_user(self, name):
        self._exec("delete", "user", "name={0}".format(name))
        self.declared_users.remove(name)

    @property
    def vhosts(self):
        out = self._exec("list", "vhosts", "name")
        vhosts = []
        for line in self._parse_listing(out):
            vhosts.append(line[1])
        return vhosts

    @property
    def exchanges(self):
        out = self._exec("list", "exchanges", "name")
        exchanges = []
        for line in self._parse_listing(out):
            exchanges.append(line[1])
        return exchanges

    @property
    def queues(self):
        out = self._exec("list", "queues", "name")
        queues = []
        for line in self._parse_listing(out):
            queues.append(line[1])
        return queues

    @staticmethod
    def _parse_listing(result):
        return [[e.strip() for e in l.split("|")] for l in result.splitlines()[3:-1]]

    def messages_number(self, queue):
        """Return the number of messages in a queue"""
        out = self._exec("list", "queues", "name", "messages")
        for line in self._parse_listing(out):
            if line[1] == queue:
                try:
                    return int(line[2])
                except:
                    break
        return 0
