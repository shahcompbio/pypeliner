'''
Created on Jun 15, 2018

@author: dgrewal
'''
import pika
import time


class RabbitMqSemaphore(object):

    def __init__(self, username, password, ipaddress,
                 queue_name, queue_length=5, port='5672'):
        self.connection = self.initialize_connection(
            username,
            password,
            ipaddress,
            port=port)

        self.queue_name = queue_name

        self.queue_length = queue_length

        self.channel = self.connection.channel()

        self.initialize_queue(self.channel, queue_length)

    def __enter__(self):
        self.delivery_tag = self.get_exclusive_access()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release_exclusive_access(self.delivery_tag)

    def initialize_queue(self):
        self.channel.queue_declare(queue=self.queue_name)

        queue = self.channel.queue_declare(queue=self.queue_name, passive=True)

        queue_length = queue.method.message_count

        if self.queue_length > queue_length:
            for _ in range(self.queue_length - queue_length):
                self.channel.basic_publish(exchange='',
                                           routing_key=self.queue_name,
                                           body='Queue Slot')

    def initialize_connection(
            self, username, password, ipaddress, port='5672'):

        credentials = pika.PlainCredentials(username, password)
        connection_params = pika.ConnectionParameters(
            ipaddress,
            port,
            '/',
            credentials=credentials)

        connection = pika.BlockingConnection(connection_params)

        return connection

    def get_exclusive_access(self):
        while True:
            get = self.channel.basic_get(self.queue_name)

            if not all(get):
                time.sleep(10)
                continue

            delivery_tag = get[0].delivery_tag

            return delivery_tag

    def release_exclusive_access(self, delivery_tag):

        self.channel.basic_nack(delivery_tag)
