'''
Created on Jun 15, 2018

@author: dgrewal
'''
import pika
import requests
import warnings


class RabbitMqSemaphore(object):

    def __init__(self, username, password, ipaddress,
                 queue_name, vhost, queue_length=20,
                 port='5672', http_port='15672'):

        self.username = username
        self.password = password
        self.ipaddress = ipaddress
        self.queue_name = queue_name
        self.vhost = vhost
        self.port = port
        self.http_port = http_port
        self.queue_length = queue_length

        self.initialize_connection()

        self.initialize_queue()

        self.declare_queue()

    def __enter__(self):
        self.delivery_tag = self.get_exclusive_access()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release_exclusive_access(self.delivery_tag)
        self.connection.close()

    def get_queue_length(self):

        url = 'http://{}:{}/api/queues/{}/{}'.format(
            self.ipaddress,
            self.http_port,
            self.vhost,
            self.queue_name)

        info = requests.get(url, auth=(self.username, self.password))

        if not info.status_code == 200:
            return None

        info = info.json()

        ready = info["messages_ready"]
        used = info["messages_unacknowledged"]

        return ready + used

    def declare_queue(self):
        self.channel.queue_declare(queue=self.queue_name)

    def queue_exists(self):
        try:
            self.channel.queue_declare(queue=self.queue_name, passive=True)
            return True
        except Exception as exc:
            if not exc[0] == 404:
                raise

            # pika closes connections on errors
            self.initialize_connection()
            return False

    def initialize_queue(self):

        if not self.queue_exists():
            self.channel.queue_declare(queue=self.queue_name)

            for _ in range(self.queue_length):
                self.channel.basic_publish(exchange='',
                                           routing_key=self.queue_name,
                                           body='Queue Slot')

    def initialize_connection(self):

        credentials = pika.PlainCredentials(self.username, self.password)
        connection_params = pika.ConnectionParameters(
            self.ipaddress,
            self.port,
            self.vhost,
            credentials=credentials)

        self.connection = pika.BlockingConnection(connection_params)

        self.channel = self.connection.channel()

    def get_exclusive_access(self):
        while True:
            get = self.channel.basic_get(self.queue_name)

            if not get[0]:
                self.connection.sleep(30)
                warnings.warn("waiting for slots to download from blob")
                continue

            delivery_tag = get[0].delivery_tag

            return delivery_tag

    def release_exclusive_access(self, delivery_tag):

        self.channel.basic_nack(delivery_tag)
