#!/usr/bin/env python
import pika, time
import sys

class publish_engine:
    """
    Class to publish asynchronous messages to RabbitMQ server using pika.

    :param username: username to login to RabbitMQ server
    :param password: password for user to login to RabbitMQ server
    :param host: location of RabbitMQ server
    :param port: port to connect to RabbitMQ server on host
    :param vhost: virtual host on RabbitMQ server
    :param queue_name: queue name to publish messages to
    :param number_of_messages: number of messages to publish
    :param message_interval: number of seconds to wait between publishing each message
    """

    def __init__(self, username, password, host, port, vhost, queue_name, number_of_messages, message_interval):
        self._username = username
        self._password = password
        self._host = host
        self._port = port
        self._vhost = vhost
        self._messages = number_of_messages
        self._message_interval = message_interval
        self._queue_name = queue_name
        self._connection = None
        self._channel = None

    def make_connection(self):
        """
        Makes a connection to a RabbitMQ server using the credentials and server info 
        used to instantiate this class.
        """

        credentials = pika.PlainCredentials(self._username, self._password)
        parameters = pika.ConnectionParameters(self._host, self._port, self._vhost, credentials, socket_timeout=300)
        self._connection = pika.BlockingConnection(parameters)
        print("Connected Successfully...")

    def channel(self):
        """
        Opens channel on RabbitMQ server with current connection.
        """

        self._channel = self._connection.channel()
        print("Channel opened...")

    def declare_queue(self):
        """
        Declares the queue to publish messages to.
        """

        self._channel.queue_declare(queue=self._queue_name, durable=True)
        print("Queue declared....")

    def publish_message(self):
        """
        Publishes messages to queue on RabbitMQ server.
        """

        message_count = 0
        while message_count < self._messages:
            message_count += 1
            message_body = "task number %i" %(message_count)
            self._channel.basic_publish(exchange='',
                                  routing_key=self._queue_name,
                                  body=message_body,
                                  properties=pika.BasicProperties(
                                      delivery_mode=2  # make message persistant
                                  ))
            print("Published message %i" %(message_count))
            time.sleep(self._message_interval)

    def close_connection(self):
        """
        Close connection to RabbitMQ server.
        """

        self._connection.close()
        print("Closed connection....")

    def run(self):
        """
        Method to run publisher. Makes connection to RabbitMQ server, creates channel,
        sets up queue, publishes required number of messages, and closes the connection.
        """

        self.make_connection()
        self.channel()
        self.declare_queue()
        self.publish_message()
        self.close_connection()

if __name__ == '__main__':
    engine = publish_engine(username='guest', password='guest', host='localhost', port=5672, vhost='/', queue_name='sample_test', number_of_messages=3, message_interval=1)
    engine.run()