import pika, time
from random import randint

class publish_engine:
    """
    Class to publish messages to RabbitMQ server using pika.
    Messages are published to a Fanout Exchange, so that they are
    received by all consumers subscribed to that exchange. Queues are 
    automatically created when a consumer connects, and automatically 
    destroyed when they close their connection, therefore only need to 
    specify exchange, not queue.

    :param username: username to login to RabbitMQ server
    :param password: password for user to login to RabbitMQ server
    :param host: location of RabbitMQ server
    :param port: port to connect to RabbitMQ server on host
    :param vhost: virtual host on RabbitMQ server
    :param exchange_name: exchange name to publish messages to 
    :param number_of_messages: number of messages to publish
    :param message_interval: number of seconds to wait between publishing each message
    """

    def __init__(self, username, password, host, port, vhost, exchange, number_of_messages, message_interval):
        self._username = username
        self._password = password
        self._host = host
        self._port = port
        self._vhost = vhost
        self._messages = number_of_messages
        self._message_interval = message_interval
        self._exchange_name = exchange
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
        print("Connected successfully...")

    def channel(self):
        """
        Opens channel on RabbitMQ server with current connection.
        """

        self._channel = self._connection.channel()
        print("Channel opened...")

    def declare_exchange(self):
        """
        Declares the exchange to publish messages to, with type of 'fanout'.
        """

        self._channel.exchange_declare(exchange=self._exchange_name,
                         exchange_type='fanout')
        print("Exchange declared....")

    def publish_message(self):
        """
        Publishes messages to Fanout Exchange on RabbitMQ Server.
        """

        message_count = 0
        score = 0
        while message_count < self._messages:
            message_count += 1
            score += randint(0, 9)
            message_body = "Curling Score | Home Team : Canada | Away Team : England | Score : %i " %(score)
            self._channel.basic_publish(exchange=self._exchange_name,
                                  routing_key='',
                                  body=message_body,
                                  properties=pika.BasicProperties(
                                      delivery_mode=2,  # make message persistant
                                  ))
            print("Published message %i with score %i" %(message_count, score))
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
        sets up exchange, publishes required number of messages, and closes the connection.
        """

        self.make_connection()
        self.channel()
        self.declare_exchange()
        self.publish_message()
        self.close_connection()

if __name__ == '__main__':
    engine = publish_engine(username='guest', password='guest', host='localhost', port=5672, vhost='/', exchange='score.feed.fanout_exchange', number_of_messages=25, message_interval=1)
    engine.run()