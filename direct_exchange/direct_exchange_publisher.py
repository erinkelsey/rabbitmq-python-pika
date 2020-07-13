import pika, time
from random import randint

class publish_engine:
    """
    Class to publish messages to RabbitMQ server using pika.
    Messages are published to a Direct Exchange, so that they are
    received by queues that exactly match the routing key in the message. 

    :param username: username to login to RabbitMQ server
    :param password: password for user to login to RabbitMQ server
    :param host: location of RabbitMQ server
    :param port: port to connect to RabbitMQ server on host
    :param vhost: virtual host on RabbitMQ server
    :param exchange_name: exchange name to publish messages to 
    :param number_of_messages: number of messages to publish
    :param message_interval: number of seconds to wait between publishing each message
    :param routing_key_curling: routing key
    :param routing_key_hockey: routing key
    :param routing_key_football: routing key
    """

    def __init__(self, username, password, host, port, vhost, exchange, number_of_messages, message_interval, routing_key_curling, routing_key_hockey, routing_key_football):
        self._username = username
        self._password = password
        self._host = host
        self._port = port
        self._vhost = vhost
        self._messages = number_of_messages
        self._message_interval = message_interval
        self._exchange_name = exchange
        self._routing_key_curling = routing_key_curling
        self._routing_key_hockey = routing_key_hockey
        self._routing_key_football = routing_key_football
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
        Declares the exchange to publish messages to, with type of 'direct'.
        """

        self._channel.exchange_declare(exchange=self._exchange_name,
                         exchange_type='direct')
        print("Exchange declared....")

    def publish_message(self):
        """
        Publishes messages to Direct Exchange on RabbitMQ Server.
        """

        message_count = 0
        score = 0
        football_score = 0
        hockey_score = 0
        while message_count < self._messages:
            message_count += 1
            score += randint(0, 9)
            football_score += randint(0, 1)
            hockey_score += randint(0, 1)

            message_body = "Curling Score | Home Team : Australia | Away Team : England | Score : %i " %(score)
            self._channel.basic_publish(exchange=self._exchange_name,
                                  routing_key=self._routing_key_curling,
                                  body=message_body,
                                  properties=pika.BasicProperties(
                                      delivery_mode=2,
                                  ))

            message_body = "Football Score | New York Vs New England | New York : %i | New England : 0" %(football_score)
            self._channel.basic_publish(exchange=self._exchange_name,
                                        routing_key=self._routing_key_football,
                                        body=message_body,
                                        properties=pika.BasicProperties(
                                            delivery_mode=2, 
                                        ))

            message_body = "Hockey Score | Canada Vs Russia | Canada : %i | Russia : 0" % (hockey_score)
            self._channel.basic_publish(exchange=self._exchange_name,
                                        routing_key=self._routing_key_hockey,
                                        body=message_body,
                                        properties=pika.BasicProperties(
                                            delivery_mode=2,
                                        ))

            print("Published scorecard for curling, football and hockey - %i " %(message_count))
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
    engine = publish_engine(username='guest', password='guest', host='localhost', port=5672, vhost='/', exchange='score.feed.exchange', number_of_messages=25, message_interval=1, routing_key_curling='scores.curling', routing_key_hockey='scores.hockey', routing_key_football='scores.football')
    engine.run()