import pika
import time

class consume_engine:
    """
    Class to consume messages to RabbitMQ server using pika.
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
    :param exchange_name: exchange name to consume messages from
    """

    def __init__(self, username, password, host, port, vhost, exchange):
        self._username = username
        self._password = password
        self._host = host
        self._port = port
        self._vhost = vhost
        self._exchange_name = exchange
        self._queue_name = None
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

    def open_channel(self):
        """
        Opens channel on RabbitMQ server with current connection.
        """

        self._channel = self._connection.channel()
        print("Channel opened...")

    def declare_exchange(self):
        """
        Declares the exchange to consume messages from, with type of 'fanout'.
        """

        self._channel.exchange_declare(exchange=self._exchange_name,
                         exchange_type='fanout')
        print("Exchange declared....")

    def declare_queue(self):
        """
        Get the name of the queue, which is automatically created by the RabbitMQ server
        """

        result = self._channel.queue_declare('', exclusive=True)
        self._queue_name = result.method.queue
        print("Queue declared....")
        print(' [*] Waiting for messages. To exit press CTRL+C')

    def make_binding(self):
        """
        Bind the automatically created queue to the exchange
        """

        self._channel.queue_bind(exchange=self._exchange_name,
                           queue=self._queue_name)
        print("Made binding between exchange: %s and queue: %s" %(self._exchange_name, self._queue_name))

    def on_message(self, channel, method, properties, body):
        """
        Called when a message is received. Does not need to send an acknowledgement.

        :param channel: channel passed through from server on callback
        :param method: message details passed through from server on callback
        :param properties: message properties passed through from server on callback
        :param body: message body passed through from server on callback
        """

        print(" [x] Feed Received - %s \n" % str(body))
        time.sleep(2)

    def consume_messages(self):
        """
        Consumes all messages that are sent to the Fanout Exchange on the RabbitMQ server
        """

        self._channel.basic_consume(self._queue_name, self.on_message,
                                    auto_ack=True)
        self._channel.start_consuming()

    def run(self):
        """
        Method to run consumer. Makes connection to RabbitMQ server, creates channel,
        sets up Fanout Exchange, binds queue and exchange, consumes messages.
        """

        self.make_connection()
        self.open_channel()
        self.declare_exchange()
        self.declare_queue()
        self.make_binding()
        self.consume_messages()

if __name__ == '__main__':
    engine = consume_engine(username='guest', password='guest', host='localhost', port=5672, vhost='/', exchange='score.feed.fanout_exchange')
    engine.run()