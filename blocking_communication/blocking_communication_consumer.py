import pika
import time

class consume_engine:
    """
    Class to consume blocking messages to RabbitMQ server using pika.

    :param username: username to login to RabbitMQ server
    :param password: password for user to login to RabbitMQ server
    :param host: location of RabbitMQ server
    :param port: port to connect to RabbitMQ server on host
    :param vhost: virtual host on RabbitMQ server
    :param queue_name: queue name to consume messages from
    """

    def __init__(self, username, password, host, port, vhost, queue_name):
        self._username = username
        self._password = password
        self._host = host
        self._port = port
        self._vhost = vhost
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
        print(' [*] Waiting for messages. To exit press CTRL+C')

    def on_message(self, channel, method, properties, body):
        """
        Called when a message is received. Sends an acknowledgement that the 
        message has been received.

        :param channel: channel passed through from server on callback
        :param method: message details passed through from server on callback
        :param properties: message properties passed through from server on callback
        :param body: message body passed through from server on callback
        """

        print(" [x] working on %r" % body)
        time.sleep(3)
        print(" [x] Done")
        self._channel.basic_ack(delivery_tag = method.delivery_tag)

    def consume_messages(self):
        """
        Consumes messages that are in the queue on the RabbitMQ server
        """

        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(self._queue_name, self.on_message)
        self._channel.start_consuming()

    def run(self):
        """
        Method to run consumer. Makes connection to RabbitMQ server, creates channel,
        sets up queue, consumes messages.
        """

        self.make_connection()
        self.channel()
        self.declare_queue()
        self.consume_messages()

if __name__ == '__main__':
    engine = consume_engine(username='guest', password='guest', host='localhost', port=5672, vhost='/', queue_name='sample_test')
    engine.run()