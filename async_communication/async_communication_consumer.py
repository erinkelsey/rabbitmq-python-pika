import pika, time
import logging
from pika.frame import *

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

class consume_engine:
    """
    Class to consume asynchronous messages from RabbitMQ server using pika.

    :param username: username to login to RabbitMQ server
    :param password: password for user to login to RabbitMQ server
    :param host: location of RabbitMQ server
    :param port: port to connect to RabbitMQ server on host
    :param vhost: virtual host on RabbitMQ server
    :param queue: queue to consume messages from
    """

    def __init__(self, username, password, host, port, vhost, queue):
        self._username = username
        self._password = password
        self._host = host
        self._port = port
        self._vhost = vhost
        self._channel = None
        self._connection = None
        self._queue = queue
        self._consumer_tag = None

    def on_open(self, connection):
        """
        Method called after connection to RabbitMQ server has been opened. Calls method
        to set up queue to consume messages.

        :param connection: connection passed through from server callback
        """

        print("Reached connection open \n")
        self._channel = self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """
        Method called when the channel is opened. Declares the queue to consume messages from.

        :param channel: channel passed through from server on callback
        """

        print("Reached channel open \n")
        argument_list = {'x-queue-master-locator': 'random'}
        self._channel.queue_declare(self._queue, durable=True, arguments=argument_list, callback=self.on_declare)

    def on_declare(self, channel):
        """
        Method called after channel has been opened, and queue has been declared, to set up
        the consumer with correct queue and callback functions for when a message is received,
        and 

        :param channel: channel passed through from server on callback
        """
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._consumer_tag = self._channel.basic_consume(self._queue, self.on_message)

    def on_consumer_cancelled(self, method_frame):
        """
        Method called when consumer is cancelled. Close channel on cancel.

        :param method_frame: method frame passed through from server on callback
        """
        print(method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, channel, basic_deliver, properties, body):
        """
        Method called when a message is received by consumer. Sends an acknowledgement that
        the message has been received.

        :param channel: channel passed through from server on callback
        :param basic_deliver: message details passed through from server on callback
        :param properties: message properties passed through from server on callback
        :param body: message body passed through from server on callback
        """

        self._channel.basic_ack(basic_deliver.delivery_tag)
        print(basic_deliver)
        print("Delivery tag is: " + str(basic_deliver.delivery_tag))
        print(properties)
        print("Recevied Content: " + str(body))

    def on_close(self, connection, reply_code):
        """
        Method called when the connection to the RabbitMQ server is closed.

        :param connection: connection passed through from server callback
        :param reply_code: code passed through from server on callback containing shutdown code
        """

        print(reply_code)
        print("connection is being closed \n")

    def stop_consuming(self):
        """
        Method to cancel consumer connection to server. Pass in callback methods for handling shutdown,
        once server gives ok.
        """
        print("Keyboard Interupt recevied !!!")
        if self._channel:
            self._channel.basic_cancel(consumer_tag=self._consumer_tag, callback=self.on_cancelok)

    def on_cancelok(self, unused_frame):
        """
        Method called when basic consumer connection is cancelled to RabbitMQ server.
        Closes channel and calls method to close connection.

        :param unused_frame: unused method frame passed through from server on callback
        """
        self._channel.close()
        self.close_connection()

    def close_connection(self):
        """
        Method to close the connection to RabbitMQ server.
        """
        self._connection.close()

    def run(self):
        """
        Set up the asynchronous connection to RabbitMQ server using the credentials used to instantiate this
        consumer engine. Pass in the on_open and on_close methods as callbacks to handle connection open 
        and close.
        """
        logging.basicConfig(level=logging.ERROR, format=LOG_FORMAT)
        credentials = pika.PlainCredentials(self._username, self._password)
        parameters = pika.ConnectionParameters(self._host, self._port, self._vhost, credentials, socket_timeout=300)
        self._connection = pika.SelectConnection(parameters, on_open_callback=self.on_open)
        self._connection.add_on_close_callback(self.on_close)

        try:
            # Loop so we can communicate with RabbitMQ
            self._connection.ioloop.start()
        except KeyboardInterrupt:
            # Gracefully close the connection
            self.stop_consuming()


if __name__ == '__main__':
    engine = consume_engine(username='guest', password='guest', host='localhost', port=5672, vhost='/', queue='sample_test')
    engine.run()
