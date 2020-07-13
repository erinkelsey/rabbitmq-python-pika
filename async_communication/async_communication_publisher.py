import pika
import logging

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                '-35s %(lineno) -5d: %(message)s\n')
LOGGER = logging.getLogger(__name__)

class publish_engine:
    """
    Class to publish asynchronous messages to RabbitMQ server using pika.

    :param username: username to login to RabbitMQ server
    :param password: password for user to login to RabbitMQ server
    :param host: location of RabbitMQ server
    :param port: port to connect to RabbitMQ server on host
    :param vhost: virtual host on RabbitMQ server
    :param routing_key: routing_key to direct messages to consumer
    :param number_of_messages: number of messages to publish
    """

    def __init__(self, username, password, host, port, vhost, routing_key, number_of_messages):
        self._username = username
        self._password = password
        self._host = host
        self._port = port
        self._vhost = vhost
        self._routing_key = routing_key
        self._number_of_messages = number_of_messages
        self._channel = None
        self._connection = None

    def on_open(self, connection):
        """
        Method called after connection to RabbitMQ server has been opened. Calls method
        to set up queue to publish messages.

        :param connection: connection passed through from server callback
        """

        print("Reached connection open \n")
        self._channel = self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """
        Method called when the channel is opened. Declares the queue to publish messages to.

        :param channel: channel passed through from server on callback
        """

        print("Reached channel open \n")
        argument_list = {'x-queue-master-locator': 'random'}
        self._channel.queue_declare('sample_test', 
                                      durable=True, 
                                      arguments=argument_list, 
                                      callback=self.on_declare)

    def on_declare(self, method_frame):
        """
        Method to publish the messages to RabbitMQ server. The number of messages to publish
        is contained in the private attribute number_of_messages. Publish using default
        exchange type.

        :param method_frame: method frame passed through from server callback
        """

        while self._number_of_messages > 0:
            print(self._number_of_messages)

            # default exchange -> auto binding
            # delivery_mode=2 -> message is persistent
            self._channel.basic_publish(exchange='',
                                routing_key=self._routing_key,
                                body='H' + str(self._number_of_messages),
                                properties=pika.BasicProperties(content_type='text/plain',
                                                        delivery_mode=2))

            self._number_of_messages -= 1

    def on_close(self, connection, reply_code):
        """
        Method called when the connection to the RabbitMQ server is to be closed, and close connection
        to RabbitMQ server.

        :param connection: connection passed through from server callback
        :param reply_code: code passed through from server on callback containing shutdown code
        """

        print(reply_code)
        self._connection.close()
        print("Connection is closed \n")
       
    def run(self):
        """
        Set up the asynchronous connection to RabbitMQ server using the credentials used to instantiate this
        publisher engine. Pass in the on_open and on_close methods as callbacks to handle connection open 
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
            print("Connection is opened")
        except KeyboardInterrupt:
            # Close connection if user kills process
            print("Closing connection.")


if __name__ == '__main__':
    engine = publish_engine(username='guest', password='guest', host='localhost', port=5672, vhost='/', routing_key='sample_test', number_of_messages=10)
    engine.run()