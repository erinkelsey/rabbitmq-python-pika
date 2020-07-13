# rabbitmq-python-pika
Publisher and Consumer implementations for RabbitMQ using Python Pika

## Connection Types
Asynchronous Connection:
  - Does not require an response from server that message has been received before sending another, or moving on to another task.

Blocking Connection:
  - Requires an response from server that message has been received after making every request.

## Exchange Types
Fanout Exchange:
  - Published messages are broadcasted to all binded (subscribed) queues
  - For publishing messages to all consumers
  - Consumers get messages that are published only after they have connected to the RabbitMQ Server

Direct Exchange:
  - sldkfj

Topic Exchange:
  - sldfkj

## Install and Setup RabbitMQ on localhost
Install on Mac OSX:
    
    $ brew install rabbitmq
    
Export RabbitMQ to PATH:

    $ export PATH=$PATH:/user/local/sbin
    
Start RabbitMQ Management GUI:

    $ rabbitmq-plugins enable rabbitmq-management
    
  - URL: http://localhost:15672
  - Default Credentials: 
    - Username: guest
    - Password: guest
  
RabbitMQ Config File: 
    [

        {rabbit,
            [
            {tcp_listeners, [{"127.0.0.1", 5672},
                            {"::1", 5672}]},
            {num_tcp_acceptors, 100},
            ]
        }
        
    ].

  - Location: /usr/local/etc/rabbitmq/rabbitmq.conf


Start RabbitMQ Server:

    $ brew services start rabbitmq
    
Stop RabbitMQ Server:

    $ brew services stop rabbitmq

## Install Pika
Install Dependencies (in same folder as Pipfile):

    $ pipenv install 
    
## Run:
Example:

    $ python blocking_communication_publisher.py
