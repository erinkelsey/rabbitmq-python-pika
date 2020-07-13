# rabbitmq-python-pika
Publisher and Consumer implementations for RabbitMQ using Python Pika

## Install and Setup RabbitMQ on localhost
Install on Mac OSX:
    
    $ brew install rabbitmq
    
Export RabbitMQ to PATH:

    $ export PATH=$PATH:/user/local/sbin
    
Start RabbitMQ Management GUI:
  - URL: http://localhost:15672
  - Default Credentials: 
    - Username: guest
    - Password: guest
  
    $ rabbitmq-plugins enable rabbitmq-management
    
RabbitMQ Config File Location: /usr/local/etc/rabbitmq/rabbitmq.conf

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
