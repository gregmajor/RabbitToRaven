"""get-rabbit-messages: Gets messages from RabbitMQ and stores them in RavenDB."""

__author__ = "Greg Major"
__copyright__ = "Copyright 2015, Lead Pipe Software"
__credits__ = ["Greg Major"]
__license__ = "GPL"
__version__ = "0.1"
__maintainer__ = "Greg Major"
__email__ = "greg.b.major@gmail.com"
__status__ = "Development"

import sys
import argparse
import json
import requests
import pprint

# Authorization String Examples
#
# guest/guest is 'Basic Z3Vlc3Q6Z3Vlc3Q='
# rabbit/rabbit is 'Basic cmFiYml0OnJhYmJpdA=='

def main(args=None):
    """The program entry point."""

    parser = argparse.ArgumentParser(description='Gets messages from RabbitMQ and stores them in RavenDB.')

    parser.add_argument('rabbit_queue', help='the RabbitMQ queue to get the messages from')
    parser.add_argument('message_count', help='the number of messages to get')

    parser.add_argument('-r', '--rabbit_host_url', default='http://localhost', help='the RabbitMQ host URL')
    parser.add_argument('-p', '--rabbit_port', type=int, default=15672, help='the RabbitMQ port')
    parser.add_argument('-s', '--rabbit_vhost', default='%2F', help='the RabbitMQ vhost name')
    parser.add_argument('-z', '--rabbit_authorization_string', default='Basic Z3Vlc3Q6Z3Vlc3Q=', help='the authorization string for the RabbitMQ request header')
    parser.add_argument('-u', '--raven_host_url', default='http://localhost', help='the RavenDB host URL')
    parser.add_argument('-t', '--raven_port', type=int, default=8080, help='the RavenDB port')
    parser.add_argument('-e', '--raven_entity', default='CopiedMessages', help='the name of the RavenDB entities')
    parser.add_argument('-q', '--requeue', type=bool, default=True, help='whether or not to requeue messages')
    parser.add_argument('-v', '--verbose', action='store_true')

    args = parser.parse_args()

    # Get the RabbitMQ messages...
    messages = get_rabbit_messages(args.message_count, args.requeue, args.rabbit_host_url, args.rabbit_port, args.rabbit_vhost, args.rabbit_queue, args.rabbit_authorization_string)

    # Store the messages in RavenDB...
    stored_message_count = store_messages_in_raven(messages, args.raven_entity, args.raven_host_url, args.raven_port)

    print('Fetched: ' + str(len(messages)))
    print(' Stored: ' + str(stored_message_count))

def build_rabbit_url(rabbit_host_url, rabbit_port, rabbit_vhost, rabbit_queue, verbose=False):
    """Builds the RabbitMQ URL.

    Args:
      rabbit_host_url (str): The RabbitMQ host URL.
      rabbit_port (int): The RabbitMQ host port.
      rabbit_vhost (str): The RabbitMQ vhost.
      rabbit_queue (str): The RabbitMQ queue.
      verbose (bool): Determines if verbose output is displayed.

    Returns:
      str: A fully-constructed URL for RabbitMQ.
    """

    url = rabbit_host_url + ':' + str(rabbit_port) + '/api/queues/' + rabbit_vhost + '/' + rabbit_queue + '/get'

    if verbose:
        print(url)

    return url

def build_raven_url(raven_host_url, raven_port, verbose=False):
    """Builds the RavenDB URL.

    Args:
      raven_host_url (str): The RavenDB host URL.
      raven_port (int): The RavenDB host port.
      verbose (bool): Determines if verbose output is displayed.

    Returns:
      str: A fully-constructed URL for RavenDB.
    """

    url = raven_host_url + ':' + str(raven_port) + '/docs'

    if verbose:
        print(url)

    return url

def get_rabbit_messages(message_count, requeue, rabbit_host_url, rabbit_port, rabbit_vhost, rabbit_queue, rabbit_authorization_string):
    """Gets messages from RabbitMQ.

    Args:
      message_count (int): The number of messages to fetch from the queue.
      requeue (bool): Determines if the messages fetched should be requeued.
      rabbit_host_url (str): The RabbitMQ host URL.
      rabbit_port (int): The RabbitMQ host port.
      rabbit_vhost (str): The name of the RabbitMQ vhost.
      rabbit_queue (str): The name of the queue.
      rabbit_authorization_string (str): The authorization string for the request header.

    Returns:
      list: A list of the requested messages.
    """

    rabbit_url = build_rabbit_url(rabbit_host_url, rabbit_port, rabbit_vhost, rabbit_queue)
    rabbit_request_data = {'count': message_count, 'requeue': requeue, 'encoding': 'auto'}
    rabbit_request_json = json.dumps(rabbit_request_data)
    rabbit_request_headers = {'Content-type': 'application/json', 'Authorization': rabbit_authorization_string}

    rabbit_response = requests.post(rabbit_url, data=rabbit_request_json, headers=rabbit_request_headers)

    if rabbit_response.status_code != 200:
        pprint.pprint(rabbit_response.status_code)
        sys.exit('ERROR (' + str(rabbit_response.status_code) + '): ' + rabbit_response.text)

    return rabbit_response.json()

def store_messages_in_raven(messages, raven_entity, raven_host_url, raven_port, verbose=False):
    """Stores messages in RavenDB.

    Args:
      messages (list): The messages to store in RavenDB.
      raven_entity (str): The name of the RavenDB entity.
      raven_host_url (str): The RavenDB host URL.
      raven_port (int): The RavenDB host port.
      verbose (bool): Determines if verbose output is displayed.

    Returns:
      count: The number of messages stored.
    """

    raven_url = build_raven_url(raven_host_url, raven_port)
    raven_request_headers = {'Raven-Entity-name': raven_entity}

    count = 0

    for message in messages:

        count += 1

        json_message = json.dumps(message)

        raven_response = requests.post(raven_url, data=json_message, headers=raven_request_headers)

        if raven_response.status_code != 201:
            pprint.pprint(raven_response.status_code)
            sys.exit('ERROR (' + str(raven_response.status_code) + '): ' + raven_response.text)

        if verbose:
            pprint.pprint(raven_response.text)

    return count

if __name__ == "__main__":
    main()
