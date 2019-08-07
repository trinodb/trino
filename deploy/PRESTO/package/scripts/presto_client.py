# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Simple client to communicate with a Presto server.
"""
from httplib import HTTPConnection, HTTPException
import logging
import json
import socket
import sys
import time

from urllib2 import HTTPError, urlopen, URLError


URL_TIMEOUT_MS = 5000
NUM_ROWS = 1000
DATA_RESP = 'data'
NEXT_URI_RESP = 'nextUri'
RETRY_TIMEOUT = 120
SYSTEM_RUNTIME_NODES = 'select * from system.runtime.nodes'
SHOW_CATALOGS = 'show catalogs'
SLEEP_INTERVAL = 10

logging.basicConfig(stream=sys.stdout)
_LOGGER = logging.getLogger(__name__)

def smoketest_presto(client, all_hosts):
    ensure_nodes_are_up(client, all_hosts)
    ensure_catalogs_are_available(client)
    client.execute_query('select * from nation', schema='sf1', catalog='tpch')
    rows = client.get_rows()
    if len(rows) != 25:
        raise RuntimeError('Presto server failed to return the correct \
number of rows from nation table in TPCH connector. Expected 25 but got {0}'.format(len(rows)))

def ensure_catalogs_are_available(client):
    rows = []
    elapsed_time = 0
    while elapsed_time < RETRY_TIMEOUT:
        client.execute_query(SHOW_CATALOGS)
        rows = client.get_rows()
        if not rows:
            time.sleep(SLEEP_INTERVAL)
            _LOGGER.debug('Failed to load catalogs after '
                          'waiting for %d seconds. Retrying...' % elapsed_time)
            elapsed_time += SLEEP_INTERVAL
        else:
            break
    if not rows:
        raise RuntimeError('Presto server failed to load all catalogs within \
{0} seconds.'.format(RETRY_TIMEOUT))


def ensure_nodes_are_up(client, all_hosts):
    result = True
    elapsed_time = 0
    while elapsed_time < RETRY_TIMEOUT:
        result = client.execute_query(SYSTEM_RUNTIME_NODES)
        if not result:
            time.sleep(SLEEP_INTERVAL)
            _LOGGER.debug('Status retrieval for the server failed after '
                          'waiting for %d seconds. Retrying...' % elapsed_time)
            elapsed_time += SLEEP_INTERVAL
        else:
            break
    if not result:
        raise RuntimeError('Presto server failed to start within \
{0} seconds.'.format(RETRY_TIMEOUT))

    # Verify that the nodes we expect to have registered with the Discovery
    # service have actually registered correctly
    elapsed_time = 0
    are_expected_nodes_up = False
    while elapsed_time < RETRY_TIMEOUT:
        client.execute_query(SYSTEM_RUNTIME_NODES)
        nodes_returned_from_presto = []
        for row in client.get_rows():
            nodes_returned_from_presto.append(row[0])
        if len(nodes_returned_from_presto) == len(all_hosts):
            are_expected_nodes_up = True
            break
        else:
            time.sleep(SLEEP_INTERVAL)
            _LOGGER.debug('Elapsed time {0}'.format(elapsed_time))
            _LOGGER.debug(
                'Number of hosts returned from Presto {0} do not match number of hosts specified by user {1}'.format(
                nodes_returned_from_presto, all_hosts))
            elapsed_time += SLEEP_INTERVAL
    if not are_expected_nodes_up:
        raise RuntimeError(
                'Number of hosts returned from Presto {0} do not equal the number of hosts specified by user {1}'.format(
                nodes_returned_from_presto, all_hosts))

# This class was copied more or less verbatim from
# https://github.com/prestodb/presto-admin/blob/master/prestoadmin/prestoclient.py
class PrestoClient:
    response_from_server = {}
    # rows returned by the query
    rows = []
    next_uri = ''

    def __init__(self, server, user, port=None):
        self.server = server
        self.user = user
        self.port = port if port else None

    def clear_old_results(self):
        if self.rows:
            self.rows = []

        if self.next_uri:
            self.next_uri = ''

        if self.response_from_server:
            self.response_from_server = {}

    def execute_query(self, sql, schema='sf1', catalog='tpch'):
        """
        Execute a query connecting to Presto server using passed parameters.

        Client sends http POST request to the Presto server, page:
        '/v1/statement'. Header information should
        include: X-Presto-Catalog, X-Presto-Schema,  X-Presto-User

        Args:
            sql: SQL query to be executed
            schema: Presto schema to be used while executing query
                (default=default)
            catalog: Catalog to be used by the server

        Returns:
            True or False exit status
        """
        if not sql:
            raise InvalidArgumentError('SQL query missing')

        if not self.server:
            raise InvalidArgumentError('Server IP missing')

        if not self.user:
            raise InvalidArgumentError('Username missing')

        if not self.port:
            raise InvalidArgumentError('Port missing')

        self.clear_old_results()

        headers = {'X-Presto-Catalog': catalog,
                   'X-Presto-Schema': schema,
                   'X-Presto-User': self.user}
        answer = ''
        try:
            _LOGGER.info('Connecting to server at: ' + self.server +
                         ':' + str(self.port) + ' as user ' + self.user)
            conn = HTTPConnection(self.server, self.port, False,
                                  URL_TIMEOUT_MS)
            conn.request('POST', '/v1/statement', sql, headers)
            response = conn.getresponse()

            if response.status != 200:
                conn.close()
                _LOGGER.error('Connection error: '
                              + str(response.status) + ' ' + response.reason)
                return False

            answer = response.read()
            conn.close()

            self.response_from_server = json.loads(answer)
            _LOGGER.info('Query executed successfully')
            return True
        except (HTTPException, socket.error):
            _LOGGER.error('Error connecting to presto server at: ' +
                          self.server + ':' + str(self.port))
            return False
        except ValueError as e:
            _LOGGER.error('Error connecting to Presto server: ' + str(e) +
                          ' error from server: ' + answer)
            raise e

    def get_response_from(self, uri):
        """
        Sends a GET request to the Presto server at the specified next_uri
        and updates the response
        """
        try:
            conn = urlopen(uri, None, URL_TIMEOUT_MS)
            answer = conn.read()
            conn.close()

            self.response_from_server = json.loads(answer)
            _LOGGER.info('GET request successful for uri: ' + uri)
            return True
        except (HTTPError, URLError) as e:
            _LOGGER.error('Error opening the presto response uri: ' +
                          str(e.reason))
            return False

    def build_results_from_response(self):
        """
        Build result from the response

        The reponse_from_server may contain up to 3 uri's.
        1. link to fetch the next packet of data ('nextUri')
        2. TODO: information about the query execution ('infoUri')
        3. TODO: cancel the query ('partialCancelUri').
        """
        if NEXT_URI_RESP in self.response_from_server:
            self.next_uri = self.response_from_server[NEXT_URI_RESP]
        else:
            self.next_uri = ''

        if DATA_RESP in self.response_from_server:
            if self.rows:
                self.rows.extend(self.response_from_server[DATA_RESP])
            else:
                self.rows = self.response_from_server[DATA_RESP]

    def get_rows(self, num_of_rows=NUM_ROWS):
        """
        Get the rows returned from the query.

        The client sends GET requests to the server using the 'nextUri'
        from the previous response until the servers response does not
        contain anymore 'nextUri's.  When there is no 'nextUri' the query is
        finished

        Note that this can only be called once and does not page through
        the results.

        Parameters:
            num_of_rows: to be retrieved. 1000 by default
        """
        if num_of_rows == 0:
            return []

        self.build_results_from_response()

        if not self.get_next_uri():
            return []

        while self.get_next_uri():
            if not self.get_response_from(self.get_next_uri()):
                return []
            if len(self.rows) <= num_of_rows:
                self.build_results_from_response()
        return self.rows

    def get_next_uri(self):
        return self.next_uri

class InvalidArgumentError(ValueError):
    pass
