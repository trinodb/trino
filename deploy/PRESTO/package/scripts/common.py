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

import os
import ast
import ConfigParser

from resource_management.core.resources.system import Execute

script_dir = os.path.dirname(os.path.realpath(__file__))
config = ConfigParser.ConfigParser()
config.readfp(open(os.path.join(script_dir, 'download.ini')))

PRESTO_RPM_URL = config.get('download', 'presto_rpm_url')
PRESTO_RPM_NAME = PRESTO_RPM_URL.split('/')[-1]
PRESTO_CLI_URL = config.get('download', 'presto_cli_url')

def create_connectors(node_properties, connectors_to_add):
    if not connectors_to_add:
        return
    Execute('mkdir -p {0}'.format(node_properties['plugin.config-dir']))
    connectors_dict = ast.literal_eval(connectors_to_add)
    for connector in connectors_dict:
        connector_file = os.path.join(node_properties['plugin.config-dir'], connector + '.properties')
        with open(connector_file, 'w') as f:
            for lineitem in connectors_dict[connector]:
                f.write('{0}\n'.format(lineitem))

def delete_connectors(node_properties, connectors_to_delete):
    if not connectors_to_delete:
        return
    connectors_list = ast.literal_eval(connectors_to_delete)
    for connector in connectors_list:
        connector_file_name = os.path.join(node_properties['plugin.config-dir'], connector + '.properties')
        Execute('rm -f {0}'.format(connector_file_name))
