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

import uuid
import os.path as path

from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Execute
from common import PRESTO_RPM_URL, PRESTO_RPM_NAME, create_connectors,\
    delete_connectors
from resource_management.libraries.functions.check_process_status import check_process_status

import os, re  
import sys
import logging
import json

logging.basicConfig(stream=sys.stdout)
_LOGGER = logging.getLogger(__name__)
key_val_template = '{0}={1}\n'

# execute command, and return the output  
def execCmd(cmd):  
    r = os.popen(cmd)  
    text = r.read()  
    r.close()  
    return text

class Coordinator(Script):
    def install(self, env):
        Execute('mkdir -p /etc/presto')
        Execute('mkdir -p /etc/presto/catalog')
        self.configure(env)

    def stop(self, env):
        from params import daemon_control_script
        try:
            Execute('source /etc/profile && {0} stop'.format(daemon_control_script))
        except Exception as e:
            _LOGGER.error("stop error " + str(e.exception_message) + ' ' + str(e.code) + ' ' + str(e.out) + ' ' + str(e.err))

    def start(self, env):
        from params import daemon_control_script
        self.configure(env)
        Execute('source /etc/profile && {0} start'.format(daemon_control_script))

    def status(self, env):
        check_process_status('/data1/var/presto/data/var/run/launcher.pid')
        return

    def add_kv_file(self):
        from params import config, added_properties, config_directory
        for file in added_properties:
            with open(path.join(config_directory, file), 'w') as f:
                for key, value in config['configurations'][file].iteritems():
                    print 'add:',key,'=',value,'to',file
                    f.write(key_val_template.format(key, value))

    def add_other_config_file(self):
        from params import config, config_directory
        for key, value in config['configurations']['all-other-configs.filecontent'].iteritems():
            with open(path.join(config_directory, key), 'w') as f:
                print 'add config file:',key,'with content',value
                f.write(value)

    def configure(self, env):
        from params import config, node_properties, jvm_config, config_properties, \
            config_directory, memory_configs, host_info, connectors_to_add, connectors_to_delete

        print "config", json.dumps(config)

        self.add_other_config_file()
        with open(path.join(config_directory, 'node.properties'), 'w') as f:
            for key, value in node_properties.iteritems():
                f.write(key_val_template.format(key, value))
            f.write(key_val_template.format('node.id', str(uuid.uuid4())))
            f.write(key_val_template.format('node.data-dir', '/data1/var/presto/data'))

        with open(path.join(config_directory, 'jvm.config'), 'w') as f:
            f.write(jvm_config['jvm.config'])

        with open(path.join(config_directory, 'config.properties'), 'w') as f:
            for key, value in config_properties.iteritems():
                _LOGGER.info("config key:" + key + "config value:" + value)
                if key == 'query.queue-config-file' and value.strip() == '':
                    _LOGGER.info("key == 'query.queue-config-file'")
                    continue
                if key in memory_configs:
                    _LOGGER.info("key in memory_configs")
                    value += 'GB'
                f.write(key_val_template.format(key, value))
            f.write(key_val_template.format('coordinator', 'true'))
            f.write(key_val_template.format('discovery-server.enabled', 'true'))

        create_connectors(node_properties, connectors_to_add)
        delete_connectors(node_properties, connectors_to_delete)


if __name__ == '__main__':
    Coordinator().execute()
