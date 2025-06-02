#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# noinspection PyUnresolvedReferences
from docutils import nodes, utils
# noinspection PyUnresolvedReferences
from sphinx.errors import SphinxError

GROUP_ID = 'io.trino'
ARTIFACTS = {
    'server': ('trino-server', 'tar.gz', None),
    'server-core': ('trino-server-core', 'tar.gz', None),
    'cli': ('trino-cli', 'jar', 'executable'),
    'jdbc': ('trino-jdbc', 'jar', None),
    #plugins
    'ai-functions':  ('trino-ai-functions', 'zip', None),
    'bigquery':  ('trino-bigquery', 'zip', None),
    'blackhole':  ('trino-blackhole', 'zip', None),
    'cassandra':  ('trino-cassandra', 'zip', None),
    'clickhouse':  ('trino-clickhouse', 'zip', None),
    'delta-lake':  ('trino-delta-lake', 'zip', None),
    'druid':  ('trino-druid', 'zip', None),
    'duckdb':  ('trino-duckdb', 'zip', None),
    'elasticsearch':  ('trino-elasticsearch', 'zip', None),
    'example-http':  ('trino-example-http', 'zip', None),
    'exasol':  ('trino-exasol', 'zip', None),
    'exchange-filesystem':  ('trino-exchange-filesystem', 'zip', None),
    'exchange-hdfs':  ('trino-exchange-hdfs', 'zip', None),
    'faker':  ('trino-faker', 'zip', None),
    'functions-python':  ('trino-functions-python', 'zip', None),
    'geospatial':  ('trino-geospatial', 'zip', None),
    'google-sheets':  ('trino-google-sheets', 'zip', None),
    'hive':  ('trino-hive', 'zip', None),
    'http-event-listener':  ('trino-http-event-listener', 'zip', None),
    'http-server-event-listener':  ('trino-http-server-event-listener', 'zip', None),
    'hudi':  ('trino-hudi', 'zip', None),
    'iceberg':  ('trino-iceberg', 'zip', None),
    'ignite':  ('trino-ignite', 'zip', None),
    'jmx':  ('trino-jmx', 'zip', None),
    'kafka':  ('trino-kafka', 'zip', None),
    'kafka-event-listener':  ('trino-kafka-event-listener', 'zip', None),
    'loki':  ('trino-loki', 'zip', None),
    'mariadb':  ('trino-mariadb', 'zip', None),
    'memory':  ('trino-memory', 'zip', None),
    'ml':  ('trino-ml', 'zip', None),
    'mongodb':  ('trino-mongodb', 'zip', None),
    'mysql':  ('trino-mysql', 'zip', None),
    'mysql-event-listener':  ('trino-mysql-event-listener', 'zip', None),
    'opa':  ('trino-opa', 'zip', None),
    'openlineage':  ('trino-openlineage', 'zip', None),
    'opensearch':  ('trino-opensearch', 'zip', None),
    'oracle':  ('trino-oracle', 'zip', None),
    'password-authenticators':  ('trino-password-authenticators', 'zip', None),
    'pinot':  ('trino-pinot', 'zip', None),
    'postgresql':  ('trino-postgresql', 'zip', None),
    'prometheus':  ('trino-prometheus', 'zip', None),
    'ranger':  ('trino-ranger', 'zip', None),
    'redis':  ('trino-redis', 'zip', None),
    'redshift':  ('trino-redshift', 'zip', None),
    'resource-group-managers':  ('trino-resource-group-managers', 'zip', None),
    'session-property-managers':  ('trino-session-property-managers', 'zip', None),
    'singlestore':  ('trino-singlestore', 'zip', None),
    'snowflake':  ('trino-snowflake', 'zip', None),
    'spooling-filesystem':  ('trino-spooling-filesystem', 'zip', None),
    'sqlserver':  ('trino-sqlserver', 'zip', None),
    'teradata-functions':  ('trino-teradata-functions', 'zip', None),
    'thrift':  ('trino-thrift', 'zip', None),
    'tpcds':  ('trino-tpcds', 'zip', None),
    'tpch':  ('trino-tpch', 'zip', None),
    'vertica':  ('trino-vertica', 'zip', None),
}

def maven_filename(artifact, version, packaging, classifier):
    classifier = '-' + classifier if classifier else ''
    return '%s-%s%s.%s' % (artifact, version, classifier, packaging)


def maven_download(group, artifact, version, packaging, classifier):
    base = 'https://repo1.maven.org/maven2/'
    group_path = group.replace('.', '/')
    filename = maven_filename(artifact, version, packaging, classifier)
    return base + '/'.join((group_path, artifact, version, filename))


def setup(app):
    # noinspection PyDefaultArgument,PyUnusedLocal
    def download_link_role(role, rawtext, text, lineno, inliner, options={}, content=[]):
        version = app.config.release

        if not text in ARTIFACTS:
            inliner.reporter.error('Unsupported download type: ' + text, line=lineno)
            return [], []

        artifact, packaging, classifier = ARTIFACTS[text]

        title = maven_filename(artifact, version, packaging, classifier)
        uri = maven_download(GROUP_ID, artifact, version, packaging, classifier)

        node = nodes.reference(title, title, internal=False, refuri=uri)

        return [node], []
    app.add_role('maven_download', download_link_role)

    return {
        'parallel_read_safe': True,
    }
