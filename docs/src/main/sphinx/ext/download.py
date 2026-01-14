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

ARTIFACTS = {
    'server': ('trino-server', 'tar.gz'),
    'server-core': ('trino-server-core', 'tar.gz'),
    'cli': ('trino-cli', None),
    'jdbc': ('trino-jdbc', 'jar'),
    #plugins
    'ai-functions':  ('trino-ai-functions', 'zip'),
    'bigquery':  ('trino-bigquery', 'zip'),
    'blackhole':  ('trino-blackhole', 'zip'),
    'cassandra':  ('trino-cassandra', 'zip'),
    'clickhouse':  ('trino-clickhouse', 'zip'),
    'delta-lake':  ('trino-delta-lake', 'zip'),
    'druid':  ('trino-druid', 'zip'),
    'duckdb':  ('trino-duckdb', 'zip'),
    'elasticsearch':  ('trino-elasticsearch', 'zip'),
    'example-http':  ('trino-example-http', 'zip'),
    'exasol':  ('trino-exasol', 'zip'),
    'exchange-filesystem':  ('trino-exchange-filesystem', 'zip'),
    'exchange-hdfs':  ('trino-exchange-hdfs', 'zip'),
    'faker':  ('trino-faker', 'zip'),
    'functions-python':  ('trino-functions-python', 'zip'),
    'geospatial':  ('trino-geospatial', 'zip'),
    'google-sheets':  ('trino-google-sheets', 'zip'),
    'hive':  ('trino-hive', 'zip'),
    'http-event-listener':  ('trino-http-event-listener', 'zip'),
    'http-server-event-listener':  ('trino-http-server-event-listener', 'zip'),
    'hudi':  ('trino-hudi', 'zip'),
    'iceberg':  ('trino-iceberg', 'zip'),
    'ignite':  ('trino-ignite', 'zip'),
    'jmx':  ('trino-jmx', 'zip'),
    'kafka':  ('trino-kafka', 'zip'),
    'kafka-event-listener':  ('trino-kafka-event-listener', 'zip'),
    'lakehouse':  ('trino-lakehouse', 'zip'),
    'loki':  ('trino-loki', 'zip'),
    'mariadb':  ('trino-mariadb', 'zip'),
    'memory':  ('trino-memory', 'zip'),
    'ml':  ('trino-ml', 'zip'),
    'mongodb':  ('trino-mongodb', 'zip'),
    'mysql':  ('trino-mysql', 'zip'),
    'mysql-event-listener':  ('trino-mysql-event-listener', 'zip'),
    'opa':  ('trino-opa', 'zip'),
    'openlineage':  ('trino-openlineage', 'zip'),
    'opensearch':  ('trino-opensearch', 'zip'),
    'oracle':  ('trino-oracle', 'zip'),
    'password-authenticators':  ('trino-password-authenticators', 'zip'),
    'pinot':  ('trino-pinot', 'zip'),
    'postgresql':  ('trino-postgresql', 'zip'),
    'prometheus':  ('trino-prometheus', 'zip'),
    'ranger':  ('trino-ranger', 'zip'),
    'redis':  ('trino-redis', 'zip'),
    'redshift':  ('trino-redshift', 'zip'),
    'resource-group-managers':  ('trino-resource-group-managers', 'zip'),
    'session-property-managers':  ('trino-session-property-managers', 'zip'),
    'singlestore':  ('trino-singlestore', 'zip'),
    'snowflake':  ('trino-snowflake', 'zip'),
    'spooling-filesystem':  ('trino-spooling-filesystem', 'zip'),
    'sqlserver':  ('trino-sqlserver', 'zip'),
    'teradata-functions':  ('trino-teradata-functions', 'zip'),
    'thrift':  ('trino-thrift', 'zip'),
    'tpcds':  ('trino-tpcds', 'zip'),
    'tpch':  ('trino-tpch', 'zip'),
    'vertica':  ('trino-vertica', 'zip'),
    'weaviate':  ('trino-weaviate', 'zip'),
}

def filename(artifact, version, extension):
    extension = '.' + extension if extension else ''
    return artifact + '-' + version + extension

# Download from Maven Central
def download_mc_url(artifact, version, extension):
    base = 'https://repo1.maven.org/maven2/io/trino'
    file = filename(artifact, version, extension)
    return '%s/%s/%s/%s' % (base, artifact, version, file)


# Download from GitHub Releases
def download_gh_url(artifact, version, extension):
    base = 'https://github.com/trinodb/trino/releases/download'
    file = filename(artifact, version, extension)
    return '%s/%s/%s' % (base, version, file)


def setup(app):
    # noinspection PyDefaultArgument,PyUnusedLocal
    def download_gh_link_role(role, rawtext, text, lineno, inliner, options={}, content=[]):
        version = app.config.release

        if not text in ARTIFACTS:
            inliner.reporter.error('Unsupported download type: ' + text, line=lineno)
            return [], []

        artifact, extension = ARTIFACTS[text]

        title = filename(artifact, version, extension)
        uri = download_gh_url(artifact, version, extension)

        node = nodes.reference(title, title, internal=False, refuri=uri)

        return [node], []

    def download_mc_link_role(role, rawtext, text, lineno, inliner, options={}, content=[]):
        version = app.config.release

        if not text in ARTIFACTS:
            inliner.reporter.error('Unsupported download type: ' + text, line=lineno)
            return [], []

        artifact, extension = ARTIFACTS[text]

        title = filename(artifact, version, extension)
        uri = download_mc_url(artifact, version, extension)

        node = nodes.reference(title, title, internal=False, refuri=uri)

        return [node], []

    app.add_role('download_gh', download_gh_link_role)
    app.add_role('download_mc', download_mc_link_role)

    return {
        'parallel_read_safe': True,
    }
