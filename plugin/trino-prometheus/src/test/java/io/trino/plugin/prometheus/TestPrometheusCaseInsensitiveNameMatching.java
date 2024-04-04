/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.prometheus;

import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestPrometheusCaseInsensitiveNameMatching
{
    private static final String DEFAULT_SCHEMA = "default";
    private static final String UPPER_CASE_METRIC = "UpperCase-Metric";

    private final PrometheusHttpServer prometheusHttpServer = new PrometheusHttpServer();

    @AfterAll
    public void tearDown()
    {
        prometheusHttpServer.stop();
    }

    @Test
    public void testCaseInsensitiveNameMatchingFalse()
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(prometheusHttpServer.resolve("/prometheus-data/uppercase-metrics.json"));

        PrometheusClient client = new PrometheusClient(config, METRIC_CODEC, TESTING_TYPE_MANAGER);

        Set<String> tableNames = client.getTableNames(DEFAULT_SCHEMA);
        assertThat(tableNames).hasSize(1);
        assertThat(tableNames).contains(UPPER_CASE_METRIC);

        PrometheusMetadata metadata = new PrometheusMetadata(client);
        List<SchemaTableName> tables = metadata.listTables(null, Optional.of(DEFAULT_SCHEMA));
        assertThat(tableNames).hasSize(1);
        assertThat(UPPER_CASE_METRIC.toLowerCase(ENGLISH)).isEqualTo(tables.get(0).getTableName());

        assertThat(client.getTable(DEFAULT_SCHEMA, tables.get(0).getTableName())).isNull();
    }

    @Test
    public void testCaseInsensitiveNameMatchingTrue()
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(prometheusHttpServer.resolve("/prometheus-data/uppercase-metrics.json"));
        config.setCaseInsensitiveNameMatching(true);

        PrometheusClient client = new PrometheusClient(config, METRIC_CODEC, TESTING_TYPE_MANAGER);

        Set<String> tableNames = client.getTableNames(DEFAULT_SCHEMA);
        assertThat(tableNames).hasSize(1);
        assertThat(tableNames).contains(UPPER_CASE_METRIC);

        PrometheusMetadata metadata = new PrometheusMetadata(client);
        List<SchemaTableName> tables = metadata.listTables(null, Optional.of(DEFAULT_SCHEMA));
        assertThat(tableNames).hasSize(1);
        SchemaTableName table = tables.get(0);
        assertThat(UPPER_CASE_METRIC.toLowerCase(ENGLISH)).isEqualTo(table.getTableName());

        assertThat(client.getTable(DEFAULT_SCHEMA, tables.get(0).getTableName())).isNotNull();
        assertThat(UPPER_CASE_METRIC).isEqualTo(client.getTable(DEFAULT_SCHEMA, tables.get(0).getTableName()).getName());
    }
}
