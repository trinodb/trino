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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestPrometheusCaseInsensitiveNameMatching
{
    private PrometheusHttpServer prometheusHttpServer;
    private static final String DEFAULT_SCHEMA = "default";
    private static final String UPPER_CASE_METRIC = "UpperCase-Metric";

    @BeforeClass
    public void setUp()
    {
        prometheusHttpServer = new PrometheusHttpServer();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (prometheusHttpServer != null) {
            prometheusHttpServer.stop();
            prometheusHttpServer = null;
        }
    }

    @Test
    public void testCaseInsensitiveNameMatchingFalse()
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(prometheusHttpServer.resolve("/prometheus-data/uppercase-metrics.json"));

        PrometheusClient client = new PrometheusClient(config, METRIC_CODEC, TESTING_TYPE_MANAGER);

        Set<String> tableNames = client.getTableNames(DEFAULT_SCHEMA);
        assertThat(tableNames).hasSize(1);
        assertTrue(tableNames.contains(UPPER_CASE_METRIC));

        PrometheusMetadata metadata = new PrometheusMetadata(client);
        List<SchemaTableName> tables = metadata.listTables(null, Optional.of(DEFAULT_SCHEMA));
        assertThat(tableNames).hasSize(1);
        assertEquals(UPPER_CASE_METRIC.toLowerCase(ENGLISH), tables.get(0).getTableName());

        assertNull(client.getTable(DEFAULT_SCHEMA, tables.get(0).getTableName()));
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
        assertTrue(tableNames.contains(UPPER_CASE_METRIC));

        PrometheusMetadata metadata = new PrometheusMetadata(client);
        List<SchemaTableName> tables = metadata.listTables(null, Optional.of(DEFAULT_SCHEMA));
        assertThat(tableNames).hasSize(1);
        SchemaTableName table = tables.get(0);
        assertEquals(UPPER_CASE_METRIC.toLowerCase(ENGLISH), table.getTableName());

        assertNotNull(client.getTable(DEFAULT_SCHEMA, tables.get(0).getTableName()));
        assertEquals(UPPER_CASE_METRIC, client.getTable(DEFAULT_SCHEMA, tables.get(0).getTableName()).getName());
    }
}
