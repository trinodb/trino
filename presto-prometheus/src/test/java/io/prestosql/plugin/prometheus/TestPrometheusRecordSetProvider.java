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
package io.prestosql.plugin.prometheus;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.DoubleType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.prestosql.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static io.prestosql.plugin.prometheus.MetadataUtil.varcharMapType;
import static io.prestosql.plugin.prometheus.PrometheusClient.TIMESTAMP_COLUMN_TYPE;
import static io.prestosql.plugin.prometheus.PrometheusRecordCursor.getMapFromBlock;
import static io.prestosql.plugin.prometheus.TestPrometheusTable.TYPE_MANAGER;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static java.time.Instant.ofEpochMilli;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestPrometheusRecordSetProvider
{
    private PrometheusHttpServer prometheusHttpServer;
    private URI dataUri;
    private PrometheusClient client;

    @BeforeClass
    public void setUp()
    {
        prometheusHttpServer = new PrometheusHttpServer();
        dataUri = prometheusHttpServer.resolve("/prometheus-data/up_matrix_response.json");
        client = new PrometheusClient(new PrometheusConnectorConfig(), METRIC_CODEC, TYPE_MANAGER);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (prometheusHttpServer != null) {
            prometheusHttpServer.stop();
        }
    }

    @Test
    public void testGetRecordSet()
    {
        ConnectorTableHandle tableHandle = new PrometheusTableHandle("schema", "table");
        PrometheusRecordSetProvider recordSetProvider = new PrometheusRecordSetProvider(client);
        RecordSet recordSet = recordSetProvider.getRecordSet(PrometheusTransactionHandle.INSTANCE, SESSION,
                new PrometheusSplit(dataUri), tableHandle, ImmutableList.of(
                        new PrometheusColumnHandle("labels", varcharMapType, 0),
                        new PrometheusColumnHandle("timestamp", TIMESTAMP_COLUMN_TYPE, 1),
                        new PrometheusColumnHandle("value", DoubleType.DOUBLE, 2)));
        assertNotNull(recordSet, "recordSet is null");

        RecordCursor cursor = recordSet.cursor();
        assertNotNull(cursor, "cursor is null");

        Map<Instant, Map<?, ?>> actual = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            actual.put((Instant) cursor.getObject(1), getMapFromBlock(varcharMapType, (Block) cursor.getObject(0)));
        }
        Map<Instant, Map<String, String>> expected = ImmutableMap.<Instant, Map<String, String>>builder()
                .put(ofEpochMilli(1565962969044L), ImmutableMap.of("instance",
                        "localhost:9090", "__name__", "up",
                        "job", "prometheus"))
                .put(ofEpochMilli(1565962984045L), ImmutableMap.of("instance",
                        "localhost:9090", "__name__", "up",
                        "job", "prometheus"))
                .put(ofEpochMilli(1565962999044L), ImmutableMap.of("instance",
                        "localhost:9090", "__name__", "up",
                        "job", "prometheus"))
                .put(ofEpochMilli(1565963014044L), ImmutableMap.of("instance",
                        "localhost:9090", "__name__", "up",
                        "job", "prometheus"))
                .build();
        assertEquals(actual, expected);
    }
}
