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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.DoubleType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.trino.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static io.trino.plugin.prometheus.MetadataUtil.varcharMapType;
import static io.trino.plugin.prometheus.PrometheusClient.TIMESTAMP_COLUMN_TYPE;
import static io.trino.plugin.prometheus.PrometheusRecordCursor.getMapFromSqlMap;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.time.Instant.ofEpochMilli;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestPrometheusRecordSetProvider
{
    private final PrometheusHttpServer prometheusHttpServer = new PrometheusHttpServer();
    private final String dataUri = prometheusHttpServer.resolve("/prometheus-data/up_matrix_response.json").toString();
    private final PrometheusClient client = new PrometheusClient(new PrometheusConnectorConfig(), METRIC_CODEC, TESTING_TYPE_MANAGER);

    @AfterAll
    public void tearDown()
    {
        prometheusHttpServer.stop();
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
            actual.put((Instant) cursor.getObject(1), getMapFromSqlMap(varcharMapType, (SqlMap) cursor.getObject(0)));
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
                .buildOrThrow();
        assertEquals(actual, expected);
    }
}
