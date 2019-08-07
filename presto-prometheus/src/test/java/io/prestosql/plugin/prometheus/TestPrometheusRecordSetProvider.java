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
import io.prestosql.spi.type.TimestampType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.prestosql.plugin.prometheus.MetadataUtil.varcharMapType;
import static io.prestosql.plugin.prometheus.PrometheusRecordCursor.getMapFromBlock;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestPrometheusRecordSetProvider
{
    private PrometheusHttpServer prometheusHttpServer;
    private URI dataUri;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        prometheusHttpServer = new PrometheusHttpServer();
        dataUri = prometheusHttpServer.resolve("/prometheus-data/up_matrix_response.json");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (prometheusHttpServer != null) {
            prometheusHttpServer.stop();
        }
    }

    @Test
    public void testGetRecordSet()
    {
        ConnectorTableHandle tableHandle = new PrometheusTableHandle("schema", "table");
        PrometheusRecordSetProvider recordSetProvider = new PrometheusRecordSetProvider();
        RecordSet recordSet = recordSetProvider.getRecordSet(PrometheusTransactionHandle.INSTANCE, SESSION,
                new PrometheusSplit(dataUri), tableHandle, ImmutableList.of(
                        new PrometheusColumnHandle("labels", varcharMapType, 0),
                        new PrometheusColumnHandle("timestamp", TimestampType.TIMESTAMP, 1),
                        new PrometheusColumnHandle("value", DoubleType.DOUBLE, 2)));
        assertNotNull(recordSet, "recordSet is null");

        RecordCursor cursor = recordSet.cursor();
        assertNotNull(cursor, "cursor is null");

        Map<Timestamp, Map> actual = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            actual.put((Timestamp) cursor.getObject(1), (Map) getMapFromBlock(varcharMapType, (Block) cursor.getObject(0)));
        }
        Map<Timestamp, Map> expected = ImmutableMap.<Timestamp, Map>builder()
                .put(Timestamp.from(Instant.ofEpochMilli(1565962969044L)), ImmutableMap.of("instance",
                        "localhost:9090", "__name__", "up", "job", "prometheus"))
                .put(Timestamp.from(Instant.ofEpochMilli(1565962984045L)), ImmutableMap.of("instance",
                        "localhost:9090", "__name__", "up", "job", "prometheus"))
                .put(Timestamp.from(Instant.ofEpochMilli(1565962999044L)), ImmutableMap.of("instance",
                        "localhost:9090", "__name__", "up", "job", "prometheus"))
                .put(Timestamp.from(Instant.ofEpochMilli(1565963014044L)), ImmutableMap.of("instance",
                        "localhost:9090", "__name__", "up", "job", "prometheus"))
                .build();
        assertEquals(actual, expected);
    }
}
