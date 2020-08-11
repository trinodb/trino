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
import com.google.common.collect.Streams;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.DoubleType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.prestosql.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static io.prestosql.plugin.prometheus.MetadataUtil.varcharMapType;
import static io.prestosql.plugin.prometheus.PrometheusClient.TIMESTAMP_COLUMN_TYPE;
import static io.prestosql.plugin.prometheus.PrometheusRecordCursor.getBlockFromMap;
import static io.prestosql.plugin.prometheus.PrometheusRecordCursor.getMapFromBlock;
import static io.prestosql.plugin.prometheus.TestPrometheusTable.TYPE_MANAGER;
import static java.time.Instant.ofEpochMilli;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestPrometheusRecordSet
{
    private PrometheusHttpServer prometheusHttpServer;
    private URI dataUri;

    @Test
    public void testCursorSimple()
    {
        RecordSet recordSet = new PrometheusRecordSet(
                new PrometheusClient(new PrometheusConnectorConfig(), METRIC_CODEC, TYPE_MANAGER),
                new PrometheusSplit(dataUri),
                ImmutableList.of(
                        new PrometheusColumnHandle("labels", varcharMapType, 0),
                        new PrometheusColumnHandle("timestamp", TIMESTAMP_COLUMN_TYPE, 1),
                        new PrometheusColumnHandle("value", DoubleType.DOUBLE, 2)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), varcharMapType);
        assertEquals(cursor.getType(1), TIMESTAMP_COLUMN_TYPE);
        assertEquals(cursor.getType(2), DoubleType.DOUBLE);

        List<PrometheusStandardizedRow> actual = new ArrayList<>();
        while (cursor.advanceNextPosition()) {
            actual.add(new PrometheusStandardizedRow(
                    (Block) cursor.getObject(0),
                    (Instant) cursor.getObject(1),
                    cursor.getDouble(2)));
            assertFalse(cursor.isNull(0));
            assertFalse(cursor.isNull(1));
            assertFalse(cursor.isNull(2));
        }
        List<PrometheusStandardizedRow> expected = ImmutableList.<PrometheusStandardizedRow>builder()
                .add(new PrometheusStandardizedRow(getBlockFromMap(varcharMapType,
                        ImmutableMap.of("instance", "localhost:9090", "__name__", "up", "job", "prometheus")), ofEpochMilli(1565962969044L), 1.0))
                .add(new PrometheusStandardizedRow(getBlockFromMap(varcharMapType,
                        ImmutableMap.of("instance", "localhost:9090", "__name__", "up", "job", "prometheus")), ofEpochMilli(1565962984045L), 1.0))
                .add(new PrometheusStandardizedRow(getBlockFromMap(varcharMapType,
                        ImmutableMap.of("instance", "localhost:9090", "__name__", "up", "job", "prometheus")), ofEpochMilli(1565962999044L), 1.0))
                .add(new PrometheusStandardizedRow(getBlockFromMap(varcharMapType,
                        ImmutableMap.of("instance", "localhost:9090", "__name__", "up", "job", "prometheus")), ofEpochMilli(1565963014044L), 1.0))
                .build();
        List<PairLike<PrometheusStandardizedRow, PrometheusStandardizedRow>> pairs = Streams.zip(actual.stream(), expected.stream(), PairLike::new)
                .collect(Collectors.toList());
        pairs.forEach(pair -> {
            assertEquals(getMapFromBlock(varcharMapType, pair.getFirst().getLabels()), getMapFromBlock(varcharMapType, pair.getSecond().getLabels()));
            assertEquals(pair.getFirst().getTimestamp(), pair.getSecond().getTimestamp());
            assertEquals(pair.getFirst().getValue(), pair.getSecond().getValue());
        });
    }

    @BeforeClass
    public void setUp()
    {
        prometheusHttpServer = new PrometheusHttpServer();
        dataUri = prometheusHttpServer.resolve("/prometheus-data/up_matrix_response.json");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (prometheusHttpServer != null) {
            prometheusHttpServer.stop();
        }
    }
}
