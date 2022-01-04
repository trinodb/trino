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
import io.trino.spi.block.Block;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.DoubleType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static io.trino.plugin.prometheus.MetadataUtil.varcharMapType;
import static io.trino.plugin.prometheus.PrometheusClient.TIMESTAMP_COLUMN_TYPE;
import static io.trino.plugin.prometheus.PrometheusRecordCursor.getBlockFromMap;
import static io.trino.plugin.prometheus.PrometheusRecordCursor.getMapFromBlock;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.time.Instant.ofEpochMilli;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestPrometheusRecordSet
{
    private PrometheusHttpServer prometheusHttpServer;
    private String dataUri;

    @Test
    public void testCursorSimple()
    {
        RecordSet recordSet = new PrometheusRecordSet(
                new PrometheusClient(new PrometheusConnectorConfig(), METRIC_CODEC, TESTING_TYPE_MANAGER),
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

        assertThat(actual).as("actual")
                .hasSize(expected.size());
        for (int i = 0; i < actual.size(); i++) {
            PrometheusStandardizedRow actualRow = actual.get(i);
            PrometheusStandardizedRow expectedRow = expected.get(i);
            assertEquals(getMapFromBlock(varcharMapType, actualRow.getLabels()), getMapFromBlock(varcharMapType, expectedRow.getLabels()));
            assertEquals(actualRow.getTimestamp(), expectedRow.getTimestamp());
            assertEquals(actualRow.getValue(), expectedRow.getValue());
        }
    }

    @BeforeClass
    public void setUp()
    {
        prometheusHttpServer = new PrometheusHttpServer();
        dataUri = prometheusHttpServer.resolve("/prometheus-data/up_matrix_response.json").toString();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (prometheusHttpServer != null) {
            prometheusHttpServer.stop();
        }
    }
}
