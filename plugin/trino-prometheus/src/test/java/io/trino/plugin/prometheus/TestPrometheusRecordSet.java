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
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.DoubleType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static io.trino.plugin.prometheus.MetadataUtil.varcharMapType;
import static io.trino.plugin.prometheus.PrometheusClient.TIMESTAMP_COLUMN_TYPE;
import static io.trino.plugin.prometheus.PrometheusRecordCursor.getMapFromSqlMap;
import static io.trino.plugin.prometheus.PrometheusRecordCursor.getSqlMapFromMap;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.time.Instant.ofEpochMilli;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestPrometheusRecordSet
{
    private final PrometheusHttpServer prometheusHttpServer = new PrometheusHttpServer();
    private final String dataUri = prometheusHttpServer.resolve("/prometheus-data/up_matrix_response.json").toString();

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

        assertThat(cursor.getType(0)).isEqualTo(varcharMapType);
        assertThat(cursor.getType(1)).isEqualTo(TIMESTAMP_COLUMN_TYPE);
        assertThat(cursor.getType(2)).isEqualTo(DoubleType.DOUBLE);

        List<PrometheusStandardizedRow> actual = new ArrayList<>();
        while (cursor.advanceNextPosition()) {
            actual.add(new PrometheusStandardizedRow(
                    getMapFromSqlMap(varcharMapType, (SqlMap) cursor.getObject(0)).entrySet().stream()
                            .collect(toImmutableMap(entry -> (String) entry.getKey(), entry -> (String) entry.getValue())),
                    (Instant) cursor.getObject(1),
                    cursor.getDouble(2)));
            assertThat(cursor.isNull(0)).isFalse();
            assertThat(cursor.isNull(1)).isFalse();
            assertThat(cursor.isNull(2)).isFalse();
        }
        List<PrometheusStandardizedRow> expected = ImmutableList.<PrometheusStandardizedRow>builder()
                .add(new PrometheusStandardizedRow(
                        ImmutableMap.of("instance", "localhost:9090", "__name__", "up", "job", "prometheus"), ofEpochMilli(1565962969044L), 1.0))
                .add(new PrometheusStandardizedRow(
                        ImmutableMap.of("instance", "localhost:9090", "__name__", "up", "job", "prometheus"), ofEpochMilli(1565962984045L), 1.0))
                .add(new PrometheusStandardizedRow(
                        ImmutableMap.of("instance", "localhost:9090", "__name__", "up", "job", "prometheus"), ofEpochMilli(1565962999044L), 1.0))
                .add(new PrometheusStandardizedRow(
                        ImmutableMap.of("instance", "localhost:9090", "__name__", "up", "job", "prometheus"), ofEpochMilli(1565963014044L), 1.0))
                .build();

        assertThat(actual).as("actual")
                .hasSize(expected.size());
        for (int i = 0; i < actual.size(); i++) {
            PrometheusStandardizedRow actualRow = actual.get(i);
            PrometheusStandardizedRow expectedRow = expected.get(i);
            assertThat(getMapFromSqlMap(varcharMapType, getSqlMapFromMap(varcharMapType, actualRow.labels()))).isEqualTo(getMapFromSqlMap(varcharMapType, getSqlMapFromMap(varcharMapType, expectedRow.labels())));
            assertThat(actualRow.timestamp()).isEqualTo(expectedRow.timestamp());
            assertThat(actualRow.value()).isEqualTo(expectedRow.value());
        }
    }

    @AfterAll
    public void tearDown()
    {
        prometheusHttpServer.stop();
    }
}
