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
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.DoubleType;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.prometheus.MetadataUtil.TABLE_CODEC;
import static io.trino.plugin.prometheus.MetadataUtil.varcharMapType;
import static io.trino.plugin.prometheus.PrometheusClient.TIMESTAMP_COLUMN_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPrometheusTable
{
    private final PrometheusTable prometheusTable = new PrometheusTable("tableName",
            ImmutableList.of(
                    new PrometheusColumn("labels", varcharMapType),
                    new PrometheusColumn("timestamp", TIMESTAMP_COLUMN_TYPE),
                    new PrometheusColumn("value", DoubleType.DOUBLE)));

    @Test
    public void testColumnMetadata()
    {
        assertThat(prometheusTable.columnsMetadata()).isEqualTo(ImmutableList.of(
                new ColumnMetadata("labels", varcharMapType),
                new ColumnMetadata("timestamp", TIMESTAMP_COLUMN_TYPE),
                new ColumnMetadata("value", DoubleType.DOUBLE)));
    }

    @Test
    public void testRoundTrip()
    {
        String json = TABLE_CODEC.toJson(prometheusTable);
        PrometheusTable prometheusTableCopy = TABLE_CODEC.fromJson(json);

        assertThat(prometheusTableCopy.name()).isEqualTo(prometheusTable.name());
        assertThat(prometheusTableCopy.columns()).isEqualTo(prometheusTable.columns());
    }
}
