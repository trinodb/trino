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
package io.trino.plugin.iceberg.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.OrcType;
import io.trino.orc.metadata.statistics.ColumnStatistics;
import io.trino.orc.metadata.statistics.TimestampStatistics;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.orc.metadata.OrcType.OrcTypeKind.STRUCT;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.TIMESTAMP;
import static io.trino.plugin.iceberg.util.OrcTypeConverter.ORC_ICEBERG_ID_KEY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOrcMetrics
{
    @Test
    public void testTimestampBoundsAreDroppedWhenScalingOverflows()
    {
        Schema schema = new Schema(Types.NestedField.optional(1, "ts_nano", Types.TimestampNanoType.withoutZone()));

        ColumnMetadata<OrcType> orcColumns = new ColumnMetadata<>(ImmutableList.of(
                new OrcType(STRUCT, ImmutableList.of(new OrcColumnId(1)), ImmutableList.of("ts_nano"), Optional.empty(), Optional.empty(), Optional.empty(), ImmutableMap.of()),
                new OrcType(TIMESTAMP, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), ImmutableMap.of(ORC_ICEBERG_ID_KEY, "1"))));

        ColumnMetadata<ColumnStatistics> columnStatistics = new ColumnMetadata<>(ImmutableList.of(
                new ColumnStatistics(1L, 0, null, null, null, null, null, null, null, null, null, null),
                new ColumnStatistics(1L, 0, null, null, null, null, null, null, new TimestampStatistics(0L, Long.MAX_VALUE), null, null, null)));

        Metrics metrics = OrcMetrics.computeMetrics(MetricsConfig.getDefault(), schema, orcColumns, 1, Optional.of(columnStatistics));

        assertThat(metrics.valueCounts()).containsEntry(1, 1L);
        assertThat(metrics.nullValueCounts()).containsEntry(1, 0L);
        assertThat(metrics.lowerBounds()).isNull();
        assertThat(metrics.upperBounds()).isNull();
    }
}
