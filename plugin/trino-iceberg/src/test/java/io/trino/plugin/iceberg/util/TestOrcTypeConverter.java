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

import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.OrcType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static io.trino.orc.metadata.OrcType.OrcTypeKind.TIMESTAMP;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.TIMESTAMP_INSTANT;
import static io.trino.plugin.iceberg.util.OrcTypeConverter.ICEBERG_TIMESTAMP_UNIT;
import static io.trino.plugin.iceberg.util.OrcTypeConverter.ICEBERG_TIMESTAMP_UNIT_MICROS;
import static io.trino.plugin.iceberg.util.OrcTypeConverter.ICEBERG_TIMESTAMP_UNIT_NANOS;
import static io.trino.plugin.iceberg.util.OrcTypeConverter.toOrcType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOrcTypeConverter
{
    @Test
    public void testTimestampUnitsAreAnnotatedForOrc()
    {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "ts_micros", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(2, "ts_micros_tz", Types.TimestampType.withZone()),
                Types.NestedField.optional(3, "ts_nanos", Types.TimestampNanoType.withoutZone()),
                Types.NestedField.optional(4, "ts_nanos_tz", Types.TimestampNanoType.withZone()));

        ColumnMetadata<OrcType> orcTypes = toOrcType(schema);

        OrcType tsMicros = orcTypes.get(new OrcColumnId(1));
        assertThat(tsMicros.getOrcTypeKind()).isEqualTo(TIMESTAMP);
        assertThat(tsMicros.getAttributes()).containsEntry(ICEBERG_TIMESTAMP_UNIT, ICEBERG_TIMESTAMP_UNIT_MICROS);

        OrcType tsMicrosTz = orcTypes.get(new OrcColumnId(2));
        assertThat(tsMicrosTz.getOrcTypeKind()).isEqualTo(TIMESTAMP_INSTANT);
        assertThat(tsMicrosTz.getAttributes()).containsEntry(ICEBERG_TIMESTAMP_UNIT, ICEBERG_TIMESTAMP_UNIT_MICROS);

        OrcType tsNanos = orcTypes.get(new OrcColumnId(3));
        assertThat(tsNanos.getOrcTypeKind()).isEqualTo(TIMESTAMP);
        assertThat(tsNanos.getAttributes()).containsEntry(ICEBERG_TIMESTAMP_UNIT, ICEBERG_TIMESTAMP_UNIT_NANOS);

        OrcType tsNanosTz = orcTypes.get(new OrcColumnId(4));
        assertThat(tsNanosTz.getOrcTypeKind()).isEqualTo(TIMESTAMP_INSTANT);
        assertThat(tsNanosTz.getAttributes()).containsEntry(ICEBERG_TIMESTAMP_UNIT, ICEBERG_TIMESTAMP_UNIT_NANOS);
    }
}
