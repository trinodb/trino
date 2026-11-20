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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.plugin.iceberg.IcebergUtil.canEnforceColumnConstraintInSpecs;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnMetadatas;
import static io.trino.plugin.iceberg.IcebergUtil.getProjectedColumns;
import static io.trino.plugin.iceberg.IcebergUtil.parseVersion;
import static io.trino.plugin.iceberg.util.Timestamps.timestampFromNanos;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzFromNanos;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;

public class TestIcebergUtil
{
    @Test
    public void testCanEnforceTimestampConstraintWithDayTransform()
    {
        Schema schema = new Schema(NestedField.optional(1, "ts", Types.TimestampType.withZone()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("ts").build();
        Domain negativeYear = timestampYearDomain(1969);
        Domain positiveYear = timestampYearDomain(2015);

        // Iceberg's inclusive projection includes the next partition for negative timestamps.
        assertThat(canEnforceTimestampConstraint(spec, negativeYear)).isFalse();
        assertThat(canEnforceTimestampConstraint(spec, positiveYear)).isTrue();
        assertThat(canEnforceTimestampConstraint(spec, Domain.union(List.of(negativeYear, positiveYear)))).isFalse();
    }

    @Test
    public void testCanEnforceTimestampConstraintWithIdentityAndDayTransforms()
    {
        Schema schema = new Schema(NestedField.optional(1, "ts", Types.TimestampType.withZone()));
        for (PartitionSpec spec : List.of(
                PartitionSpec.builderFor(schema).identity("ts").day("ts").build(),
                PartitionSpec.builderFor(schema).day("ts").identity("ts").build())) {
            assertThat(canEnforceTimestampConstraint(spec, timestampYearDomain(1969))).isTrue();
            assertThat(canEnforceTimestampConstraint(spec, timestampYearDomain(2015))).isTrue();
        }
    }

    @Test
    public void testCanEnforceNullConstraintWithBucketAndDayTransforms()
    {
        Schema schema = new Schema(NestedField.optional(1, "ts", Types.TimestampType.withZone()));
        for (PartitionSpec spec : List.of(
                PartitionSpec.builderFor(schema).bucket("ts", 16).day("ts").build(),
                PartitionSpec.builderFor(schema).day("ts").bucket("ts", 16).build())) {
            assertThat(canEnforceTimestampConstraint(spec, Domain.onlyNull(TIMESTAMP_TZ_MICROS))).isTrue();
            assertThat(canEnforceTimestampConstraint(spec, Domain.notNull(TIMESTAMP_TZ_MICROS))).isTrue();
        }
    }

    @Test
    public void testCanEnforceTimestampConstraintAfterColumnRename()
    {
        Schema schema = new Schema(NestedField.optional(1, "ts", Types.TimestampType.withZone()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("ts").build();
        TableMetadata metadata = TableMetadata.newTableMetadata(
                schema, spec, SortOrder.unsorted(), "memory:///test_table", ImmutableMap.of());
        metadata = metadata.updateSchema(new Schema(NestedField.optional(1, "renamed_ts", Types.TimestampType.withZone())));

        // A time travel query still uses the column name from the snapshot's schema.
        for (String columnName : List.of("ts", "renamed_ts")) {
            assertThat(canEnforceTimestampConstraint(metadata, columnName, timestampYearDomain(1969))).isFalse();
            assertThat(canEnforceTimestampConstraint(metadata, columnName, timestampYearDomain(2015))).isTrue();
        }
    }

    @Test
    public void testCanEnforceTimestampConstraintAfterColumnDrop()
    {
        Schema schema = new Schema(
                NestedField.optional(1, "ts", Types.TimestampType.withZone()),
                NestedField.optional(2, "id", Types.LongType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("ts").build();
        TableMetadata metadata = TableMetadata.newTableMetadata(
                schema, spec, SortOrder.unsorted(), "memory:///test_table", ImmutableMap.of());
        int historicalSpecId = metadata.defaultSpecId();
        metadata = metadata.updatePartitionSpec(PartitionSpec.unpartitioned());
        metadata = metadata.updateSchema(new Schema(NestedField.optional(2, "id", Types.LongType.get())));

        assertThat(canEnforceTimestampConstraint(metadata, "ts", timestampYearDomain(2015), ImmutableSet.of(historicalSpecId))).isFalse();
    }

    @Test
    public void testCanEnforceNanoTimestampConstraintAtRangeBoundaries()
    {
        assertCanEnforceNanoTimestampConstraintAtRangeBoundaries(
                TIMESTAMP_NANOS,
                Types.TimestampNanoType.withoutZone(),
                timestampFromNanos(Long.MIN_VALUE),
                timestampFromNanos(Long.MAX_VALUE));
        assertCanEnforceNanoTimestampConstraintAtRangeBoundaries(
                TIMESTAMP_TZ_NANOS,
                Types.TimestampNanoType.withZone(),
                timestampTzFromNanos(Long.MIN_VALUE),
                timestampTzFromNanos(Long.MAX_VALUE));
    }

    private static void assertCanEnforceNanoTimestampConstraintAtRangeBoundaries(
            Type type,
            org.apache.iceberg.types.Type icebergType,
            Object minimum,
            Object maximum)
    {
        Schema schema = new Schema(NestedField.optional(1, "ts", icebergType));
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("ts").build();
        Object belowMinimum = type.getPreviousValue(minimum).orElseThrow();
        Object aboveMaximum = type.getNextValue(maximum).orElseThrow();
        for (Range range : List.of(
                Range.greaterThanOrEqual(type, belowMinimum),
                Range.lessThanOrEqual(type, aboveMaximum),
                // The bounds fit, but the adjacent values used to check partition alignment do not.
                Range.greaterThanOrEqual(type, minimum),
                Range.lessThanOrEqual(type, maximum))) {
            assertThat(canEnforceTimestampConstraint(spec, Domain.create(ValueSet.ofRanges(range), false))).isFalse();
        }
        assertThat(canEnforceTimestampConstraint(spec, timestampYearDomain(type, 2015))).isTrue();
        assertThat(canEnforceTimestampConstraint(spec, timestampYearDomain(type, 10_000))).isFalse();
    }

    private static boolean canEnforceTimestampConstraint(PartitionSpec spec, Domain domain)
    {
        TableMetadata metadata = TableMetadata.newTableMetadata(
                spec.schema(), spec, SortOrder.unsorted(), "memory:///test_table", ImmutableMap.of(FORMAT_VERSION, "3"));
        return canEnforceTimestampConstraint(metadata, "ts", domain);
    }

    private static boolean canEnforceTimestampConstraint(TableMetadata metadata, String columnName, Domain domain)
    {
        return canEnforceTimestampConstraint(metadata, columnName, domain, ImmutableSet.of(metadata.defaultSpecId()));
    }

    private static boolean canEnforceTimestampConstraint(TableMetadata metadata, String columnName, Domain domain, Set<Integer> partitionSpecIds)
    {
        try (InMemoryFileIO fileIO = new InMemoryFileIO()) {
            Table table = new BaseTable(new StaticTableOperations(metadata, fileIO), "test_table");
            IcebergColumnHandle column = IcebergColumnHandle.optional(primitiveColumnIdentity(1, columnName))
                    .columnType(domain.getType())
                    .build();
            return canEnforceColumnConstraintInSpecs(
                    TESTING_TYPE_MANAGER.getTypeOperators(), table, partitionSpecIds, column, domain);
        }
    }

    private static Domain timestampYearDomain(int year)
    {
        return timestampYearDomain(TIMESTAMP_TZ_MICROS, year);
    }

    private static Domain timestampYearDomain(Type type, int year)
    {
        long startMillis = LocalDate.of(year, 1, 1).toEpochDay() * MILLISECONDS_PER_DAY;
        long endMillis = LocalDate.of(year + 1, 1, 1).toEpochDay() * MILLISECONDS_PER_DAY;
        Object start;
        Object end;
        if (type.equals(TIMESTAMP_NANOS)) {
            start = new LongTimestamp(startMillis * MICROSECONDS_PER_MILLISECOND, 0);
            end = new LongTimestamp(endMillis * MICROSECONDS_PER_MILLISECOND, 0);
        }
        else {
            start = LongTimestampWithTimeZone.fromEpochMillisAndFraction(startMillis, 0, UTC_KEY);
            end = LongTimestampWithTimeZone.fromEpochMillisAndFraction(endMillis, 0, UTC_KEY);
        }
        return Domain.create(ValueSet.ofRanges(Range.range(type, start, true, end, false)), false);
    }

    @Test
    public void testParseVersion()
    {
        assertThat(parseVersion("00000-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json")).isEqualTo(0);
        assertThat(parseVersion("99999-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json")).isEqualTo(99999);
        assertThat(parseVersion("00010-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json")).isEqualTo(10);
        assertThat(parseVersion("00011-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json")).isEqualTo(11);
        assertThat(parseVersion("v0.metadata.json")).isEqualTo(0);
        assertThat(parseVersion("v10.metadata.json")).isEqualTo(10);
        assertThat(parseVersion("v99999.metadata.json")).isEqualTo(99999);
        assertThat(parseVersion("v0.gz.metadata.json")).isEqualTo(0);
        assertThat(parseVersion("v0.metadata.json.gz")).isEqualTo(0);

        assertThatThrownBy(() -> parseVersion("hdfs://hadoop-master:9000/user/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44/metadata/00000-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Not a file name: .*");
        assertThatThrownBy(() -> parseVersion("orders_5_581fad8517934af6be1857a903559d44"))
                .hasMessageMatching("Invalid metadata file name: .*");
        assertThatThrownBy(() -> parseVersion("metadata"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("00010_409702ba_4735_4645_8f14_09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v10_metadata_json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v1..gz.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v1.metadata.json.gz."))
                .hasMessageMatching("Invalid metadata file name:.*");

        assertThatThrownBy(() -> parseVersion("00003_409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("-00010-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v-10.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
    }

    @Test
    public void testColumnMetadataTimeZone()
    {
        Schema schema = new Schema(
                NestedField.required(1, "id", Types.LongType.get()),
                NestedField.required(2, "ts", Types.TimestampType.withZone()),
                NestedField.required(3, "nested", Types.ListType.ofRequired(4, Types.TimestampType.withZone())),
                NestedField.required(5, "local_ts", Types.TimestampType.withoutZone()));

        assertThat(getColumnMetadatas(schema, Optional.empty(), TESTING_TYPE_MANAGER, 2))
                .extracting(ColumnMetadata::getName, ColumnMetadata::getTimeZone)
                .containsExactly(
                        tuple("id", Optional.empty()),
                        tuple("ts", Optional.of(UTC_KEY)),
                        tuple("nested", Optional.of(UTC_KEY)),
                        tuple("local_ts", Optional.empty()),
                        tuple("$path", Optional.empty()),
                        tuple("$file_modified_time", Optional.of(UTC_KEY)));
    }

    @Test
    public void testGetProjectedColumns()
    {
        Schema schema = new Schema(
                NestedField.required(1, "id", Types.LongType.get()),
                NestedField.required(2, "nested", Types.StructType.of(
                        NestedField.required(3, "value", Types.StringType.get()),
                        NestedField.required(4, "list", Types.ListType.ofRequired(5, Types.StringType.get())),
                        NestedField.required(6, "nested", Types.StructType.of(
                                NestedField.required(7, "value", Types.StringType.get()))))));

        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(1)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(tuple(1, "id", 1, ImmutableList.of()));
        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(2)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(tuple(2, "nested", 2, ImmutableList.of()));
        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(3)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(tuple(3, "value", 2, ImmutableList.of(3)));
        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(4)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(tuple(4, "list", 2, ImmutableList.of(4)));
        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(5)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(tuple(5, "element", 2, ImmutableList.of(4, 5)));
        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(6)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(tuple(6, "nested", 2, ImmutableList.of(6)));
        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(7)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(tuple(7, "value", 2, ImmutableList.of(6, 7)));

        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(3, 7)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(
                        tuple(3, "value", 2, ImmutableList.of(3)),
                        tuple(7, "value", 2, ImmutableList.of(6, 7)));

        assertThat(getProjectedColumns(schema, TESTING_TYPE_MANAGER, ImmutableSet.of(1, 4, 5)))
                .extracting(IcebergColumnHandle::getId, IcebergColumnHandle::getName, column -> column.getBaseColumn().getId(), IcebergColumnHandle::getPath)
                .containsExactly(
                        tuple(1, "id", 1, ImmutableList.of()),
                        tuple(4, "list", 2, ImmutableList.of(4)),
                        tuple(5, "element", 2, ImmutableList.of(4, 5)));
    }
}
