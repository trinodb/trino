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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HiveType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveMetadata.createPredicate;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveMetadata
{
    private static final HiveColumnHandle TEST_COLUMN_HANDLE = createBaseColumn(
            "test",
            0,
            HiveType.HIVE_STRING,
            VARCHAR,
            HiveColumnHandle.ColumnType.PARTITION_KEY,
            Optional.empty());

    private static final HiveColumnHandle DOUBLE_COLUMN_HANDLE = createBaseColumn(
            "test",
            0,
            HiveType.HIVE_DOUBLE,
            DOUBLE,
            HiveColumnHandle.ColumnType.PARTITION_KEY,
            Optional.empty());

    @Test
    @Timeout(10)
    public void testCreatePredicate()
    {
        ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();

        for (int i = 0; i < 5_000; i++) {
            partitions.add(new HivePartition(
                    new SchemaTableName("test", "test"),
                    Integer.toString(i),
                    ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.of(VARCHAR, utf8Slice(Integer.toString(i))))));
        }

        Domain domain = createPredicate(ImmutableList.of(TEST_COLUMN_HANDLE), partitions.build())
                .getDomains().orElseThrow().get(TEST_COLUMN_HANDLE);
        assertThat(domain).isEqualTo(Domain.create(
                ValueSet.copyOf(VARCHAR,
                        IntStream.range(0, 5_000)
                                .mapToObj(i -> utf8Slice(Integer.toString(i)))
                                .collect(toImmutableList())),
                false));
    }

    @Test
    public void testCreateOnlyNullsPredicate()
    {
        ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();

        for (int i = 0; i < 5; i++) {
            partitions.add(new HivePartition(
                    new SchemaTableName("test", "test"),
                    Integer.toString(i),
                    ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.asNull(VARCHAR))));
        }

        Domain domain = createPredicate(ImmutableList.of(TEST_COLUMN_HANDLE), partitions.build())
                .getDomains().orElseThrow().get(TEST_COLUMN_HANDLE);
        assertThat(domain).isEqualTo(Domain.onlyNull(VARCHAR));
    }

    @Test
    public void testCreatePredicateWithNaN()
    {
        HiveColumnHandle columnHandle = DOUBLE_COLUMN_HANDLE;
        ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();

        partitions.add(new HivePartition(
                new SchemaTableName("test", "test"),
                "p1",
                ImmutableMap.of(columnHandle, NullableValue.of(DOUBLE, Double.NaN))));

        partitions.add(new HivePartition(
                new SchemaTableName("test", "test"),
                "p2",
                ImmutableMap.of(columnHandle, NullableValue.of(DOUBLE, 4.2))));

        Domain domain = createPredicate(ImmutableList.of(columnHandle), partitions.build())
                .getDomains().orElseThrow().get(columnHandle);
        assertThat(domain).isEqualTo(Domain.notNull(DOUBLE));
    }

    @Test
    public void testCreatePredicateWithNaNAndNull()
    {
        HiveColumnHandle columnHandle = DOUBLE_COLUMN_HANDLE;
        ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();

        partitions.add(new HivePartition(
                new SchemaTableName("test", "test"),
                "p1",
                ImmutableMap.of(columnHandle, NullableValue.of(DOUBLE, Double.NaN))));

        partitions.add(new HivePartition(
                new SchemaTableName("test", "test"),
                "p2",
                ImmutableMap.of(columnHandle, NullableValue.of(DOUBLE, 4.2))));

        partitions.add(new HivePartition(
                new SchemaTableName("test", "test"),
                "p3",
                ImmutableMap.of(columnHandle, NullableValue.asNull(DOUBLE))));

        Domain domain = createPredicate(ImmutableList.of(columnHandle), partitions.build())
                .getDomains().orElseThrow().get(columnHandle);
        assertThat(domain).isNull();
    }

    @Test
    public void testCreateMixedPredicate()
    {
        ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();

        for (int i = 0; i < 5; i++) {
            partitions.add(new HivePartition(
                    new SchemaTableName("test", "test"),
                    Integer.toString(i),
                    ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.of(VARCHAR, utf8Slice(Integer.toString(i))))));
        }

        partitions.add(new HivePartition(
                new SchemaTableName("test", "test"),
                "null",
                ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.asNull(VARCHAR))));

        Domain domain = createPredicate(ImmutableList.of(TEST_COLUMN_HANDLE), partitions.build())
                .getDomains().orElseThrow().get(TEST_COLUMN_HANDLE);
        assertThat(domain).isEqualTo(Domain.create(ValueSet.of(VARCHAR, utf8Slice("0"), utf8Slice("1"), utf8Slice("2"), utf8Slice("3"), utf8Slice("4")), true));
    }
}
