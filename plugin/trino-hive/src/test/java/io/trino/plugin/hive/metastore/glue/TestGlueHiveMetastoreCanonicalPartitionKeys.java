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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableList;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.Table;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DateType;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.metastore.HiveType.HIVE_DATE;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.metastore.glue.PartitionFilterBuilder.DECIMAL_TYPE_PRECISION;
import static io.trino.plugin.hive.metastore.glue.PartitionFilterBuilder.DECIMAL_TYPE_SCALE;
import static io.trino.plugin.hive.metastore.glue.TestingGlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.file.Files.createTempDirectory;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestGlueHiveMetastoreCanonicalPartitionKeys
{
    private static final String DATABASE_NAME = "test_database_" + randomNameSuffix();

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private final Path tempDir;
    private final GlueHiveMetastore metastore;

    TestGlueHiveMetastoreCanonicalPartitionKeys()
            throws IOException
    {
        tempDir = createTempDirectory("test");
        metastore = createTestingGlueHiveMetastore(tempDir, closer::register, true);
    }

    @BeforeAll
    void setup()
    {
        metastore.createDatabase(Database.builder()
                .setDatabaseName(DATABASE_NAME)
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty())
                .build());
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        metastore.dropDatabase(DATABASE_NAME, false);

        metastore.shutdown();
        deleteRecursively(tempDir, ALLOW_INSECURE);
        closer.close();
    }

    @Test
    void testDatePartitionPushdown()
    {
        withPartitionedTable(
                new Column("part", HIVE_DATE, Optional.empty(), Map.of()),
                table -> {
                    metastore.addPartitions(table.getDatabaseName(), table.getTableName(), ImmutableList.of(
                            partitionWithValue(table, "2024-01-01"),
                            partitionWithValue(table, "2024-01-02"),
                            partitionWithValue(table, "2024-01-15")));

                    // Filter: 2024-01-01 <= part <= 2024-01-10 — only 2024-01-01 and 2024-01-02 fall within the range
                    TupleDomain<String> rangeFilter = new PartitionFilterBuilder()
                            .addRanges("part", Range.range(
                                    DateType.DATE,
                                    LocalDate.of(2024, 1, 1).toEpochDay(),
                                    true,
                                    LocalDate.of(2024, 1, 10).toEpochDay(),
                                    true))
                            .build();
                    assertThat(metastore.getPartitionNamesByFilter(table.getDatabaseName(), table.getTableName(), ImmutableList.of("part"), rangeFilter))
                            .hasValueSatisfying(names -> assertThat(names).containsExactlyInAnyOrder("part=2024-01-01", "part=2024-01-02"));

                    // Filter: part = 2024-01-02 — only that partition matches
                    TupleDomain<String> equalityFilter = new PartitionFilterBuilder()
                            .addDateValues("part", LocalDate.of(2024, 1, 2).toEpochDay())
                            .build();
                    assertThat(metastore.getPartitionNamesByFilter(table.getDatabaseName(), table.getTableName(), ImmutableList.of("part"), equalityFilter))
                            .hasValueSatisfying(names -> assertThat(names).containsExactly("part=2024-01-02"));

                    // Filter: part = 2025-06-01 — no partition matches
                    TupleDomain<String> noMatchFilter = new PartitionFilterBuilder()
                            .addDateValues("part", LocalDate.of(2025, 6, 1).toEpochDay())
                            .build();
                    assertThat(metastore.getPartitionNamesByFilter(table.getDatabaseName(), table.getTableName(), ImmutableList.of("part"), noMatchFilter))
                            .hasValueSatisfying(names -> assertThat(names).isEmpty());
                });
    }

    @Test
    void testIntPartitionPushdown()
    {
        withPartitionedTable(
                new Column("part", HIVE_INT, Optional.empty(), Map.of()),
                table -> {
                    metastore.addPartitions(table.getDatabaseName(), table.getTableName(), ImmutableList.of(
                            partitionWithValue(table, "1"),
                            partitionWithValue(table, "5"),
                            partitionWithValue(table, "10")));

                    // Filter: 1 <= part <= 7 — only part=1 and part=5 fall within the range
                    TupleDomain<String> rangeFilter = new PartitionFilterBuilder()
                            .addRanges("part", Range.range(IntegerType.INTEGER, 1L, true, 7L, true))
                            .build();
                    assertThat(metastore.getPartitionNamesByFilter(table.getDatabaseName(), table.getTableName(), ImmutableList.of("part"), rangeFilter))
                            .hasValueSatisfying(names -> assertThat(names).containsExactlyInAnyOrder("part=1", "part=5"));

                    // Filter: part = 5 — only part=5 matches
                    TupleDomain<String> equalityFilter = new PartitionFilterBuilder()
                            .addIntegerValues("part", 5L)
                            .build();
                    assertThat(metastore.getPartitionNamesByFilter(table.getDatabaseName(), table.getTableName(), ImmutableList.of("part"), equalityFilter))
                            .hasValueSatisfying(names -> assertThat(names).containsExactly("part=5"));

                    // Filter: part = 99 — no partition matches
                    TupleDomain<String> noMatchFilter = new PartitionFilterBuilder()
                            .addIntegerValues("part", 99L)
                            .build();
                    assertThat(metastore.getPartitionNamesByFilter(table.getDatabaseName(), table.getTableName(), ImmutableList.of("part"), noMatchFilter))
                            .hasValueSatisfying(names -> assertThat(names).isEmpty());
                });
    }

    @Test
    void testDecimalPartitionPushdown()
    {
        withPartitionedTable(
                new Column("part", HiveType.valueOf("decimal(%s,%s)".formatted(DECIMAL_TYPE_PRECISION, DECIMAL_TYPE_SCALE)), Optional.empty(), Map.of()),
                table -> {
                    metastore.addPartitions(table.getDatabaseName(), table.getTableName(), ImmutableList.of(
                            partitionWithValue(table, "10.13400"),
                            partitionWithValue(table, "15.50000"),
                            partitionWithValue(table, "25.00000")));

                    // Filter: part IN (10.134, 15.5) — Glue expression uses 5-decimal-place strings, matching part=10.13400 and part=15.50000
                    TupleDomain<String> equalityFilter = new PartitionFilterBuilder()
                            .addDecimalValues("part", "10.134", "15.5")
                            .build();
                    assertThat(metastore.getPartitionNamesByFilter(table.getDatabaseName(), table.getTableName(), ImmutableList.of("part"), equalityFilter))
                            .hasValueSatisfying(names -> assertThat(names).containsExactlyInAnyOrder("part=10.13400", "part=15.50000"));

                    // Filter: part IN (99.0) — no partition matches
                    TupleDomain<String> noMatchFilter = new PartitionFilterBuilder()
                            .addDecimalValues("part", "99.0")
                            .build();
                    assertThat(metastore.getPartitionNamesByFilter(table.getDatabaseName(), table.getTableName(), ImmutableList.of("part"), noMatchFilter))
                            .hasValueSatisfying(names -> assertThat(names).isEmpty());
                });
    }

    @Test
    void testStringPartitionPushdown()
    {
        withPartitionedTable(
                new Column("part", HIVE_STRING, Optional.empty(), Map.of()),
                table -> {
                    metastore.addPartitions(table.getDatabaseName(), table.getTableName(), ImmutableList.of(
                            partitionWithValue(table, "apple"),
                            partitionWithValue(table, "banana"),
                            partitionWithValue(table, "cherry")));

                    // Filter: part IN ('apple', 'banana') — only those two partitions match
                    TupleDomain<String> equalityFilter = new PartitionFilterBuilder()
                            .addStringValues("part", "apple", "banana")
                            .build();
                    assertThat(metastore.getPartitionNamesByFilter(table.getDatabaseName(), table.getTableName(), ImmutableList.of("part"), equalityFilter))
                            .hasValueSatisfying(names -> assertThat(names).containsExactlyInAnyOrder("part=apple", "part=banana"));

                    // Filter: part IN ('mango') — no partition matches
                    TupleDomain<String> noMatchFilter = new PartitionFilterBuilder()
                            .addStringValues("part", "mango")
                            .build();
                    assertThat(metastore.getPartitionNamesByFilter(table.getDatabaseName(), table.getTableName(), ImmutableList.of("part"), noMatchFilter))
                            .hasValueSatisfying(names -> assertThat(names).isEmpty());
                });
    }

    @Test
    void testComposedPartitionPushdown()
    {
        withPartitionedTable(
                ImmutableList.of(
                        new Column("region", HIVE_STRING, Optional.empty(), Map.of()),
                        new Column("year", HIVE_INT, Optional.empty(), Map.of())),
                table -> {
                    metastore.addPartitions(table.getDatabaseName(), table.getTableName(), ImmutableList.of(
                            partitionWithValues(table, "us", "2023"),
                            partitionWithValues(table, "us", "2024"),
                            partitionWithValues(table, "eu", "2023"),
                            partitionWithValues(table, "eu", "2024")));

                    // Filter: region = 'us' AND year >= 2024 — only region=us/year=2024 matches
                    TupleDomain<String> composedFilter = new PartitionFilterBuilder()
                            .addStringValues("region", "us")
                            .addRanges("year", Range.greaterThanOrEqual(IntegerType.INTEGER, 2024L))
                            .build();
                    assertThat(metastore.getPartitionNamesByFilter(
                            table.getDatabaseName(), table.getTableName(), ImmutableList.of("region", "year"), composedFilter))
                            .hasValueSatisfying(names -> assertThat(names).containsExactly("region=us/year=2024"));

                    // Filter: region = 'us' AND year >= 2025 — no partition matches
                    TupleDomain<String> noMatchFilter = new PartitionFilterBuilder()
                            .addStringValues("region", "us")
                            .addRanges("year", Range.greaterThanOrEqual(IntegerType.INTEGER, 2025L))
                            .build();
                    assertThat(metastore.getPartitionNamesByFilter(
                            table.getDatabaseName(), table.getTableName(), ImmutableList.of("region", "year"), noMatchFilter))
                            .hasValueSatisfying(names -> assertThat(names).isEmpty());
                });
    }

    private void withPartitionedTable(Column partitionColumns, Consumer<Table> test)
    {
        withPartitionedTable(ImmutableList.of(partitionColumns), test);
    }

    private void withPartitionedTable(List<Column> partitionColumns, Consumer<Table> test)
    {
        String tableName = "test_table_" + randomNameSuffix();

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(DATABASE_NAME)
                .setTableName(tableName)
                .setTableType(EXTERNAL_TABLE.name())
                .setDataColumns(ImmutableList.of(new Column("value", HIVE_STRING, Optional.empty(), Map.of())))
                .setPartitionColumns(partitionColumns)
                .setOwner(Optional.empty());
        tableBuilder.getStorageBuilder()
                .setLocation(Optional.of("/tmp/location"))
                .setStorageFormat(PARQUET.toStorageFormat());
        metastore.createTable(tableBuilder.build(), NO_PRIVILEGES);

        try {
            test.accept(metastore.getTable(DATABASE_NAME, tableName).orElseThrow());
        }
        finally {
            metastore.dropTable(DATABASE_NAME, tableName, false);
        }
    }

    private static PartitionWithStatistics partitionWithValue(Table table, String value)
    {
        return partitionWithValues(table, value);
    }

    private static PartitionWithStatistics partitionWithValues(Table table, String... values)
    {
        List<Column> partitionColumns = table.getPartitionColumns();
        String partitionName = IntStream.range(0, values.length)
                .mapToObj(i -> partitionColumns.get(i).getName() + "=" + values[i])
                .collect(joining("/"));
        Partition partition = Partition.builder()
                .setDatabaseName(table.getDatabaseName())
                .setTableName(table.getTableName())
                .setValues(ImmutableList.copyOf(values))
                .setColumns(table.getDataColumns())
                .withStorage(storage -> storage
                        .setLocation(Optional.of("/tmp/location/" + partitionName))
                        .setStorageFormat(PARQUET.toStorageFormat()))
                .build();
        return new PartitionWithStatistics(partition, partitionName, PartitionStatistics.empty());
    }
}
