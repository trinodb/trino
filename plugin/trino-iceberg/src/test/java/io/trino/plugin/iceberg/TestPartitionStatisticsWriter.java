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

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.spi.NodeVersion;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionStatistics;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.loadTable;
import static io.trino.plugin.iceberg.TestPartitionStatisticsComputation.fieldCount;
import static io.trino.plugin.iceberg.TestPartitionStatisticsComputation.flatten;
import static io.trino.plugin.iceberg.TestPartitionStatisticsComputation.trinoRows;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestPartitionStatisticsWriter
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;
    private final PartitionStatisticsWriter writer = new PartitionStatisticsWriter(TESTING_TYPE_MANAGER, new NodeVersion("testversion"), newDirectExecutorService());

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build();
    }

    @BeforeAll
    public void setUp()
    {
        metastore = getHiveMetastore(getQueryRunner());
        fileSystemFactory = getFileSystemFactory(getQueryRunner());
    }

    @ParameterizedTest
    @CsvSource({"AVRO, avro", "PARQUET, parquet"})
    public void testWriteReadByLibrary(String format, String extension)
            throws Exception
    {
        String tableName = createTable(format, "id integer, part integer", "part");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1), (2, 2), (3, 1)", 3);
        assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);

        BaseTable table = load(tableName);
        long snapshotId = table.currentSnapshot().snapshotId();
        PartitionStatisticsFile file = writer.write(table, snapshotId, Optional.empty()).orElseThrow();

        assertThat(file.snapshotId()).isEqualTo(snapshotId);
        assertThat(file.path()).matches(Pattern.quote(table.location()) + "/metadata/partition-stats-" + snapshotId + "-[0-9a-f-]{36}\\." + extension);
        assertThat(file.fileSizeInBytes()).isEqualTo(fileSystem().newInputFile(Location.of(file.path())).length());
        assertReadByLibrary(table, file);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testWrittenFileUsedByReader()
            throws Exception
    {
        for (String format : List.of("AVRO", "PARQUET")) {
            // position deletes are subtracted from the row count for format version 3 only
            String tableName = createTable(format, "id integer, part integer", "part", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1), (2, 2), (3, 1)", 3);
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);

            BaseTable table = load(tableName);
            long snapshotId = table.currentSnapshot().snapshotId();
            PartitionStatisticsFile file = writer.write(table, snapshotId, Optional.empty()).orElseThrow();
            table.updatePartitionStatistics().setPartitionStatistics(file).commit();

            assertThat(new PartitionStatisticsReader().filteredRowCount(table, file, Expressions.alwaysTrue(), PartitionFieldPredicate.fromPredicate(TupleDomain.all()), 3, TESTING_TYPE_MANAGER))
                    .hasValue(2);
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', quoteCharacter = '"', value = {
            "timestamp(6)|TIMESTAMP '1960-01-01 00:00:00.000001'|TIMESTAMP '2020-01-01 00:00:00.123456'",
            "timestamp(9)|TIMESTAMP '1960-01-01 00:00:00.000000001'|TIMESTAMP '2020-01-01 00:00:00.123456789'",
            "timestamp(6) with time zone|TIMESTAMP '1960-01-01 00:00:00.000001 UTC'|TIMESTAMP '2020-01-01 00:00:00.123456 UTC'",
            "decimal(38, 2)|DECIMAL '-12345678901234567890123456789012345.67'|DECIMAL '12345678901234567890123456789012345.67'",
            "uuid|UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'|UUID '22151fd2-7586-11e9-8f9e-2a86e4085a59'",
            "varbinary|X'0001ff'|X'80'",
            "varchar|'abc'|'xyz'",
            "date|DATE '1960-01-01'|DATE '2020-01-01'",
            "time(6)|TIME '00:00:00.000001'|TIME '23:59:59.999999'",
            "boolean|true|false",
            "double|-1.5|2.5",
    })
    public void testParquetValueTypes(String type, String first, String second)
            throws Exception
    {
        String tableName = createTable("PARQUET", "id integer, col " + type, "col", 3);
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, " + first + "), (2, " + second + "), (3, NULL), (4, " + first + ")", 4);

        BaseTable table = load(tableName);
        PartitionStatisticsFile file = writer.write(table, table.currentSnapshot().snapshotId(), Optional.empty()).orElseThrow();

        assertReadByLibrary(table, file);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testNoRowsNoFile()
            throws Exception
    {
        String tableName = "test_ps_writer_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id integer, part integer) WITH (partitioning = ARRAY['part'])");

        // a snapshot without manifests has no partitions
        BaseTable table = load(tableName);
        table.newFastAppend().commit();
        Set<String> before = metadataFiles(table);
        assertThat(table.currentSnapshot().allManifests(table.io())).isEmpty();
        assertThat(writer.write(table, table.currentSnapshot().snapshotId(), Optional.empty())).isEmpty();
        assertThat(metadataFiles(table)).isEqualTo(before);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOrcRejected()
    {
        String tableName = createTable("ORC", "id integer, part integer", "part");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1)", 1);

        BaseTable table = load(tableName);
        assertThatThrownBy(() -> writer.write(table, table.currentSnapshot().snapshotId(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertUpdate("DROP TABLE " + tableName);
    }

    private void assertReadByLibrary(BaseTable table, PartitionStatisticsFile file)
            throws IOException
    {
        long snapshotId = file.snapshotId();
        List<List<Object>> expected = trinoRows(PartitionStatisticsComputation.compute(table, snapshotId, Optional.empty(), TESTING_TYPE_MANAGER, newDirectExecutorService()));
        assertThat(expected).isNotEmpty();
        table.updatePartitionStatistics().setPartitionStatistics(file).commit();
        int fields = fieldCount(table);
        List<List<Object>> actual = new ArrayList<>();
        try (CloseableIterable<PartitionStatistics> scan = table.newPartitionStatisticsScan().useSnapshot(snapshotId).scan()) {
            for (PartitionStatistics statistics : scan) {
                actual.add(flatten(statistics, fields));
            }
        }
        assertThat(actual).isEqualTo(expected);
    }

    private Set<String> metadataFiles(Table table)
            throws IOException
    {
        FileIterator files = fileSystem().listFiles(Location.of(table.location() + "/metadata"));
        Set<String> names = new HashSet<>();
        while (files.hasNext()) {
            FileEntry entry = files.next();
            names.add(entry.location().toString());
        }
        return names;
    }

    private TrinoFileSystem fileSystem()
    {
        return fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
    }

    private BaseTable load(String tableName)
    {
        return loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }

    private String createTable(String format, String columns, String partitionColumn)
    {
        return createTable(format, columns, partitionColumn, 2);
    }

    private String createTable(String format, String columns, String partitionColumn, int formatVersion)
    {
        String table = "test_ps_writer_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (" + columns + ") WITH (format_version = " + formatVersion + ", format = '" + format + "', partitioning = ARRAY['" + partitionColumn + "'])");
        return table;
    }
}
