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
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.STRUCT;
import static io.trino.plugin.iceberg.IcebergMetadata.extractTupleDomainsFromCommitTasks;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.iceberg.FileContent.DATA;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

class TestFileBasedConflictDetection
{
    private static final HadoopTables HADOOP_TABLES = new HadoopTables(new Configuration(false));
    private static final String COLUMN_1_NAME = "col1";
    private static final ColumnIdentity COLUMN_1_IDENTITY = new ColumnIdentity(1, COLUMN_1_NAME, PRIMITIVE, ImmutableList.of());
    private static final IcebergColumnHandle COLUMN_1_HANDLE = new IcebergColumnHandle(COLUMN_1_IDENTITY, INTEGER, ImmutableList.of(), INTEGER, true, Optional.empty());
    private static final String COLUMN_2_NAME = "part";
    private static final ColumnIdentity COLUMN_2_IDENTITY = new ColumnIdentity(2, COLUMN_2_NAME, PRIMITIVE, ImmutableList.of());
    private static final IcebergColumnHandle COLUMN_2_HANDLE = new IcebergColumnHandle(COLUMN_2_IDENTITY, INTEGER, ImmutableList.of(), INTEGER, true, Optional.empty());
    private static final String CHILD_COLUMN_NAME = "child";
    private static final ColumnIdentity CHILD_COLUMN_IDENTITY = new ColumnIdentity(4, CHILD_COLUMN_NAME, PRIMITIVE, ImmutableList.of());
    private static final String PARENT_COLUMN_NAME = "parent";
    private static final ColumnIdentity PARENT_COLUMN_IDENTITY = new ColumnIdentity(3, PARENT_COLUMN_NAME, STRUCT, ImmutableList.of(CHILD_COLUMN_IDENTITY));
    private static final IcebergColumnHandle CHILD_COLUMN_HANDLE = new IcebergColumnHandle(PARENT_COLUMN_IDENTITY, RowType.rowType(new RowType.Field(Optional.of(CHILD_COLUMN_NAME), INTEGER)), ImmutableList.of(CHILD_COLUMN_IDENTITY.getId()), INTEGER, true, Optional.empty());
    private static final Schema TABLE_SCHEMA = new Schema(
            optional(COLUMN_1_IDENTITY.getId(), COLUMN_1_NAME, Types.IntegerType.get()),
            optional(COLUMN_2_IDENTITY.getId(), COLUMN_2_NAME, Types.IntegerType.get()),
            optional(
                    PARENT_COLUMN_IDENTITY.getId(),
                    PARENT_COLUMN_NAME,
                    Types.StructType.of(optional(CHILD_COLUMN_IDENTITY.getId(), CHILD_COLUMN_NAME, Types.IntegerType.get()))));

    @Test
    void testConflictDetectionOnNonPartitionedTable()
    {
        PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
        Table icebergTable = createIcebergTable(partitionSpec);

        List<CommitTaskData> commitTasks = getCommitTaskDataForUpdate(partitionSpec, Optional.empty());
        TupleDomain<IcebergColumnHandle> icebergColumnHandleTupleDomain = extractTupleDomainsFromCommitTasks(getIcebergTableHandle(partitionSpec), icebergTable, commitTasks, null);
        assertThat(icebergColumnHandleTupleDomain.getDomains().orElseThrow()).isEmpty();

        dropIcebergTable(icebergTable);
    }

    @Test
    void testConflictDetectionOnPartitionedTable()
    {
        PartitionSpec partitionSpec = PartitionSpec.builderFor(TABLE_SCHEMA)
                .identity(COLUMN_2_NAME)
                .build();
        Table icebergTable = createIcebergTable(partitionSpec);

        String partitionDataJson =
                """
                {"partitionValues":[40]}
                """;
        Map<IcebergColumnHandle, Domain> expectedDomains = Map.of(COLUMN_2_HANDLE, Domain.singleValue(INTEGER, 40L));
        List<CommitTaskData> commitTasks = getCommitTaskDataForUpdate(partitionSpec, Optional.of(partitionDataJson));
        TupleDomain<IcebergColumnHandle> icebergColumnHandleTupleDomain = extractTupleDomainsFromCommitTasks(getIcebergTableHandle(partitionSpec), icebergTable, commitTasks, null);
        assertThat(icebergColumnHandleTupleDomain.getDomains().orElseThrow()).isEqualTo(expectedDomains);

        dropIcebergTable(icebergTable);
    }

    @Test
    void testConflictDetectionOnPartitionedTableWithMultiplePartitionValues()
    {
        PartitionSpec partitionSpec = PartitionSpec.builderFor(TABLE_SCHEMA)
                .identity(COLUMN_2_NAME)
                .build();
        Table icebergTable = createIcebergTable(partitionSpec);

        String partitionDataJson1 =
                """
                {"partitionValues":[40]}
                """;
        String partitionDataJson2 =
                """
                {"partitionValues":[50]}
                """;
        Map<IcebergColumnHandle, Domain> expectedDomains = Map.of(COLUMN_2_HANDLE, Domain.multipleValues(INTEGER, ImmutableList.of(40L, 50L)));
        // Create commit tasks for updates in two partitions, with values 40 and 50
        List<CommitTaskData> commitTasks = Stream.concat(getCommitTaskDataForUpdate(partitionSpec, Optional.of(partitionDataJson1)).stream(),
                getCommitTaskDataForUpdate(partitionSpec, Optional.of(partitionDataJson2)).stream()).collect(toImmutableList());
        TupleDomain<IcebergColumnHandle> icebergColumnHandleTupleDomain = extractTupleDomainsFromCommitTasks(getIcebergTableHandle(partitionSpec), icebergTable, commitTasks, null);
        assertThat(icebergColumnHandleTupleDomain.getDomains().orElseThrow()).isEqualTo(expectedDomains);

        dropIcebergTable(icebergTable);
    }

    @Test
    void testConflictDetectionOnNestedPartitionedTable()
    {
        PartitionSpec partitionSpec = PartitionSpec.builderFor(TABLE_SCHEMA)
                .identity(PARENT_COLUMN_NAME + "." + CHILD_COLUMN_NAME)
                .build();
        Table icebergTable = createIcebergTable(partitionSpec);

        String partitionDataJson =
                """
                {"partitionValues":[40]}
                """;
        Map<IcebergColumnHandle, Domain> expectedDomains = Map.of(CHILD_COLUMN_HANDLE, Domain.singleValue(INTEGER, 40L));
        List<CommitTaskData> commitTasks = getCommitTaskDataForUpdate(partitionSpec, Optional.of(partitionDataJson));
        TupleDomain<IcebergColumnHandle> icebergColumnHandleTupleDomain = extractTupleDomainsFromCommitTasks(getIcebergTableHandle(partitionSpec), icebergTable, commitTasks, null);
        assertThat(icebergColumnHandleTupleDomain.getDomains().orElseThrow()).isEqualTo(expectedDomains);

        dropIcebergTable(icebergTable);
    }

    @Test
    void testConflictDetectionOnTableWithTwoPartitions()
    {
        PartitionSpec partitionSpec = PartitionSpec.builderFor(TABLE_SCHEMA)
                .identity(COLUMN_2_NAME)
                .identity(COLUMN_1_NAME)
                .build();
        Table icebergTable = createIcebergTable(partitionSpec);

        String partitionDataJson =
                """
                {"partitionValues":[40, 12]}
                """;
        Map<IcebergColumnHandle, Domain> expectedDomains = Map.of(COLUMN_2_HANDLE, Domain.singleValue(INTEGER, 40L), COLUMN_1_HANDLE, Domain.singleValue(INTEGER, 12L));
        List<CommitTaskData> commitTasks = getCommitTaskDataForUpdate(partitionSpec, Optional.of(partitionDataJson));
        TupleDomain<IcebergColumnHandle> icebergColumnHandleTupleDomain = extractTupleDomainsFromCommitTasks(getIcebergTableHandle(partitionSpec), icebergTable, commitTasks, null);
        assertThat(icebergColumnHandleTupleDomain.getDomains().orElseThrow()).isEqualTo(expectedDomains);

        dropIcebergTable(icebergTable);
    }

    @Test
    void testConflictDetectionOnTableWithTwoPartitionsAndMissingPartitionData()
    {
        PartitionSpec partitionSpec = PartitionSpec.builderFor(TABLE_SCHEMA)
                .identity(COLUMN_2_NAME)
                .identity(COLUMN_1_NAME)
                .build();
        Table icebergTable = createIcebergTable(partitionSpec);

        String partitionDataJson =
                """
                {"partitionValues":[40]}
                """;
        Map<IcebergColumnHandle, Domain> expectedDomains = Map.of(COLUMN_2_HANDLE, Domain.singleValue(INTEGER, 40L), COLUMN_1_HANDLE, Domain.onlyNull(INTEGER));
        List<CommitTaskData> commitTasks = getCommitTaskDataForUpdate(partitionSpec, Optional.of(partitionDataJson));
        TupleDomain<IcebergColumnHandle> icebergColumnHandleTupleDomain = extractTupleDomainsFromCommitTasks(getIcebergTableHandle(partitionSpec), icebergTable, commitTasks, null);
        assertThat(icebergColumnHandleTupleDomain.getDomains().orElseThrow()).isEqualTo(expectedDomains);

        dropIcebergTable(icebergTable);
    }

    @Test
    void testConflictDetectionOnEvolvedTable()
    {
        PartitionSpec previousPartitionSpec = PartitionSpec.builderFor(TABLE_SCHEMA)
                .identity(COLUMN_1_NAME)
                .build();
        PartitionSpec currentPartitionSpec = PartitionSpec.builderFor(TABLE_SCHEMA)
                .identity(COLUMN_2_NAME)
                .build();
        Table icebergTable = createIcebergTable(currentPartitionSpec);

        String partitionDataJson =
                """
                {"partitionValues":[40]}
                """;
        CommitTaskData commitTaskData1 = new CommitTaskData("test_location/data/new.parquet", IcebergFileFormat.PARQUET, 0, new MetricsWrapper(new Metrics()), PartitionSpecParser.toJson(currentPartitionSpec),
                Optional.of(partitionDataJson), DATA, Optional.empty(), Optional.empty());
        // Remove file from version with previous partition specification
        CommitTaskData commitTaskData2 = new CommitTaskData("test_location/data/old.parquet", IcebergFileFormat.PARQUET, 0, new MetricsWrapper(new Metrics()), PartitionSpecParser.toJson(previousPartitionSpec),
                Optional.of(partitionDataJson), POSITION_DELETES, Optional.empty(), Optional.empty());
        TupleDomain<IcebergColumnHandle> icebergColumnHandleTupleDomain = extractTupleDomainsFromCommitTasks(getIcebergTableHandle(currentPartitionSpec), icebergTable, List.of(commitTaskData1, commitTaskData2), null);
        assertThat(icebergColumnHandleTupleDomain.getDomains().orElseThrow()).isEmpty();

        dropIcebergTable(icebergTable);
    }

    private static List<CommitTaskData> getCommitTaskDataForUpdate(PartitionSpec partitionSpec, Optional<String> partitionDataJson)
    {
        // Update operation contains two commit tasks
        CommitTaskData commitTaskData1 = new CommitTaskData(
                "test_location/data/new.parquet",
                IcebergFileFormat.PARQUET,
                0,
                new MetricsWrapper(new Metrics()),
                PartitionSpecParser.toJson(partitionSpec),
                partitionDataJson,
                DATA,
                Optional.empty(),
                Optional.empty());
        CommitTaskData commitTaskData2 = new CommitTaskData(
                "test_location/data/old.parquet",
                IcebergFileFormat.PARQUET,
                0,
                new MetricsWrapper(new Metrics()),
                PartitionSpecParser.toJson(partitionSpec),
                partitionDataJson,
                POSITION_DELETES,
                Optional.empty(),
                Optional.empty());

        return List.of(commitTaskData1, commitTaskData2);
    }

    private static IcebergTableHandle getIcebergTableHandle(PartitionSpec partitionSpec)
    {
        String partitionSpecJson = PartitionSpecParser.toJson(partitionSpec);
        return new IcebergTableHandle(
                CatalogHandle.fromId("iceberg:NORMAL:v12345"),
                "schemaName",
                "tableName",
                TableType.DATA,
                Optional.empty(),
                SchemaParser.toJson(TABLE_SCHEMA),
                Optional.of(partitionSpecJson),
                1,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                "dummy_table_location",
                ImmutableMap.of(),
                Optional.empty(),
                false,
                Optional.empty(),
                ImmutableSet.of(),
                Optional.of(false));
    }

    private static Table createIcebergTable(PartitionSpec partitionSpec)
    {
        return HADOOP_TABLES.create(
                TABLE_SCHEMA,
                partitionSpec,
                SortOrder.unsorted(),
                ImmutableMap.of("write.format.default", "ORC"),
                "table_location" + randomNameSuffix());
    }

    private static void dropIcebergTable(Table icebergTable)
    {
        HADOOP_TABLES.dropTable(icebergTable.location());
    }
}
