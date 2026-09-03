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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.Session;
import io.trino.metastore.Column;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.Table;
import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.HiveMetadata.AVRO_SCHEMA_LITERAL_KEY;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_PARTITION_NAMES;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_TABLE;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // glueStats is shared mutable state
public class TestCachedHiveGlueMetastore
        extends AbstractTestQueryFramework
{
    private static final int MAX_PREFIXES_COUNT = 5;

    private static final String AVRO_SCHEMA =
            """
            {
              "type": "record",
              "name": "Envelope",
              "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "amount", "type": ["null", "int"], "default": null}
              ]
            }
            """;

    // Deliberately different from the Avro schema above, standing in for columns that have drifted in Glue
    private static final List<Column> STORED_COLUMNS = ImmutableList.of(
            new Column("event_id", HIVE_STRING, Optional.empty(), ImmutableMap.of()),
            new Column("stale_column", HIVE_INT, Optional.empty(), ImmutableMap.of()));

    private static final Column PARTITION_COLUMN = new Column("acquisition_date", HIVE_STRING, Optional.empty(), ImmutableMap.of());
    private static final List<String> PARTITION_VALUES = ImmutableList.of("2021-10-31");
    private static final String PARTITION_NAME = "acquisition_date=2021-10-31";

    private final String testSchema = "test_schema_" + randomNameSuffix();

    private GlueHiveMetastore metastore;
    private GlueMetastoreStats glueStats;
    private GlueClient glueClient;
    private String warehouseLocation;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        FlociS3AndGlue floci = closeAfterClass(new FlociS3AndGlue());
        String bucketName = "test-cached-hive-glue-metastore-" + randomNameSuffix();
        floci.createBucket(bucketName);
        warehouseLocation = "s3://%s/glue".formatted(bucketName);

        DistributedQueryRunner queryRunner = HiveQueryRunner.builder(testSessionBuilder()
                        .setCatalog("hive")
                        .setSchema(testSchema)
                        .build())
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .addHiveProperty("hive.metastore", "glue")
                .addHiveProperty("hive.metastore.glue.default-warehouse-dir", warehouseLocation)
                .addHiveProperty("hive.metastore-cache-ttl", "1d")
                .addHiveProperty("hive.metastore-refresh-interval", "1h")
                .addHiveProperty("hive.security", "allow-all")
                .addHiveProperty("fs.s3.enabled", "true")
                .addHiveProperties(floci.s3AndGlueProperties())
                .setCreateTpchSchemas(false)
                .build();
        queryRunner.execute("CREATE SCHEMA " + testSchema);
        metastore = getConnectorService(queryRunner, GlueHiveMetastore.class);
        glueStats = metastore.getStats();
        glueClient = closeAfterClass(floci.createGlueClient());
        return queryRunner;
    }

    @Test
    public void testSelectUnpartitionedTable()
    {
        try {
            assertUpdate("CREATE TABLE test_select_from (id VARCHAR, age INT)");
            String select = "SELECT * FROM test_select_from";
            // populate cache and verify test scaffolding (sanity check that getting counts works)
            assertInvocations(select, ImmutableMultiset.of(GET_TABLE));
            // cached
            assertInvocations(select, ImmutableMultiset.of());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_from");
        }
    }

    @Test
    public void testSelectPartitionedTable()
    {
        try {
            assertUpdate(
                    """
                    CREATE TABLE test_select_from_partitioned_where WITH (partitioned_by = ARRAY['regionkey']) AS
                    SELECT nationkey, name, regionkey FROM tpch.tiny.nation
                    """,
                    25);
            String select = "SELECT * FROM test_select_from_partitioned_where WHERE regionkey IN (2, 3)";
            // populate cache and verify test scaffolding (sanity check that getting counts works)
            assertInvocations(select,
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .addCopies(GET_PARTITION_NAMES, 5)
                            .build());
            // cached
            assertInvocations(select, ImmutableMultiset.of());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_from_partitioned_where");
        }
    }

    @Test
    public void testFlushTableCache()
            throws Exception
    {
        try {
            assertUpdate(
                    """
                    CREATE TABLE test_flush_table WITH (partitioned_by = ARRAY['regionkey']) AS
                    SELECT nationkey, name, regionkey FROM tpch.tiny.nation
                    """,
                    25);
            String select = "SELECT * FROM test_flush_table WHERE regionkey IN (2, 3)";
            // populate cache
            assertQuerySucceeds(select);
            // cached
            assertInvocations(select, ImmutableMultiset.of());
            // delete partition metadata behind the scenes
            glueClient.deletePartition(request -> request.databaseName(testSchema).tableName("test_flush_table").partitionValues("2"));
            // cached partition metadata still includes the deleted partition
            assertQuery("SELECT count(*) FROM (" + select + ")", "VALUES 10");
            // flush cache
            assertQuerySucceeds("CALL system.flush_metadata_cache()");
            assertInvocations(select, ImmutableMultiset.<GlueMetastoreMethod>builder()
                    .add(GET_TABLE)
                    .addCopies(GET_PARTITION_NAMES, 5)
                    .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_flush_table");
        }
    }

    @Test
    public void testFlushPartitionCache()
            throws Exception
    {
        try {
            assertUpdate(
                    """
                    CREATE TABLE test_flush_partition WITH (partitioned_by = ARRAY['regionkey']) AS
                    SELECT nationkey, name, regionkey FROM tpch.tiny.nation
                    """,
                    25);
            String select = "SELECT * FROM test_flush_partition WHERE regionkey IN (2, 3)";
            // populate cache
            assertQuerySucceeds(select);
            // cached
            assertInvocations(select, ImmutableMultiset.of());
            // delete partition metadata behind the scenes
            glueClient.deletePartition(request -> request.databaseName(testSchema).tableName("test_flush_partition").partitionValues("2"));
            // cached partition metadata still includes the deleted partition
            assertQuery("SELECT count(*) FROM (" + select + ")", "VALUES 10");
            // flush cache
            assertQuerySucceeds("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_flush_partition', partition_columns => ARRAY['regionkey'], partition_values => ARRAY['2'])");
            assertQueryFails(select, "Partition regionkey=2 no longer exists for %s.test_flush_partition".formatted(testSchema));
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_flush_partition");
        }
    }

    @Test
    void testResolvesPartitionColumnsForAvroTableWithSchemaSet()
    {
        String tableName = "test_avro_partitions_" + randomNameSuffix();
        createTableWithDriftedColumns(tableName, AVRO, ImmutableList.of(PARTITION_COLUMN), ImmutableMap.of(AVRO_SCHEMA_LITERAL_KEY, AVRO_SCHEMA));
        try {
            addPartitionWithStoredColumns(tableName, AVRO);

            Table table = metastore.getTable(testSchema, tableName).orElseThrow();
            assertThat(table.getDataColumns()).extracting(Column::getName).containsExactly("event_id", "amount");

            assertThat(metastore.getPartitionsByNames(table, ImmutableList.of(PARTITION_NAME)))
                    .hasEntrySatisfying(PARTITION_NAME, partition -> assertThat(partition.orElseThrow().getColumns())
                            .isEqualTo(table.getDataColumns()));

            // The call above cached the partition with the columns Glue stored, so this one is served from the cache
            // and must still resolve. Resolving inside the cache loader would freeze it against a table snapshot.
            assertThat(metastore.getPartition(table, PARTITION_VALUES).orElseThrow().getColumns())
                    .isEqualTo(table.getDataColumns());
        }
        finally {
            metastore.dropTable(testSchema, tableName, false);
        }
    }

    @Test
    void testLeavesPartitionColumnsUnchangedForNonAvroTable()
    {
        // The Avro schema property is set but the table is not Avro, so nothing may be resolved
        String tableName = "test_non_avro_partitions_" + randomNameSuffix();
        createTableWithDriftedColumns(tableName, PARQUET, ImmutableList.of(PARTITION_COLUMN), ImmutableMap.of(AVRO_SCHEMA_LITERAL_KEY, AVRO_SCHEMA));
        try {
            addPartitionWithStoredColumns(tableName, PARQUET);

            Table table = metastore.getTable(testSchema, tableName).orElseThrow();
            assertThat(table.getDataColumns()).isEqualTo(STORED_COLUMNS);

            assertThat(metastore.getPartitionsByNames(table, ImmutableList.of(PARTITION_NAME)))
                    .hasEntrySatisfying(PARTITION_NAME, partition -> assertThat(partition.orElseThrow().getColumns())
                            .isEqualTo(STORED_COLUMNS));
            assertThat(metastore.getPartition(table, PARTITION_VALUES).orElseThrow().getColumns())
                    .isEqualTo(STORED_COLUMNS);
        }
        finally {
            metastore.dropTable(testSchema, tableName, false);
        }
    }

    /**
     * Creates a table whose stored columns have drifted from its Avro schema, which cannot be expressed in SQL.
     */
    private void createTableWithDriftedColumns(String tableName, HiveStorageFormat storageFormat, List<Column> partitionColumns, Map<String, String> schemaParameters)
    {
        metastore.createTable(
                Table.builder()
                        .setDatabaseName(testSchema)
                        .setTableName(tableName)
                        .setOwner(Optional.empty())
                        .setTableType("EXTERNAL_TABLE")
                        .setDataColumns(STORED_COLUMNS)
                        .setPartitionColumns(partitionColumns)
                        .setParameters(schemaParameters)
                        .withStorage(storage -> storage
                                .setStorageFormat(storageFormat.toStorageFormat())
                                .setLocation("%s/%s".formatted(warehouseLocation, tableName)))
                        .build(),
                NO_PRIVILEGES);
    }

    /**
     * Adds a partition carrying the columns stored for the table, which is what Glue holds for a partition created
     * before the table's Avro schema evolved.
     */
    private void addPartitionWithStoredColumns(String tableName, HiveStorageFormat storageFormat)
    {
        Partition partition = Partition.builder()
                .setDatabaseName(testSchema)
                .setTableName(tableName)
                .setValues(PARTITION_VALUES)
                .setColumns(STORED_COLUMNS)
                .withStorage(storage -> storage
                        .setStorageFormat(storageFormat.toStorageFormat())
                        .setLocation("%s/%s/%s".formatted(warehouseLocation, tableName, PARTITION_NAME)))
                .build();
        metastore.addPartitions(testSchema, tableName, ImmutableList.of(new PartitionWithStatistics(partition, PARTITION_NAME, PartitionStatistics.empty())));
    }

    private void assertInvocations(@Language("SQL") String query, Multiset<GlueMetastoreMethod> expectedGlueInvocations)
    {
        assertInvocations(getSession(), query, expectedGlueInvocations);
    }

    private void assertInvocations(Session session, @Language("SQL") String query, Multiset<GlueMetastoreMethod> expectedGlueInvocations)
    {
        Map<GlueMetastoreMethod, Integer> countsBefore = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMap(Function.identity(), method -> method.getInvocationCount(glueStats)));

        getQueryRunner().execute(session, query);

        Map<GlueMetastoreMethod, Integer> countsAfter = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMap(Function.identity(), method -> method.getInvocationCount(glueStats)));

        Multiset<GlueMetastoreMethod> actualGlueInvocations = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMultiset(Function.identity(), method -> requireNonNull(countsAfter.get(method)) - requireNonNull(countsBefore.get(method))));

        assertMultisetsEqual(actualGlueInvocations, expectedGlueInvocations);
    }
}
