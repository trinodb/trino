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
import io.trino.metastore.Column;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.Table;
import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveMetadata.AVRO_SCHEMA_LITERAL_KEY;
import static io.trino.plugin.hive.HiveMetadata.AVRO_SCHEMA_URL_KEY;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the Avro schema resolution {@code GlueHiveMetastore} performs for tables backed by
 * {@code avro.schema.url}/{@code avro.schema.literal}. The metastore cache is enabled because resolution has to
 * hold both for a freshly loaded table and for one served from the cache.
 */
final class TestGlueHiveMetastoreAvroSchemaResolution
        extends AbstractTestQueryFramework
{
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
    private String warehouseLocation;
    private String avroSchemaUrl;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        FlociS3AndGlue floci = closeAfterClass(new FlociS3AndGlue());
        String bucketName = "test-glue-avro-schema-resolution-" + randomNameSuffix();
        floci.createBucket(bucketName);
        warehouseLocation = "s3://%s/glue".formatted(bucketName);

        avroSchemaUrl = "%s/envelope.avsc".formatted(warehouseLocation);
        try (S3Client s3 = floci.createS3Client()) {
            s3.putObject(request -> request.bucket(bucketName).key("glue/envelope.avsc"), RequestBody.fromString(AVRO_SCHEMA));
        }

        QueryRunner queryRunner = HiveQueryRunner.builder(testSessionBuilder()
                        .setCatalog("hive")
                        .setSchema(testSchema)
                        .build())
                .addHiveProperty("hive.metastore", "glue")
                .addHiveProperty("hive.metastore.glue.default-warehouse-dir", warehouseLocation)
                .addHiveProperty("hive.metastore-cache-ttl", "1d")
                .addHiveProperty("hive.security", "allow-all")
                .addHiveProperty("fs.s3.enabled", "true")
                .addHiveProperties(floci.s3AndGlueProperties())
                .setCreateTpchSchemas(false)
                .build();
        queryRunner.execute("CREATE SCHEMA " + testSchema);
        metastore = getConnectorService(queryRunner, GlueHiveMetastore.class);
        return queryRunner;
    }

    @Test
    void testListingDoesNotCacheUnresolvedAvroColumns()
    {
        String tableName = "test_avro_listing_" + randomNameSuffix();
        // The schema is fetched from S3 through the metastore's own file system, the wiring an avro.schema.url table depends on
        createTableWithDriftedColumns(tableName, AVRO, ImmutableList.of(), ImmutableMap.of(AVRO_SCHEMA_URL_KEY, avroSchemaUrl));
        try {
            // Populate the cache through a listing before the table is ever loaded directly
            assertThat(metastore.getTables(testSchema))
                    .extracting(tableInfo -> tableInfo.tableName().getTableName())
                    .contains(tableName);

            assertThat(metastore.getTable(testSchema, tableName).orElseThrow().getDataColumns())
                    .extracting(Column::getName)
                    .containsExactly("event_id", "amount");
        }
        finally {
            metastore.dropTable(testSchema, tableName, false);
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

    @Test
    void testUnresolvableAvroSchemaFailsTableLoad()
    {
        String tableName = "test_avro_missing_schema_" + randomNameSuffix();
        createTableWithDriftedColumns(tableName, AVRO, ImmutableList.of(), ImmutableMap.of(AVRO_SCHEMA_URL_KEY, "%s/missing.avsc".formatted(warehouseLocation)));
        try {
            // Loading the table fails rather than falling back to the columns stored in Glue
            assertTrinoExceptionThrownBy(() -> metastore.getTable(testSchema, tableName))
                    .hasErrorCode(HIVE_INVALID_METADATA)
                    .hasMessageContaining("Failed to resolve the Avro schema of table %s.%s".formatted(testSchema, tableName));

            // HIVE_INVALID_METADATA is an EXTERNAL error, so HiveMetadata#streamTableColumns skips this table
            // instead of failing the listing for the whole schema
            assertQuery("SELECT count(*) FROM information_schema.columns WHERE table_name = '%s'".formatted(tableName), "VALUES 0");
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
}
