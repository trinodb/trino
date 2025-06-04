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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.hive.metastore.glue.TestingGlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Tests metadata operations on a schema which has a mix of Hive and Delta Lake tables.
 * <p>
 * Requires AWS credentials, which can be provided any way supported by the DefaultProviderChain
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 */
@TestInstance(PER_CLASS)
public class TestDeltaLakeSharedGlueMetastoreWithTableRedirections
        extends BaseDeltaLakeSharedMetastoreWithTableRedirectionsTest
{
    private Path dataDirectory;
    private GlueHiveMetastore glueMetastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session deltaLakeSession = testSessionBuilder()
                .setCatalog("delta_with_redirections")
                .setSchema(schema)
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(deltaLakeSession).build();

        this.dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data");

        queryRunner.installPlugin(new DeltaLakePlugin());
        queryRunner.createCatalog(
                "delta_with_redirections",
                "delta_lake",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "glue")
                        .put("hive.metastore.glue.default-warehouse-dir", dataDirectory.toUri().toString())
                        .put("delta.hive-catalog-name", "hive_with_redirections")
                        .put("fs.hadoop.enabled", "true")
                        .buildOrThrow());

        this.glueMetastore = createTestingGlueHiveMetastore(dataDirectory, this::closeAfterClass);
        queryRunner.installPlugin(new TestingHivePlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data"), glueMetastore));
        queryRunner.createCatalog(
                "hive_with_redirections",
                "hive",
                ImmutableMap.of("hive.delta-lake-catalog-name", "delta_with_redirections", "fs.hadoop.enabled", "true"));

        queryRunner.execute("CREATE SCHEMA " + schema + " WITH (location = '" + dataDirectory.toUri() + "')");
        queryRunner.execute("CREATE TABLE hive_with_redirections." + schema + ".hive_table (a_integer) WITH (format='PARQUET') AS VALUES 1, 2, 3");
        queryRunner.execute("CREATE TABLE delta_with_redirections." + schema + ".delta_table (a_varchar) AS VALUES 'a', 'b', 'c'");

        return queryRunner;
    }

    @AfterAll
    public void cleanup()
    {
        // Data is on the local disk and will be deleted by the deleteOnExit hook
        glueMetastore.dropDatabase(schema, false);
        glueMetastore.shutdown();
    }

    @Override
    protected String getExpectedHiveCreateSchema(String catalogName)
    {
        String expectedHiveCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "WITH (\n" +
                "   location = '%s'\n" +
                ")";

        return format(expectedHiveCreateSchema, catalogName, schema, dataDirectory.toUri());
    }

    @Override
    protected String getExpectedDeltaLakeCreateSchema(String catalogName)
    {
        String expectedDeltaLakeCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "WITH (\n" +
                "   location = '%s'\n" +
                ")";
        return format(expectedDeltaLakeCreateSchema, catalogName, schema, dataDirectory.toUri());
    }

    @Test
    public void testUnsupportedHiveTypeRedirect()
    {
        String tableName = "unsupported_types";
        // Use another complete table location so `SHOW CREATE TABLE` doesn't fail on reading metadata
        String location;
        try (GlueClient glueClient = GlueClient.create()) {
            GetTableResponse existingTable = glueClient.getTable(GetTableRequest.builder()
                    .databaseName(schema)
                    .name("delta_table")
                    .build());
            location = existingTable.table().storageDescriptor().location();
        }
        // Create a table directly in Glue, simulating an external table being created in Spark,
        // with a custom AWS data type not mapped to HiveType when
        Column timestampColumn = Column.builder()
                .name("last_hour_load")
                .type("timestamp_ntz")
                .build();
        StorageDescriptor sd = StorageDescriptor.builder()
                .columns(timestampColumn)
                .location(location)
                .inputFormat("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                .outputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
                .serdeInfo(SerDeInfo.builder()
                        .serializationLibrary("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                        .parameters(Map.of(
                                "serialization.format", "1",
                                "path", location))
                        .build())
                .build();
        TableInput tableInput = TableInput.builder()
                .name(tableName)
                .storageDescriptor(sd)
                .parameters(Map.of(
                        "spark.sql.sources.provider", "delta"))
                .tableType("EXTERNAL_TABLE")
                .partitionKeys(timestampColumn)
                .build();

        CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .databaseName(schema)
                .tableInput(tableInput)
                .build();
        try (GlueClient glueClient = GlueClient.create()) {
            glueClient.createTable(createTableRequest);

            String tableDefinition = (String) computeScalar("SHOW CREATE TABLE hive_with_redirections." + schema + "." + tableName);
            String expected = """
                    CREATE TABLE delta_with_redirections.%s.%s (
                       a_varchar varchar
                    )
                    WITH (
                       location = '%s'
                    )""";
            assertThat(tableDefinition).isEqualTo(expected.formatted(schema, tableName, location));

            glueClient.deleteTable(DeleteTableRequest.builder()
                    .databaseName(schema)
                    .name(tableInput.name())
                    .build());
        }
    }
}
