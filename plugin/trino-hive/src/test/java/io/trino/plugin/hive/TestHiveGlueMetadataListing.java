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
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.List;
import java.util.Set;

import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveGlueMetadataListing
        extends AbstractTestQueryFramework
{
    public static final String FAILING_TABLE_WITH_NULL_STORAGE_DESCRIPTOR_NAME = "failing_table_with_null_storage_descriptor";
    public static final String FAILING_TABLE_WITH_NULL_TYPE = "failing_table_with_null_type";
    public static final String FAILING_TABLE_WITH_BAD_HIVE_TYPE = "failing_table_with_bad_hive_type";
    public static final String FAILING_TABLE_WITH_NULL_SERDE = "failing_table_with_null_serde";
    private static final Logger LOG = Logger.get(TestHiveGlueMetadataListing.class);
    private static final String HIVE_CATALOG = "hive";
    private final String tpchSchema = "test_tpch_schema_" + randomNameSuffix();
    private FlociS3AndGlue floci;
    private GlueHiveMetastore glueMetastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session hiveSession = testSessionBuilder()
                .setCatalog(HIVE_CATALOG)
                .setSchema(tpchSchema)
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(hiveSession).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        floci = closeAfterClass(new FlociS3AndGlue());
        String bucketName = "test-hive-glue-metadata-listing-" + randomNameSuffix();
        floci.createBucket(bucketName);
        String schemaLocation = "s3://%s/%s".formatted(bucketName, tpchSchema);

        queryRunner.installPlugin(new HivePlugin());
        queryRunner.createCatalog(HIVE_CATALOG, "hive", ImmutableMap.<String, String>builder()
                .put("hive.metastore", "glue")
                .put("hive.metastore.glue.default-warehouse-dir", "s3://%s/".formatted(bucketName))
                .put("fs.s3.enabled", "true")
                .putAll(floci.s3AndGlueProperties())
                .buildOrThrow());
        glueMetastore = getConnectorService(queryRunner, GlueHiveMetastore.class);

        queryRunner.execute("CREATE SCHEMA " + tpchSchema + " WITH (location = '" + schemaLocation + "')");
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, hiveSession, ImmutableList.of(TpchTable.REGION, TpchTable.NATION));

        createBrokenTables();

        return queryRunner;
    }

    @AfterAll
    public void cleanup()
    {
        try {
            if (glueMetastore != null) {
                glueMetastore.dropDatabase(tpchSchema, false);
            }
        }
        catch (Exception e) {
            LOG.error(e, "Failed to clean up Glue database: %s", tpchSchema);
        }
    }

    @Test
    public void testReadInformationSchema()
    {
        Set<String> expectedTables = ImmutableSet.<String>builder()
                .add(TpchTable.REGION.getTableName())
                .add(TpchTable.NATION.getTableName())
                .add(FAILING_TABLE_WITH_NULL_STORAGE_DESCRIPTOR_NAME)
                .add(FAILING_TABLE_WITH_NULL_TYPE)
                .add(FAILING_TABLE_WITH_BAD_HIVE_TYPE)
                .add(FAILING_TABLE_WITH_NULL_SERDE)
                .build();

        assertThat(computeActual("SELECT table_name FROM hive.information_schema.tables").getOnlyColumnAsSet()).containsAll(expectedTables);
        assertThat(computeActual("SELECT table_name FROM hive.information_schema.tables WHERE table_schema='" + tpchSchema + "'").getOnlyColumnAsSet()).containsAll(expectedTables);
        assertThat(computeScalar("SELECT table_name FROM hive.information_schema.tables WHERE table_name = 'region' AND table_schema='" + tpchSchema + "'"))
                .isEqualTo(TpchTable.REGION.getTableName());
        assertQueryReturnsEmptyResult(format("SELECT table_name FROM hive.information_schema.tables WHERE table_name = '%s' AND table_schema='%s'", FAILING_TABLE_WITH_NULL_STORAGE_DESCRIPTOR_NAME, tpchSchema));
        assertQueryReturnsEmptyResult(format("SELECT table_name FROM hive.information_schema.tables WHERE table_name = '%s' AND table_schema='%s'", FAILING_TABLE_WITH_NULL_TYPE, tpchSchema));
        assertQueryReturnsEmptyResult(format("SELECT table_name FROM hive.information_schema.tables WHERE table_name = '%s' AND table_schema='%s'", FAILING_TABLE_WITH_BAD_HIVE_TYPE, tpchSchema));
        assertQueryReturnsEmptyResult(format("SELECT table_name FROM hive.information_schema.tables WHERE table_name = '%s' AND table_schema='%s'", FAILING_TABLE_WITH_NULL_SERDE, tpchSchema));

        assertQuery("SELECT table_name, column_name from hive.information_schema.columns WHERE table_schema = '" + tpchSchema + "'",
                "VALUES ('region', 'regionkey'), ('region', 'name'), ('region', 'comment'), ('nation', 'nationkey'), ('nation', 'name'), ('nation', 'regionkey'), ('nation', 'comment')");
        assertQuery("SELECT table_name, column_name from hive.information_schema.columns WHERE table_name = 'region' AND table_schema='" + tpchSchema + "'",
                "VALUES ('region', 'regionkey'), ('region', 'name'), ('region', 'comment')");
        assertQueryReturnsEmptyResult(format("SELECT table_name FROM hive.information_schema.columns WHERE table_name = '%s' AND table_schema='%s'", FAILING_TABLE_WITH_NULL_STORAGE_DESCRIPTOR_NAME, tpchSchema));
        assertQueryReturnsEmptyResult(format("SELECT table_name FROM hive.information_schema.columns WHERE table_name = '%s' AND table_schema='%s'", FAILING_TABLE_WITH_NULL_TYPE, tpchSchema));
        assertQueryReturnsEmptyResult(format("SELECT table_name FROM hive.information_schema.columns WHERE table_name = '%s' AND table_schema='%s'", FAILING_TABLE_WITH_BAD_HIVE_TYPE, tpchSchema));
        assertQueryReturnsEmptyResult(format("SELECT table_name FROM hive.information_schema.columns WHERE table_name = '%s' AND table_schema='%s'", FAILING_TABLE_WITH_NULL_SERDE, tpchSchema));

        assertThat(computeActual("SHOW TABLES FROM hive." + tpchSchema).getOnlyColumnAsSet()).isEqualTo(expectedTables);
    }

    private void createBrokenTables()
    {
        TableInput nullStorageTable = TableInput.builder()
                .name(FAILING_TABLE_WITH_NULL_STORAGE_DESCRIPTOR_NAME)
                .tableType("HIVE")
                .build();
        TableInput nullTypeTable = TableInput.builder()
                .name(FAILING_TABLE_WITH_NULL_TYPE)
                .build();
        TableInput badHiveTypeTable = TableInput.builder()
                .name(FAILING_TABLE_WITH_BAD_HIVE_TYPE)
                .tableType("HIVE")
                .storageDescriptor(
                        StorageDescriptor.builder()
                                .columns(Column.builder().name("badhivetype").type("notarealtype").build())
                                .serdeInfo(SerDeInfo.builder().serializationLibrary("org.openx.data.jsonserde.JsonSerDe").build())
                                .build())
                .build();
        TableInput nullSerdeTable = TableInput.builder()
                .name(FAILING_TABLE_WITH_NULL_SERDE)
                .tableType("HIVE")
                .storageDescriptor(
                        StorageDescriptor.builder()
                                .columns(Column.builder().name("goodhivetype").type("string").build())
                                .build())
                .build();
        createBrokenTable(List.of(nullStorageTable, nullTypeTable, badHiveTypeTable, nullSerdeTable));
    }

    private void createBrokenTable(List<TableInput> tablesInput)
    {
        try (GlueClient glueClient = floci.createGlueClient()) {
            for (TableInput tableInput : tablesInput) {
                CreateTableRequest createTableRequest = CreateTableRequest.builder()
                        .databaseName(tpchSchema)
                        .tableInput(tableInput)
                        .build();
                glueClient.createTable(createTableRequest);
            }
        }
    }
}
