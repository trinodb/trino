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
package io.trino.plugin.iceberg.catalog.hms;

import com.google.common.collect.ImmutableMap;
import io.trino.hive.thrift.metastore.Table;
import io.trino.plugin.hive.containers.Hive4FlociDataLake;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.iceberg.catalog.hms.HiveMetastoreTableOperations.isConcurrentModificationRejection;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestIcebergHiveCatalogWithoutLock
        extends AbstractTestQueryFramework
{
    private Hive4FlociDataLake hiveFlociDataLake;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String bucketName = "test-bucket" + randomNameSuffix();
        hiveFlociDataLake = closeAfterClass(new Hive4FlociDataLake(bucketName));
        hiveFlociDataLake.start();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "HIVE_METASTORE")
                                .put("hive.metastore.uri", hiveFlociDataLake.getHiveMetastoreEndpoint().toString())
                                .put("iceberg.hive-catalog.locking-enabled", "false")
                                .put("fs.s3.enabled", "true")
                                .put("s3.aws-access-key", FLOCI_ACCESS_KEY)
                                .put("s3.aws-secret-key", FLOCI_SECRET_KEY)
                                .put("s3.endpoint", hiveFlociDataLake.floci().endpoint().toString())
                                .put("s3.region", FLOCI_REGION)
                                .put("s3.path-style-access", "true")
                                .buildOrThrow())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withSchemaName("tpch")
                                .withSchemaProperties(Map.of("location", "'s3://%s/tpch'".formatted(bucketName)))
                                .build())
                .build();
    }

    @Test
    void testCommitWithoutLock()
    {
        try (TestTable table = newTrinoTable("test_lock", "(x int)", List.of("1", "2", "3"))) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 1", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES 2, 3");

            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 2", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES 3");
        }
    }

    @Test
    void testConcurrentModificationRejectedByMetastoreIsRecognized()
    {
        // The rejection message is what HiveMetastoreTableOperations relies on to tell a definite conflict from an unknown outcome
        try (TestTable table = newTrinoTable("test_conditional_update", "(x int)", List.of("1"))) {
            ThriftMetastore thriftMetastore = testingThriftHiveMetastoreBuilder()
                    .metastoreClient(hiveFlociDataLake.getHiveMetastoreEndpoint())
                    .build(this::closeAfterClass);
            Table hiveTable = thriftMetastore.getTable("tpch", table.getName()).orElseThrow();
            String currentMetadataLocation = hiveTable.getParameters().get(METADATA_LOCATION_PROP);

            Table updatedTable = hiveTable.deepCopy();
            updatedTable.getParameters().put(METADATA_LOCATION_PROP, currentMetadataLocation + ".rejected");
            assertThatThrownBy(() -> thriftMetastore.alterTable("tpch", table.getName(), updatedTable, ImmutableMap.of(
                    "expected_parameter_key", METADATA_LOCATION_PROP,
                    "expected_parameter_value", currentMetadataLocation + ".stale")))
                    .satisfies(exception -> assertThat(isConcurrentModificationRejection(exception)).isTrue());

            assertThat(thriftMetastore.getTable("tpch", table.getName()).orElseThrow().getParameters())
                    .containsEntry(METADATA_LOCATION_PROP, currentMetadataLocation);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES 1");
        }
    }
}
