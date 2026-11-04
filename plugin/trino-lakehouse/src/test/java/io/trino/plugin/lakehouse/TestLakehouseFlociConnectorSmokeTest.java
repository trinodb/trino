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
package io.trino.plugin.lakehouse;

import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLakehouseFlociConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        FlociS3AndGlue floci = closeAfterClass(new FlociS3AndGlue());
        floci.createBucket("test-bucket");

        return LakehouseQueryRunner.builder()
                .addLakehouseProperty("hive.metastore", "glue")
                .addLakehouseProperty("hive.metastore-cache-ttl", "1d")
                .addLakehouseProperty("hive.metastore.glue.default-warehouse-dir", "s3://test-bucket/")
                .addLakehouseProperty("fs.s3.enabled", "true")
                .addLakehouseProperties(floci.s3AndGlueProperties())
                .build();
    }

    @BeforeAll
    public void setUp()
    {
        computeActual("CREATE SCHEMA lakehouse.tpch WITH (location='s3://test-bucket/tpch')");
        copyTpchTables(getQueryRunner(), "tpch", TINY_SCHEMA_NAME, REQUIRED_TPCH_TABLES);
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertQueryFails(
                "ALTER SCHEMA %s RENAME TO %s".formatted(schemaName, schemaName + "_" + randomNameSuffix()),
                ".*Database rename is not supported by the Glue service.*");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region")).matches(
                """
                \\QCREATE TABLE lakehouse.tpch.region (
                   regionkey bigint,
                   name varchar,
                   comment varchar
                )
                WITH (
                   format = 'PARQUET',
                   format_version = 2,
                   location = 's3://test-bucket/tpch/region-\\E.*\\Q',
                   type = 'ICEBERG'
                )\\E""");
    }

    @Test
    public void testGlueCacheInvalidation()
    {
        String tableName = "test_cache_invalidation_" + randomNameSuffix();
        String renamedTableName = "test_cache_invalidation_renamed_" + randomNameSuffix();

        try {
            // Populate negative table cache entry and table names cache before creating the Iceberg table
            assertQueryFails("SELECT * FROM " + tableName, ".*Table .* does not exist.*");
            assertThat(computeActual("SHOW TABLES LIKE '" + tableName + "'").getRowCount()).isZero();

            assertUpdate("CREATE TABLE " + tableName + " (a int)");
            assertThat(computeActual("SHOW TABLES LIKE '" + tableName + "'").getOnlyValue()).isEqualTo(tableName);
            assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);
            assertQuery("SELECT a FROM " + tableName, "VALUES 1");

            // Drop and recreate with a different schema to verify stale table metadata is invalidated
            assertUpdate("DROP TABLE " + tableName);
            assertQueryFails("SELECT * FROM " + tableName, ".*Table .* does not exist.*");
            assertUpdate("CREATE TABLE " + tableName + " (b varchar)");
            assertUpdate("INSERT INTO " + tableName + " VALUES 'hello'", 1);
            assertQuery("SELECT b FROM " + tableName, "VALUES 'hello'");

            // Rename and verify both source and target cache entries are invalidated
            assertUpdate("ALTER TABLE " + tableName + " RENAME TO " + renamedTableName);
            assertQueryFails("SELECT * FROM " + tableName, ".*Table .* does not exist.*");
            assertQuery("SELECT b FROM " + renamedTableName, "VALUES 'hello'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
            assertUpdate("DROP TABLE IF EXISTS " + renamedTableName);
        }
    }
}
