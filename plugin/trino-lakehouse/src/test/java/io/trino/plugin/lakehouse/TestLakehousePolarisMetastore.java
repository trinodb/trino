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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Optional;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLakehousePolarisMetastore
        extends AbstractTestQueryFramework
{
    private String warehouseLocation;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        java.nio.file.Path tempDir = java.nio.file.Files.createTempDirectory("trino-polaris-test-");
        warehouseLocation = tempDir.toString();

        java.io.File warehouseDir = tempDir.toFile();
        warehouseDir.setReadable(true, false);
        warehouseDir.setWritable(true, false);
        warehouseDir.setExecutable(true, false);

        TestingPolarisCatalogLakehouse polaris = closeAfterClass(new TestingPolarisCatalogLakehouse(warehouseLocation));

        LakehouseQueryRunner.Builder builder = LakehouseQueryRunner.builder()
                .setBaseDataDir(Optional.of(tempDir))
                .addLakehouseProperty("lakehouse.table-type", "ICEBERG");

        ImmutableMap<String, String> polarisProps = polaris.getTrinoConfigurationProperties();

        builder.addLakehouseProperty("iceberg.metadata-cache.enabled", "false");
        polarisProps.forEach(builder::addLakehouseProperty);

        QueryRunner queryRunner = builder.build();
        return queryRunner;
    }

    @BeforeAll
    public void setUp()
    {
        java.io.File warehouseDir = new java.io.File(warehouseLocation);

        if (!warehouseDir.exists()) {
            warehouseDir.mkdirs();
        }
        computeActual("CREATE SCHEMA lakehouse.test_schema WITH (location = 'file://%s')".formatted(warehouseLocation));
    }

    @Test
    public void testDeltaLakeTableOperations()
    {
        String tableName = "test_delta_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE lakehouse.test_schema." + tableName + " (id BIGINT, name VARCHAR(100), value DOUBLE) " +
                    "WITH (type = 'DELTA', format = 'PARQUET')");

            assertQuery("SELECT count(*) FROM lakehouse.test_schema." + tableName, "VALUES (0)");

            assertUpdate("INSERT INTO lakehouse.test_schema." + tableName + " VALUES (1, 'test', 3.14)", 1);
            assertQuery("SELECT * FROM lakehouse.test_schema." + tableName, "VALUES (1, 'test', 3.14)");

            assertUpdate("INSERT INTO lakehouse.test_schema." + tableName + " VALUES (2, 'another', 2.71)", 1);
            assertQuery("SELECT count(*) FROM lakehouse.test_schema." + tableName, "VALUES (2)");
            assertQuery("SELECT id, name FROM lakehouse.test_schema." + tableName + " ORDER BY id",
                       "VALUES (1, 'test'), (2, 'another')");

            String createTableSql = (String) computeScalar("SHOW CREATE TABLE lakehouse.test_schema." + tableName);
            assertThat(createTableSql).contains("type = 'DELTA'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS lakehouse.test_schema." + tableName);
        }
    }

    @Test
    public void testBasicIcebergTableOperations()
    {
        String tableName = "test_iceberg_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE lakehouse.test_schema." + tableName + " (id BIGINT, name VARCHAR(100), value DOUBLE) " +
                    "WITH (type = 'ICEBERG', format = 'PARQUET')");

            assertQuery("SELECT count(*) FROM lakehouse.test_schema." + tableName, "VALUES (0)");
            assertUpdate("INSERT INTO lakehouse.test_schema." + tableName + " VALUES (1, 'test', 3.14)", 1);
            assertQuery("SELECT * FROM lakehouse.test_schema." + tableName, "VALUES (1, 'test', 3.14)");

            assertUpdate("INSERT INTO lakehouse.test_schema." + tableName + " VALUES (2, 'another', 2.71)", 1);
            assertQuery("SELECT count(*) FROM lakehouse.test_schema." + tableName, "VALUES (2)");
            assertQuery("SELECT id, name FROM lakehouse.test_schema." + tableName + " ORDER BY id",
                       "VALUES (1, 'test'), (2, 'another')");

            String createTableSql = (String) computeScalar("SHOW CREATE TABLE lakehouse.test_schema." + tableName);
            assertThat(createTableSql).contains("type = 'ICEBERG'");
            assertThat(createTableSql).contains("format = 'PARQUET'");
        }
        catch (Exception e) {
            throw e;
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS lakehouse.test_schema." + tableName);
        }
    }

    @Test
    public void testSchemaOperations()
    {
        String schemaName = "test_schema_" + randomNameSuffix();
        try {
            assertUpdate("CREATE SCHEMA lakehouse." + schemaName);
            assertThat(computeActual("SHOW SCHEMAS FROM lakehouse").getOnlyColumnAsSet()).contains(schemaName);

            String tableName = "test_table";
            assertUpdate("CREATE TABLE lakehouse." + schemaName + "." + tableName + " (id BIGINT) WITH (type = 'ICEBERG')");

            assertThat(computeActual("SHOW TABLES FROM lakehouse." + schemaName).getOnlyColumnAsSet()).contains(tableName);

            assertUpdate("DROP TABLE lakehouse." + schemaName + "." + tableName);
        }
        finally {
            assertUpdate("DROP SCHEMA IF EXISTS lakehouse." + schemaName);
        }
    }

    @Test
    public void testBasicQueries()
    {
        String tableName = "test_queries_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE lakehouse.test_schema." + tableName + " (id BIGINT, name VARCHAR(100)) " +
                    "WITH (type = 'ICEBERG', format = 'PARQUET')");

            assertUpdate("INSERT INTO lakehouse.test_schema." + tableName + " VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", 3);

            assertQuery("SELECT count(*) FROM lakehouse.test_schema." + tableName, "VALUES (3)");
            assertQuery("SELECT name FROM lakehouse.test_schema." + tableName + " WHERE id = 2", "VALUES ('Bob')");
            assertQuery("SELECT id FROM lakehouse.test_schema." + tableName + " WHERE name LIKE 'C%'", "VALUES (3)");
            assertQuery("SELECT id, name FROM lakehouse.test_schema." + tableName + " ORDER BY id LIMIT 2",
                       "VALUES (1, 'Alice'), (2, 'Bob')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS lakehouse.test_schema." + tableName);
        }
    }
}
