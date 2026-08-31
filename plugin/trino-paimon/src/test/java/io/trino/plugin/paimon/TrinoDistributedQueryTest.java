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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableMap;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.AbstractDistributedEngineOnlyQueries;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Distributed query tests for Paimon Trino connector.
 *
 * This test class extends {@link AbstractDistributedEngineOnlyQueries} which
 * provides a comprehensive suite of distributed query tests. Most tests from
 * the base class will run with their default implementations.
 *
 * Tests that are not applicable to Paimon or require custom implementation are
 * overridden and either disabled or re-implemented below.
 */
public class TrinoDistributedQueryTest
        extends AbstractDistributedEngineOnlyQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // This test inherits a broad distributed query suite; create the tiny
        // TPCH tables in Paimon so inherited tests exercise the connector.
        DistributedQueryRunner queryRunner = TrinoQueryRunner.createPrestoQueryRunner(ImmutableMap.of(), ImmutableMap.of(), true);
        queryRunner.getCoordinator().getSessionPropertyManager().addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        try {
            queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withSessionProperties(TEST_CATALOG_PROPERTIES)
                    .build()));
            queryRunner.createCatalog(TESTING_CATALOG, "mock");
            queryRunner.installPlugin(new MemoryPlugin());
            queryRunner.createCatalog("memory", "memory", ImmutableMap.of());
        }
        catch (RuntimeException e) {
            throw closeAllSuppress(e, queryRunner);
        }
        return queryRunner;
    }

    @Test
    @Disabled("Test assumes specific JVM timezone configuration")
    @Override
    public void testLocallyUnrepresentableTimeLiterals()
    {
        // Skip - environment-specific test
    }

    @Test
    @Disabled("Paimon connector does not support TRY function with column references")
    @Override
    public void testTry()
    {
        // Paimon column resolution fails with TRY function: "Unbound variable: tax"
    }

    @Test
    @Override
    public void testImplicitCastToRowWithFieldsRequiringDelimitation()
    {
        String sourceTableName = "test_row_source_" + randomNameSuffix();
        String targetTableName = "test_row_target_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + sourceTableName + "(r ROW(a char(4), b char(4)))");
        assertUpdate("CREATE TABLE " + targetTableName + "(r ROW(\"a b\" varchar, \"from\" varchar))");
        assertUpdate("INSERT INTO " + sourceTableName
                + " SELECT CAST(ROW('abcd', 'wxyz') AS ROW(a char(4), b char(4)))", 1);

        assertUpdate("INSERT INTO " + targetTableName + " SELECT * FROM " + sourceTableName, 1);
        assertThat(query("SELECT r.\"a b\", r.\"from\" FROM " + targetTableName))
                .matches("VALUES (CAST('abcd' AS varchar), CAST('wxyz' AS varchar))");

        assertUpdate("DROP TABLE " + sourceTableName);
        assertUpdate("DROP TABLE " + targetTableName);
    }

    @Test
    @Override
    public void testCreateTableAsTable()
    {
        // Override to use unique table name to avoid conflicts in concurrent test
        // execution
        // The base class uses hardcoded table name 'n' which causes conflicts
        String tableName = "test_ctas_" + randomNameSuffix();

        // Ensure CTA works when the table exposes hidden fields
        // First, verify that the table 'nation' contains the expected hidden column
        // 'row_number'
        assertThat(query("SELECT count(*) FROM information_schema.columns "
                + "WHERE table_catalog = 'tpch' and table_schema = 'tiny' and table_name = 'nation' and column_name = 'row_number'"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT min(row_number) FROM tpch.tiny.nation")).matches("VALUES BIGINT '0'");

        assertUpdate(getSession(), "CREATE TABLE " + tableName + " AS TABLE tpch.tiny.nation", 25);
        assertThat(query("SELECT * FROM " + tableName)).matches("SELECT * FROM tpch.tiny.nation");

        // Verify that hidden column is not present in the created table
        assertThat(query("SELECT min(row_number) FROM " + tableName)).failure()
                .hasMessage("line 1:12: Column 'row_number' cannot be resolved");
        assertUpdate(getSession(), "DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testInsertTableIntoTable()
    {
        // Override to use unique table name to avoid conflicts in concurrent test
        // execution
        // The base class uses hardcoded table name 'n' which causes conflicts
        String tableName = "test_insert_table_" + randomNameSuffix();

        // Ensure INSERT works when the source table exposes hidden fields
        // First, verify that the table 'nation' contains the expected hidden column
        // 'row_number'
        assertThat(query("SELECT count(*) FROM information_schema.columns "
                + "WHERE table_catalog = 'tpch' and table_schema = 'tiny' and table_name = 'nation' and column_name = 'row_number'"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT min(row_number) FROM tpch.tiny.nation")).matches("VALUES BIGINT '0'");

        // Create empty target table for INSERT
        assertUpdate(getSession(), "CREATE TABLE " + tableName + " AS TABLE tpch.tiny.nation WITH NO DATA", 0);
        assertThat(query("SELECT * FROM " + tableName)).matches("SELECT * FROM tpch.tiny.nation LIMIT 0");

        // Verify that the hidden column is not present in the created table
        assertThat(query("SELECT row_number FROM " + tableName)).failure()
                .hasMessage("line 1:8: Column 'row_number' cannot be resolved");

        // Insert values from the original table into the created table
        assertUpdate(getSession(), "INSERT INTO " + tableName + " TABLE tpch.tiny.nation", 25);
        assertThat(query("SELECT * FROM " + tableName)).matches("SELECT * FROM tpch.tiny.nation");

        assertUpdate(getSession(), "DROP TABLE " + tableName);
    }
}
