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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergCaseInsensitiveNameMatching
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.of(
                        "iceberg.case-sensitive-name-matching", "false"))
                .build();
    }

    @Test
    public void testCaseInsensitiveSchemaCreation()
    {
        // All these should create the same schema (lowercased)
        assertUpdate("CREATE SCHEMA \"MixedCaseSchema\"");

        // Trying to create with different case should fail (same schema)
        assertQueryFails("CREATE SCHEMA \"MIXEDCASESCHEMA\"", ".*already exists.*");
        assertQueryFails("CREATE SCHEMA mixedcaseschema", ".*already exists.*");

        // Verify schema exists in lowercase
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet())
                .contains("mixedcaseschema");
    }

    @Test
    public void testCaseInsensitiveTableCreation()
    {
        assertUpdate("CREATE SCHEMA test_schema");

        // Create table with mixed case
        assertUpdate("CREATE TABLE test_schema.\"MixedCaseTable\" (id bigint, name varchar)");

        // Trying to create with different case should fail (same table)
        assertQueryFails("CREATE TABLE test_schema.\"MIXEDCASETABLE\" (id bigint)", ".*already exists.*");
        assertQueryFails("CREATE TABLE test_schema.mixedcasetable (id bigint)", ".*already exists.*");

        // Verify table exists in lowercase
        assertThat(computeActual("SHOW TABLES FROM test_schema").getOnlyColumnAsSet())
                .contains("mixedcasetable");
    }

    @Test
    public void testCaseInsensitiveTableAccess()
    {
        assertUpdate("CREATE SCHEMA test_access");
        assertUpdate("CREATE TABLE test_access.\"CaseSensitiveTable\" (id bigint, name varchar)");

        // Insert data using exact case
        assertUpdate("INSERT INTO test_access.\"CaseSensitiveTable\" VALUES (1, 'test')", 1);

        // Query with any case should work (all resolve to lowercase)
        assertQuery("SELECT * FROM test_access.\"CaseSensitiveTable\"", "VALUES (1, 'test')");
        assertQuery("SELECT * FROM test_access.\"casesensitivetable\"", "VALUES (1, 'test')");
        assertQuery("SELECT * FROM test_access.\"CASESENSITIVETABLE\"", "VALUES (1, 'test')");
        assertQuery("SELECT * FROM test_access.casesensitivetable", "VALUES (1, 'test')");
    }

    @Test
    public void testAllIdentifierTypesAreCaseInsensitive()
    {
        assertUpdate("CREATE SCHEMA test_all_types");

        // Create table with mixed case (both delimited and non-delimited)
        assertUpdate("CREATE TABLE test_all_types.\"MixedCaseTable\" (id bigint, name varchar)");

        // All these should refer to the same table
        assertUpdate("INSERT INTO test_all_types.\"mixedcasetable\" VALUES (1, 'test1')", 1);
        assertUpdate("INSERT INTO test_all_types.\"MIXEDCASETABLE\" VALUES (2, 'test2')", 1);
        assertUpdate("INSERT INTO test_all_types.MixedCaseTable VALUES (3, 'test3')", 1);
        assertUpdate("INSERT INTO test_all_types.mixedcasetable VALUES (4, 'test4')", 1);

        assertQuery("SELECT COUNT(*) FROM test_all_types.mixedcasetable", "VALUES 4");
    }

    @Test
    public void testCaseInsensitiveColumnNames()
    {
        assertUpdate("CREATE SCHEMA test_columns");
        assertUpdate("CREATE TABLE test_columns.test_table (" +
                "\"MixedCaseColumn\" bigint, " +
                "\"UPPERCASECOLUMN\" varchar, " +
                "lowercasecolumn boolean)");

        // Insert with exact column names
        assertUpdate("INSERT INTO test_columns.test_table VALUES (1, 'test', true)", 1);

        // Query with any case should work (all columns are lowercased)
        assertQuery("SELECT \"MixedCaseColumn\", \"UPPERCASECOLUMN\", lowercasecolumn FROM test_columns.test_table",
                "VALUES (1, 'test', true)");
        assertQuery("SELECT \"mixedcasecolumn\", \"uppercasecolumn\", \"LOWERCASECOLUMN\" FROM test_columns.test_table",
                "VALUES (1, 'test', true)");
        assertQuery("SELECT mixedcasecolumn, uppercasecolumn, lowercasecolumn FROM test_columns.test_table",
                "VALUES (1, 'test', true)");
    }

    @Test
    public void testBackwardCompatibility()
    {
        // Test that existing behavior is preserved when case sensitivity is disabled
        assertUpdate("CREATE SCHEMA backward_compat");
        assertUpdate("CREATE TABLE backward_compat.existing_table (id bigint)");

        // Should be able to access with any case
        assertUpdate("INSERT INTO backward_compat.EXISTING_TABLE VALUES (1)", 1);
        assertUpdate("INSERT INTO backward_compat.\"Existing_Table\" VALUES (2)", 1);

        assertQuery("SELECT COUNT(*) FROM backward_compat.existing_table", "VALUES 2");
    }

    @Test
    public void testCatalogPropertyConfiguration()
    {
        // Test that the configuration is properly applied
        ConnectorSession session = getQueryRunner().getDefaultSession().toConnectorSession();
        TrinoCatalog catalog = ((IcebergConnector) getQueryRunner()
                .getCoordinator()
                .getConnector(ICEBERG_CATALOG))
                .getInjector()
                .getInstance(TrinoCatalog.class);

        // Test case-insensitive canonicalization (all should be lowercased)
        assertThat(catalog.canonicalize("MixedCase", true)).isEqualTo("mixedcase");
        assertThat(catalog.canonicalize("MixedCase", false)).isEqualTo("mixedcase");
        assertThat(catalog.canonicalize("UPPERCASE", true)).isEqualTo("uppercase");
        assertThat(catalog.canonicalize("UPPERCASE", false)).isEqualTo("uppercase");
    }
}
