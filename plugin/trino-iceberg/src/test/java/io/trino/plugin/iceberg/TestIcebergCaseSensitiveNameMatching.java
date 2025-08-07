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

public class TestIcebergCaseSensitiveNameMatching
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.of(
                        "iceberg.case-sensitive-name-matching", "true"))
                .build();
    }

    @Test
    public void testCaseSensitiveSchemaCreation()
    {
        // Test creating schemas with different cases
        assertUpdate("CREATE SCHEMA \"MixedCaseSchema\"");
        assertUpdate("CREATE SCHEMA \"UPPERCASESCHEMA\"");
        assertUpdate("CREATE SCHEMA lowercaseschema");

        // Verify schemas exist with correct case
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet())
                .contains("MixedCaseSchema", "UPPERCASESCHEMA", "lowercaseschema");
    }

    @Test
    public void testCaseSensitiveTableCreation()
    {
        assertUpdate("CREATE SCHEMA test_schema");

        // Create tables with different cases
        assertUpdate("CREATE TABLE test_schema.\"MixedCaseTable\" (id bigint, name varchar)");
        assertUpdate("CREATE TABLE test_schema.\"UPPERCASETABLE\" (id bigint, name varchar)");
        assertUpdate("CREATE TABLE test_schema.lowercasetable (id bigint, name varchar)");

        // Verify tables exist with correct case
        assertThat(computeActual("SHOW TABLES FROM test_schema").getOnlyColumnAsSet())
                .contains("MixedCaseTable", "UPPERCASETABLE", "lowercasetable");
    }

    @Test
    public void testCaseSensitiveTableAccess()
    {
        assertUpdate("CREATE SCHEMA test_access");
        assertUpdate("CREATE TABLE test_access.\"CaseSensitiveTable\" (id bigint, name varchar)");

        // Insert data using exact case
        assertUpdate("INSERT INTO test_access.\"CaseSensitiveTable\" VALUES (1, 'test')", 1);

        // Query with exact case should work
        assertQuery("SELECT * FROM test_access.\"CaseSensitiveTable\"", "VALUES (1, 'test')");

        // Query with different case should fail
        assertQueryFails("SELECT * FROM test_access.\"casesensitivetable\"", ".*Table.*not found.*");
        assertQueryFails("SELECT * FROM test_access.\"CASESENSITIVETABLE\"", ".*Table.*not found.*");
    }

    @Test
    public void testNonDelimitedIdentifiersAreCaseInsensitive()
    {
        assertUpdate("CREATE SCHEMA test_non_delimited");

        // Non-delimited identifiers should be case-insensitive (lowercased)
        assertUpdate("CREATE TABLE test_non_delimited.MixedCaseTable (id bigint, name varchar)");

        // All these should refer to the same table (lowercased)
        assertUpdate("INSERT INTO test_non_delimited.mixedcasetable VALUES (1, 'test1')", 1);
        assertUpdate("INSERT INTO test_non_delimited.MIXEDCASETABLE VALUES (2, 'test2')", 1);
        assertUpdate("INSERT INTO test_non_delimited.MixedCaseTable VALUES (3, 'test3')", 1);

        assertQuery("SELECT COUNT(*) FROM test_non_delimited.mixedcasetable", "VALUES 3");
    }

    @Test
    public void testMixedDelimitedAndNonDelimited()
    {
        assertUpdate("CREATE SCHEMA \"MixedSchema\"");
        assertUpdate("CREATE SCHEMA mixedschema"); // This creates a different schema (lowercased)

        // Create tables in both schemas
        assertUpdate("CREATE TABLE \"MixedSchema\".\"MixedTable\" (id bigint)");
        assertUpdate("CREATE TABLE mixedschema.mixedtable (id bigint)");

        // Insert different data
        assertUpdate("INSERT INTO \"MixedSchema\".\"MixedTable\" VALUES (1)", 1);
        assertUpdate("INSERT INTO mixedschema.mixedtable VALUES (2)", 1);

        // Verify they are different tables
        assertQuery("SELECT * FROM \"MixedSchema\".\"MixedTable\"", "VALUES 1");
        assertQuery("SELECT * FROM mixedschema.mixedtable", "VALUES 2");
    }

    @Test
    public void testCaseSensitiveColumnNames()
    {
        assertUpdate("CREATE SCHEMA test_columns");
        assertUpdate("CREATE TABLE test_columns.test_table (" +
                "\"MixedCaseColumn\" bigint, " +
                "\"UPPERCASECOLUMN\" varchar, " +
                "lowercasecolumn boolean)");

        // Insert with exact column names
        assertUpdate("INSERT INTO test_columns.test_table VALUES (1, 'test', true)", 1);

        // Query with exact case should work
        assertQuery("SELECT \"MixedCaseColumn\", \"UPPERCASECOLUMN\", lowercasecolumn FROM test_columns.test_table",
                "VALUES (1, 'test', true)");

        // Query with wrong case should fail for delimited columns
        assertQueryFails("SELECT \"mixedcasecolumn\" FROM test_columns.test_table", ".*Column.*not found.*");
        assertQueryFails("SELECT \"uppercasecolumn\" FROM test_columns.test_table", ".*Column.*not found.*");
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

        // Test case-sensitive canonicalization
        assertThat(catalog.canonicalize("MixedCase", true)).isEqualTo("MixedCase");
        assertThat(catalog.canonicalize("MixedCase", false)).isEqualTo("mixedcase");
    }
}
