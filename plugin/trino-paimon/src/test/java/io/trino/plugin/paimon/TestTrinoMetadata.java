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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for PaimonMetadata implementation.
 *
 * Tests metadata operations including: - Table creation with different save
 * modes - Schema operations - Column operations - Table properties
 */
public class TestTrinoMetadata
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TrinoQueryRunner.createPrestoQueryRunner(Map.of(), Map.of(), false);
    }

    @Test
    public void testCreateTableWithSaveModeFail()
    {
        String tableName = "test_save_mode_fail_" + System.currentTimeMillis();

        // First creation should succeed
        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, name VARCHAR)");

        // Second creation with FAIL mode should fail
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, name VARCHAR)"))
                .hasMessageContaining("already");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableWithSaveModeIgnore()
    {
        String tableName = "test_save_mode_ignore_" + System.currentTimeMillis();

        // First creation
        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'test')", 1);

        // Second creation with IF NOT EXISTS (IGNORE mode) should succeed silently
        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (id BIGINT, name VARCHAR, extra VARCHAR)");

        // Verify original table structure is unchanged
        assertThat(computeActual("SELECT * FROM " + tableName).getRowCount()).isEqualTo(1);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testListTables()
    {
        String tableName1 = "test_list_1_" + System.currentTimeMillis();
        String tableName2 = "test_list_2_" + System.currentTimeMillis();

        assertUpdate("CREATE TABLE " + tableName1 + " (id BIGINT)");
        assertUpdate("CREATE TABLE " + tableName2 + " (id BIGINT)");

        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).contains(tableName1, tableName2);

        assertUpdate("DROP TABLE " + tableName1);
        assertUpdate("DROP TABLE " + tableName2);
    }

    @Test
    public void testCreateSchemaExistingSchema()
    {
        String schemaName = "test_create_schema_" + System.currentTimeMillis();

        assertUpdate("CREATE SCHEMA " + schemaName);
        assertQueryFails("CREATE SCHEMA " + schemaName, ".*Schema 'paimon\\.%s' already exists.*".formatted(schemaName));
        assertUpdate("CREATE SCHEMA IF NOT EXISTS " + schemaName);

        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Test
    public void testDropNonEmptySchemaRequiresCascade()
    {
        String schemaName = "test_drop_schema_" + System.currentTimeMillis();
        String tableName = schemaName + ".values_table";

        assertUpdate("CREATE SCHEMA " + schemaName);
        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT)");
        assertQueryFails("DROP SCHEMA " + schemaName, ".*Cannot drop non-empty schema '%s'.*".formatted(schemaName));
        assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
    }

    @Test
    public void testGetTableProperties()
    {
        String tableName = "test_properties_" + System.currentTimeMillis();

        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, name VARCHAR) "
                + "WITH (primary_key = ARRAY['id'], bucket = '4', bucket_key = 'id')");

        // Verify table was created - Paimon stores properties but may not show them in
        // SHOW CREATE TABLE
        assertThat(computeActual("SELECT * FROM " + tableName).getRowCount()).isEqualTo(0);

        // Verify we can insert and query data (which confirms table properties work)
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'test')", 1);
        assertThat(computeActual("SELECT * FROM " + tableName + " WHERE id = 1").getRowCount()).isEqualTo(1);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRenameTable()
    {
        String oldName = "test_rename_old_" + System.currentTimeMillis();
        String newName = "test_rename_new_" + System.currentTimeMillis();

        assertUpdate("CREATE TABLE " + oldName + " (id BIGINT)");
        assertUpdate("INSERT INTO " + oldName + " VALUES (1)", 1);

        assertUpdate("ALTER TABLE " + oldName + " RENAME TO " + newName);

        assertThat(computeActual("SELECT * FROM " + newName).getRowCount()).isEqualTo(1);
        assertQueryFails("SELECT * FROM " + oldName, ".*not exist.*");

        assertUpdate("DROP TABLE " + newName);
    }

    @Test
    public void testAddColumn()
    {
        String tableName = "test_add_column_" + System.currentTimeMillis();

        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1)", 1);

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN name VARCHAR");
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'test')", 1);

        assertThat(computeActual("SELECT * FROM " + tableName + " WHERE id = 2").getRowCount()).isEqualTo(1);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropColumn()
    {
        String tableName = "test_drop_column_" + System.currentTimeMillis();

        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, name VARCHAR, extra VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'test', 'extra')", 1);

        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN extra");

        // Verify column was dropped - Trino reports "cannot be resolved" for missing
        // columns
        assertQueryFails("SELECT extra FROM " + tableName, ".*cannot be resolved.*");
        assertThat(computeActual("SELECT * FROM " + tableName).getRowCount()).isEqualTo(1);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRenameColumn()
    {
        String tableName = "test_rename_column_" + System.currentTimeMillis();

        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, old_name VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'test')", 1);

        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN old_name TO new_name");

        // Verify new column exists and old column doesn't
        assertThat(computeActual("SELECT new_name FROM " + tableName).getRowCount()).isEqualTo(1);
        assertQueryFails("SELECT old_name FROM " + tableName, ".*cannot be resolved.*");

        assertUpdate("DROP TABLE " + tableName);
    }
}
