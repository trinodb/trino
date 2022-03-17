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

import io.trino.testing.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public abstract class BaseDeltaLakeSharedMetastoreTest
        extends AbstractTestQueryFramework
{
    protected final String schema = "test_shared_schema_" + randomTableSuffix();

    protected abstract String getExpectedHiveCreateSchema(String catalogName);

    protected abstract String getExpectedDeltaLakeCreateSchema(String catalogName);

    @Test
    public void testReadInformationSchema()
    {
        assertThat(query("SELECT table_schema FROM hive.information_schema.tables WHERE table_name = 'region' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SELECT table_schema FROM delta.information_schema.tables WHERE table_name = 'nation' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SELECT table_schema FROM hive_with_redirections.information_schema.tables WHERE table_name = 'region' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SELECT table_schema FROM hive_with_redirections.information_schema.tables WHERE table_name = 'nation' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SELECT table_schema FROM delta_with_redirections.information_schema.tables WHERE table_name = 'region' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");

        assertQuery("SELECT table_name, column_name from hive.information_schema.columns WHERE table_schema = '" + schema + "'",
                "VALUES ('region', 'regionkey'), ('region', 'name'), ('region', 'comment')");
        assertQuery("SELECT table_name, column_name from delta.information_schema.columns WHERE table_schema = '" + schema + "'",
                "VALUES ('nation', 'nationkey'), ('nation', 'name'), ('nation', 'regionkey'), ('nation', 'comment')");
        assertQuery("SELECT table_name, column_name from hive_with_redirections.information_schema.columns WHERE table_schema = '" + schema + "'",
                "VALUES" +
                        "('region', 'regionkey'), ('region', 'name'), ('region', 'comment'), " +
                        "('nation', 'nationkey'), ('nation', 'name'), ('nation', 'regionkey'), ('nation', 'comment')");
        assertQuery("SELECT table_name, column_name from delta_with_redirections.information_schema.columns WHERE table_schema = '" + schema + "'",
                "VALUES" +
                        "('region', 'regionkey'), ('region', 'name'), ('region', 'comment'), " +
                        "('nation', 'nationkey'), ('nation', 'name'), ('nation', 'regionkey'), ('nation', 'comment')");
    }

    @Test
    public void testSelect()
    {
        assertQuery("SELECT * FROM delta." + schema + ".nation", "SELECT * FROM nation");
        assertQuery("SELECT * FROM hive." + schema + ".region", "SELECT * FROM region");
        assertQuery("SELECT * FROM hive_with_redirections." + schema + ".nation", "SELECT * FROM nation");
        assertQuery("SELECT * FROM hive_with_redirections." + schema + ".region", "SELECT * FROM region");
        assertQuery("SELECT * FROM delta_with_redirections." + schema + ".nation", "SELECT * FROM nation");
        assertQuery("SELECT * FROM delta_with_redirections." + schema + ".region", "SELECT * FROM region");

        assertThatThrownBy(() -> query("SELECT * FROM delta." + schema + ".region"))
                .hasMessageContaining("not a Delta Lake table");
        assertThatThrownBy(() -> query("SELECT * FROM hive." + schema + ".nation"))
                .hasMessageContaining("Cannot query Delta Lake table");
    }

    @Test
    public void testShowTables()
    {
        assertQuery("SHOW TABLES FROM delta." + schema, "VALUES 'region', 'nation'");
        assertQuery("SHOW TABLES FROM hive." + schema, "VALUES 'region', 'nation'");
        assertQuery("SHOW TABLES FROM hive_with_redirections." + schema, "VALUES 'region', 'nation'");
        assertQuery("SHOW TABLES FROM delta_with_redirections." + schema, "VALUES 'region', 'nation'");

        assertThatThrownBy(() -> query("SHOW CREATE TABLE delta." + schema + ".region"))
                .hasMessageContaining("not a Delta Lake table");
        assertThatThrownBy(() -> query("SHOW CREATE TABLE hive." + schema + ".nation"))
                .hasMessageContaining("Cannot query Delta Lake table");

        assertThatThrownBy(() -> query("DESCRIBE delta." + schema + ".region"))
                .hasMessageContaining("not a Delta Lake table");
        assertThatThrownBy(() -> query("DESCRIBE hive." + schema + ".nation"))
                .hasMessageContaining("Cannot query Delta Lake table");
    }

    @Test
    public void testShowSchemas()
    {
        assertThat(query("SHOW SCHEMAS FROM hive"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SHOW SCHEMAS FROM delta"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SHOW SCHEMAS FROM hive_with_redirections"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");

        String showCreateHiveSchema = (String) computeActual("SHOW CREATE SCHEMA hive." + schema).getOnlyValue();
        assertEquals(
                showCreateHiveSchema,
                getExpectedHiveCreateSchema("hive"));
        String showCreateDeltaLakeSchema = (String) computeActual("SHOW CREATE SCHEMA delta." + schema).getOnlyValue();
        assertEquals(
                showCreateDeltaLakeSchema,
                getExpectedDeltaLakeCreateSchema("delta"));
        String showCreateHiveWithRedirectionsSchema = (String) computeActual("SHOW CREATE SCHEMA hive_with_redirections." + schema).getOnlyValue();
        assertEquals(
                showCreateHiveWithRedirectionsSchema,
                getExpectedHiveCreateSchema("hive_with_redirections"));
        String showCreateDeltaLakeWithRedirectionsSchema = (String) computeActual("SHOW CREATE SCHEMA delta_with_redirections." + schema).getOnlyValue();
        assertEquals(
                showCreateDeltaLakeWithRedirectionsSchema,
                getExpectedDeltaLakeCreateSchema("delta_with_redirections"));
    }
}
