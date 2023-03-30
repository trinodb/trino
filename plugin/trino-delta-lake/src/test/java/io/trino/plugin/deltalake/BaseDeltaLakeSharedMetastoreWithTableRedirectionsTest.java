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

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public abstract class BaseDeltaLakeSharedMetastoreWithTableRedirectionsTest
        extends AbstractTestQueryFramework
{
    protected final String schema = "test_shared_schema_" + randomNameSuffix();

    protected abstract String getExpectedHiveCreateSchema(String catalogName);

    protected abstract String getExpectedDeltaLakeCreateSchema(String catalogName);

    @Test
    public void testReadInformationSchema()
    {
        assertThat(query("SELECT table_schema FROM hive_with_redirections.information_schema.tables WHERE table_name = 'hive_table' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SELECT table_schema FROM hive_with_redirections.information_schema.tables WHERE table_name = 'delta_table' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SELECT table_schema FROM delta_with_redirections.information_schema.tables WHERE table_name = 'hive_table' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SELECT table_schema FROM delta_with_redirections.information_schema.tables WHERE table_name = 'delta_table' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");

        assertQuery("SELECT table_name, column_name from hive_with_redirections.information_schema.columns WHERE table_schema = '" + schema + "'",
                "VALUES" +
                        "('hive_table', 'a_integer'), " +
                        "('delta_table', 'a_varchar')");
        assertQuery("SELECT table_name, column_name from delta_with_redirections.information_schema.columns WHERE table_schema = '" + schema + "'",
                "VALUES" +
                        "('hive_table', 'a_integer'), " +
                        "('delta_table', 'a_varchar')");
    }

    @Test
    public void testSelect()
    {
        assertThat(query("SELECT * FROM hive_with_redirections." + schema + ".hive_table")).matches("VALUES 1, 2, 3");
        assertThat(query("SELECT * FROM hive_with_redirections." + schema + ".delta_table")).matches("VALUES CAST('a' AS varchar), CAST('b' AS varchar), CAST('c' AS varchar)");
        assertThat(query("SELECT * FROM delta_with_redirections." + schema + ".hive_table")).matches("VALUES 1, 2, 3");
        assertThat(query("SELECT * FROM delta_with_redirections." + schema + ".delta_table")).matches("VALUES CAST('a' AS varchar), CAST('b' AS varchar), CAST('c' AS varchar)");
    }

    @Test
    public void testShowTables()
    {
        assertThat(query("SHOW TABLES FROM hive_with_redirections." + schema)).matches("VALUES CAST('hive_table' AS varchar), CAST('delta_table' AS varchar)");
        assertThat(query("SHOW TABLES FROM delta_with_redirections." + schema)).matches("VALUES CAST('hive_table' AS varchar), CAST('delta_table' AS varchar)");
    }

    @Test
    public void testShowSchemas()
    {
        assertThat(query("SHOW SCHEMAS FROM hive_with_redirections"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SHOW SCHEMAS FROM delta_with_redirections"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");

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
