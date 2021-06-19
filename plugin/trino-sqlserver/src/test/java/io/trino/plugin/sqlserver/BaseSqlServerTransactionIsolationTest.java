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
package io.trino.plugin.sqlserver;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.sqlserver.SqlServerQueryRunner.createSqlServerQueryRunner;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public abstract class BaseSqlServerTransactionIsolationTest
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingSqlServer sqlServer = closeAfterClass(new TestingSqlServer(this::configureDatabase));
        return createSqlServerQueryRunner(
                sqlServer,
                Map.of(),
                Map.of(),
                List.of(NATION));
    }

    protected abstract void configureDatabase(SqlExecutor executor, String databaseName);

    @Test
    public void testCreateReadTable()
    {
        assertUpdate("CREATE TABLE ctas_read AS SELECT * FROM tpch.tiny.nation", "SELECT count(*) FROM nation");
        assertQuery("SELECT AVG(LENGTH(name)) FROM ctas_read", "SELECT 7.08");
        assertQuery("SELECT SUM(LENGTH(name)) FROM ctas_read WHERE regionkey = 1", "SELECT 38");
        assertUpdate("DROP TABLE ctas_read");
    }

    @Test
    public void testDescribeShowTable()
    {
        assertUpdate("CREATE TABLE ctas_describe AS SELECT regionkey, nationkey, comment FROM tpch.tiny.nation", "SELECT count(*) FROM nation");

        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("regionkey", "bigint", "", "")
                .row("nationkey", "bigint", "", "")
                .row("comment", "varchar(152)", "", "")
                .build();

        MaterializedResult actualColumns = computeActual("DESCRIBE ctas_describe");
        assertThat(actualColumns).isEqualTo(expectedColumns);

        MaterializedResult expectedTables = MaterializedResult.resultBuilder(getSession(), VARCHAR)
                .row("ctas_describe")
                .build();

        MaterializedResult actualTables = computeActual("SHOW TABLES LIKE 'ctas_describe'");
        assertThat(actualTables).isEqualTo(expectedTables);

        assertUpdate("DROP TABLE ctas_describe");
    }

    @Test
    public void testCreateInsertReadTable()
    {
        assertUpdate("CREATE TABLE insert_table (col INTEGER)");
        assertUpdate("INSERT INTO insert_table (col) VALUES (1), (2), (3), (4)", 4);
        assertQuery("SELECT AVG(col) FROM insert_table", "SELECT 2.5");
        assertUpdate("DROP TABLE insert_table");
    }
}
