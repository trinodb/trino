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
package io.trino.plugin.databend;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;

public class TestDatabendTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingDatabendServer databendServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        databendServer = closeAfterClass(new TestingDatabendServer());
        return DatabendQueryRunner.createDatabendQueryRunner(databendServer, emptyMap(), emptyMap());
    }

    protected SqlExecutor onRemoteDatabase()
    {
        return databendServer::execute;
    }

    @Test
    public void testBoolean()
    {
        // Databend supports BOOLEAN type
        SqlExecutor jdbcSqlExecutor = onRemoteDatabase();
        Session session = getSession();

        jdbcSqlExecutor.execute("CREATE TABLE test_boolean_type (boolean_column BOOLEAN)");
        assertTypeMapping(session, jdbcSqlExecutor, "BOOLEAN", "true", "BOOLEAN");
        assertTypeMapping(session, jdbcSqlExecutor, "BOOLEAN", "false", "BOOLEAN");
        jdbcSqlExecutor.execute("DROP TABLE test_boolean_type");
    }

    @Test
    public void testTinyint()
    {
        // Databend supports TINYINT
        SqlExecutor jdbcSqlExecutor = onRemoteDatabase();
        Session session = getSession();

        jdbcSqlExecutor.execute("CREATE TABLE test_tinyint_type (tinyint_column TINYINT)");
        assertTypeMapping(session, jdbcSqlExecutor, "TINYINT", "127", "TINYINT");
        assertTypeMapping(session, jdbcSqlExecutor, "TINYINT", "-128", "TINYINT");
        jdbcSqlExecutor.execute("DROP TABLE test_tinyint_type");
    }

    @Test
    public void testSmallint()
    {
        // Databend supports SMALLINT
        SqlExecutor jdbcSqlExecutor = onRemoteDatabase();
        Session session = getSession();

        jdbcSqlExecutor.execute("CREATE TABLE test_smallint_type (smallint_column SMALLINT)");
        assertTypeMapping(session, jdbcSqlExecutor, "SMALLINT", "32767", "SMALLINT");
        assertTypeMapping(session, jdbcSqlExecutor, "SMALLINT", "-32768", "SMALLINT");
        jdbcSqlExecutor.execute("DROP TABLE test_smallint_type");
    }

    @Test
    public void testInteger()
    {
        // Databend supports INTEGER
        SqlExecutor jdbcSqlExecutor = onRemoteDatabase();
        Session session = getSession();

        jdbcSqlExecutor.execute("CREATE TABLE test_integer_type (integer_column INTEGER)");
        assertTypeMapping(session, jdbcSqlExecutor, "INTEGER", "2147483647", "INTEGER");
        assertTypeMapping(session, jdbcSqlExecutor, "INTEGER", "-2147483648", "INTEGER");
        jdbcSqlExecutor.execute("DROP TABLE test_integer_type");
    }

    @Test
    public void testBigint()
    {
        // Databend supports BIGINT
        SqlExecutor jdbcSqlExecutor = onRemoteDatabase();
        Session session = getSession();

        jdbcSqlExecutor.execute("CREATE TABLE test_bigint_type (bigint_column BIGINT)");
        assertTypeMapping(session, jdbcSqlExecutor, "BIGINT", "9223372036854775807", "BIGINT");
        assertTypeMapping(session, jdbcSqlExecutor, "BIGINT", "-9223372036854775808", "BIGINT");
        jdbcSqlExecutor.execute("DROP TABLE test_bigint_type");
    }

    private void assertTypeMapping(Session session, SqlExecutor remoteExecutor, String remoteType, String remoteValue, String expectedTrinoType)
    {
        String baseName = remoteType.toLowerCase(Locale.ENGLISH);
        String tableName = format("test_%s_type", baseName);
        String columnName = format("%s_column", baseName);

        remoteExecutor.execute(format("INSERT INTO %s (%s) VALUES (%s)", tableName, columnName, remoteValue));

        assertQuery(
                session,
                format("SELECT UPPER(typeof(%s)) FROM %s LIMIT 1", columnName, tableName),
                format("VALUES CAST('%s' AS varchar)", expectedTrinoType));

        assertQuery(
                session,
                format("SELECT %s FROM %s", columnName, tableName),
                format("VALUES (%s)", remoteValue));

        remoteExecutor.execute(format("DELETE FROM %s", tableName));
    }
}
