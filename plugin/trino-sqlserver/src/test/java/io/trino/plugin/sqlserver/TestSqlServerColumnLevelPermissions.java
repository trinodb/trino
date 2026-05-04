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

import com.google.common.collect.ImmutableList;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;

final class TestSqlServerColumnLevelPermissions
        extends AbstractTestQueryFramework
{
    private TestTable testTable;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingSqlServer sqlServer = closeAfterClass(new TestingSqlServer());

        String limitedUser = "limited_user" + randomNameSuffix();
        String password = "A_sTr0nG_p4s$word";
        sqlServer.execute("CREATE LOGIN %s WITH PASSWORD = '%s'".formatted(limitedUser, password));
        sqlServer.execute("CREATE USER %s FOR LOGIN %s".formatted(limitedUser, limitedUser));

        testTable = closeAfterClass(
                new TestTable(sqlServer::execute, "test_column_permissions",
                        "(id INT, allowed_column VARCHAR(50), denied_column VARCHAR(50))",
                        ImmutableList.of("1, 'foo', 'secret'")));
        sqlServer.execute("GRANT SELECT ON dbo.%s TO %s".formatted(testTable.getName(), limitedUser));
        sqlServer.execute("DENY SELECT ON dbo.%s (denied_column) TO %s".formatted(testTable.getName(), limitedUser));

        return SqlServerQueryRunner.builder(sqlServer)
                .addConnectorProperties(Map.of(
                        "connection-user", limitedUser,
                        "connection-password", password))
                .build();
    }

    @Test
    void testSelectPermittedColumn()
    {
        assertQuery("SELECT allowed_column FROM " + testTable.getName(), "VALUES ('foo')");
    }

    @Test
    void testSelectStarFails()
    {
        assertQueryFails("SELECT * FROM " + testTable.getName(), ".*SELECT permission was denied.*");
    }

    @Test
    void testSelectDeniedColumnFails()
    {
        assertQueryFails("SELECT denied_column FROM " + testTable.getName(), ".*SELECT permission was denied.*");
    }

    @Test
    void testDescribe()
    {
        assertQuery("DESCRIBE " + testTable.getName(),
                """
                VALUES ('id', 'integer', '', ''),
                       ('allowed_column', 'varchar(50)', '', ''),
                       ('denied_column', 'varchar(50)', '', '')
                """);
    }
}
