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
package io.prestosql.plugin.mysql;

import com.google.common.collect.ImmutableMap;
import io.airlift.testing.mysql.TestingMySqlServer;
import io.prestosql.Session;
import io.prestosql.spi.security.Identity;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestCredentialPassthrough
{
    private static final String TEST_SCHEMA = "test_database";
    private final TestingMySqlServer mysqlServer;
    private final QueryRunner mySqlQueryRunner;

    public TestCredentialPassthrough()
            throws Exception
    {
        mysqlServer = new TestingMySqlServer("testuser", "testpass", TEST_SCHEMA);
        mySqlQueryRunner = createQueryRunner(mysqlServer);
    }

    @Test
    public void testCredentialPassthrough()
            throws Exception
    {
        mySqlQueryRunner.execute(getSession(mysqlServer), "CREATE TABLE test_create (a bigint, b double, c varchar)");
    }

    public static QueryRunner createQueryRunner(TestingMySqlServer mySqlServer)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();
            queryRunner.installPlugin(new MySqlPlugin());
            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("connection-url", getConnectionUrl(mySqlServer))
                    .put("user-credential-name", "mysql.user")
                    .put("password-credential-name", "mysql.password")
                    .build();
            queryRunner.createCatalog("mysql", "mysql", properties);

            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner, mySqlServer);
            throw e;
        }
    }

    private static Session getSession(TestingMySqlServer mySqlServer)
    {
        Map<String, String> extraCredentials = ImmutableMap.of("mysql.user", mySqlServer.getUser(), "mysql.password", mySqlServer.getPassword());
        return testSessionBuilder()
                .setCatalog("mysql")
                .setSchema(TEST_SCHEMA)
                .setIdentity(new Identity(mySqlServer.getUser(), Optional.empty(), ImmutableMap.of(), extraCredentials))
                .build();
    }

    private static String getConnectionUrl(TestingMySqlServer mySqlServer)
    {
        return format("jdbc:mysql://localhost:%s?useSSL=false&allowPublicKeyRetrieval=true", mySqlServer.getPort());
    }
}
