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
import io.prestosql.Session;
import io.prestosql.spi.security.Identity;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestCredentialPassthrough
{
    private TestingMySqlServer mysqlServer;
    private QueryRunner queryRunner;

    @Test
    public void testCredentialPassthrough()
    {
        queryRunner.execute(getSession(mysqlServer), "CREATE TABLE test_create (a bigint, b double, c varchar)");
    }

    @BeforeClass
    public void createQueryRunner()
            throws Exception
    {
        mysqlServer = new TestingMySqlServer();
        try {
            queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();
            queryRunner.installPlugin(new MySqlPlugin());
            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("connection-url", mysqlServer.getJdbcUrl())
                    .put("user-credential-name", "mysql.user")
                    .put("password-credential-name", "mysql.password")
                    .build();
            queryRunner.createCatalog("mysql", "mysql", properties);
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner, mysqlServer::close);
            throw e;
        }
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        queryRunner.close();
        queryRunner = null;
        mysqlServer.close();
        mysqlServer = null;
    }

    private static Session getSession(TestingMySqlServer mySqlServer)
    {
        Map<String, String> extraCredentials = ImmutableMap.of("mysql.user", mySqlServer.getUsername(), "mysql.password", mySqlServer.getPassword());
        return testSessionBuilder()
                .setCatalog("mysql")
                .setSchema(mySqlServer.getDatabaseName())
                .setIdentity(Identity.forUser(mySqlServer.getUsername())
                        .withExtraCredentials(extraCredentials)
                        .build())
                .build();
    }
}
