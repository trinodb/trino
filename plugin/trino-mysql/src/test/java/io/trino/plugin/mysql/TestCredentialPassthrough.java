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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestCredentialPassthrough
{
    private TestingMySqlServer mySqlServer;
    private QueryRunner queryRunner;

    @Test
    public void testCredentialPassthrough()
    {
        queryRunner.execute(getSession(mySqlServer), "CREATE TABLE test_create (a bigint, b double, c varchar)");
    }

    @BeforeAll
    public void createQueryRunner()
            throws Exception
    {
        mySqlServer = new TestingMySqlServer();
        queryRunner = MySqlQueryRunner.builder(mySqlServer)
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("connection-url", mySqlServer.getJdbcUrl())
                        .put("user-credential-name", "mysql.user")
                        .put("password-credential-name", "mysql.password")
                        .buildOrThrow())
                .build();
    }

    @AfterAll
    public final void destroy()
    {
        queryRunner.close();
        queryRunner = null;
        mySqlServer.close();
        mySqlServer = null;
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
