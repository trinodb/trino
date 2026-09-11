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
package io.trino.plugin.oracle;

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

import static io.trino.plugin.oracle.TestingOracleServer.TEST_PASS;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_SCHEMA;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_USER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * Regression test for the pooled Oracle connection factory: credentials must be resolved from the
 * query session's extraCredentials on every connection checkout, not once (with no session
 * available) when the connection pool is built. The connector-level connection-user/password are
 * deliberately left blank here so the query can only succeed if the session-scoped credentials are
 * actually used.
 */
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestOracleCredentialPassthrough
{
    private TestingOracleServer oracleServer;
    private QueryRunner queryRunner;

    @Test
    public void testCredentialPassthroughWithConnectionPool()
    {
        queryRunner.execute(getSession(), "CREATE TABLE test_create (a bigint)");
    }

    @BeforeAll
    public void createQueryRunner()
            throws Exception
    {
        oracleServer = new TestingOracleServer();
        queryRunner = OracleQueryRunner.builder(oracleServer)
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        // Intentionally blank: proves the query below cannot succeed via a static
                        // connector-level credential fallback, only via the session's extraCredentials.
                        .put("connection-user", "")
                        .put("connection-password", "")
                        .put("user-credential-name", "oracle.user")
                        .put("password-credential-name", "oracle.password")
                        .put("oracle.connection-pool.enabled", "true")
                        .buildOrThrow())
                .build();
    }

    @AfterAll
    public final void destroy()
    {
        queryRunner.close();
        queryRunner = null;
        oracleServer.close();
        oracleServer = null;
    }

    private static Session getSession()
    {
        Map<String, String> extraCredentials = ImmutableMap.of("oracle.user", TEST_USER, "oracle.password", TEST_PASS);
        return testSessionBuilder()
                .setCatalog("oracle")
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.forUser(TEST_USER)
                        .withExtraCredentials(extraCredentials)
                        .build())
                .build();
    }
}
