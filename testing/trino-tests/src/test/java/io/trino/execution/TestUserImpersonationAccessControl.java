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
package io.trino.execution;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.client.ClientSession;
import io.trino.client.QueryError;
import io.trino.client.StatementClient;
import io.trino.plugin.base.security.FileBasedSystemAccessControl;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.SystemAccessControl;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import okhttp3.OkHttpClient;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestUserImpersonationAccessControl
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String securityConfigFile = getResource("access_control_rules.json").getPath();
        SystemAccessControl accessControl = new FileBasedSystemAccessControl.Factory().create(ImmutableMap.of(SECURITY_CONFIG_FILE, securityConfigFile));
        QueryRunner queryRunner = DistributedQueryRunner.builder(TEST_SESSION)
                .setNodeCount(1)
                .setSystemAccessControl(accessControl)
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

        return queryRunner;
    }

    // On the tpch catalog Alice has allow all permissions, Bob has no permissions, and Charlie has read only permissions.
    @Test
    public void testReadAccessControl()
    {
        QueryError aliceQueryError = trySelectQuery("alice");
        assertNull(aliceQueryError);

        QueryError bobQueryError = trySelectQuery("bob");
        assertNotNull(bobQueryError);
        assertEquals(bobQueryError.getErrorType(), "USER_ERROR");
        assertEquals(bobQueryError.getErrorName(), "PERMISSION_DENIED");

        QueryError charlieQueryError = trySelectQuery("charlie");
        assertNull(charlieQueryError);
    }

    @Nullable
    private QueryError trySelectQuery(String assumedUser)
    {
        OkHttpClient httpClient = new OkHttpClient();
        try {
            ClientSession clientSession = ClientSession.builder()
                    .server(getDistributedQueryRunner().getCoordinator().getBaseUrl())
                    .principal(Optional.of("user"))
                    .user(Optional.of(assumedUser))
                    .source("source")
                    .timeZone(ZoneId.of("America/Los_Angeles"))
                    .locale(Locale.ENGLISH)
                    .clientRequestTimeout(new Duration(2, MINUTES))
                    .compressionDisabled(true)
                    .build();

            // start query
            try (StatementClient client = newStatementClient(httpClient, clientSession, "SELECT * FROM tpch.tiny.nation")) {
                // wait for query to be fully scheduled
                while (client.isRunning() && !client.currentStatusInfo().getStats().isScheduled()) {
                    client.advance();
                }

                return client.currentStatusInfo().getError();
            }
        }
        finally {
            // close the client since, query is not managed by the client protocol
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        }
    }
}
