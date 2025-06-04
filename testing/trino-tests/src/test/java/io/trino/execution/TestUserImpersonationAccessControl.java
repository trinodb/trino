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
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import jakarta.annotation.Nullable;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUserImpersonationAccessControl
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String securityConfigFile = new File(getResource("access_control_rules.json").toURI()).getPath();
        QueryRunner queryRunner = DistributedQueryRunner.builder(TEST_SESSION)
                .setWorkerCount(0)
                .setSystemAccessControl("file", Map.of(SECURITY_CONFIG_FILE, securityConfigFile))
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
        assertThat(aliceQueryError).isNull();

        QueryError bobQueryError = trySelectQuery("bob");
        assertThat(bobQueryError).isNotNull();
        assertThat(bobQueryError.getErrorType()).isEqualTo("USER_ERROR");
        assertThat(bobQueryError.getErrorName()).isEqualTo("PERMISSION_DENIED");

        QueryError charlieQueryError = trySelectQuery("charlie");
        assertThat(charlieQueryError).isNull();
    }

    @Nullable
    private QueryError trySelectQuery(String sessionUser)
    {
        OkHttpClient httpClient = new OkHttpClient();
        try {
            ClientSession clientSession = ClientSession.builder()
                    .server(getDistributedQueryRunner().getCoordinator().getBaseUrl())
                    .user(Optional.of("user"))
                    .sessionUser(Optional.of(sessionUser))
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
