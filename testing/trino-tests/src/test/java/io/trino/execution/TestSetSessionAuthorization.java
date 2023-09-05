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

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.client.ClientSession;
import io.trino.client.QueryData;
import io.trino.client.StatementClient;
import io.trino.spi.ErrorCode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import okhttp3.OkHttpClient;
import org.testng.annotations.Test;

import java.io.File;
import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;

public class TestSetSessionAuthorization
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(TEST_SESSION)
                .setSystemAccessControl("file", Map.of("security.config-file", new File(getResource("set_session_authorization_permissions.json").toURI()).getPath()))
                .build();
        return queryRunner;
    }

    @Test
    public void testSetSessionAuthorizationToSelf()
    {
        ClientSession clientSession = defaultClientSessionBuilder()
                .principal(Optional.of("user"))
                .user(Optional.of("user"))
                .build();
        assertEquals(submitQuery("SET SESSION AUTHORIZATION user", clientSession).getSetAuthorizationUser().get(),
                "user");
        assertEquals(submitQuery("SET SESSION AUTHORIZATION alice", clientSession).getSetAuthorizationUser().get(),
                "alice");
        assertEquals(submitQuery("SET SESSION AUTHORIZATION user", clientSession).getSetAuthorizationUser().get(),
                "user");
    }

    @Test
    public void testValidSetSessionAuthorization()
    {
        ClientSession clientSession = defaultClientSessionBuilder()
                .principal(Optional.of("user"))
                .user(Optional.of("user"))
                .build();
        assertEquals(submitQuery("SET SESSION AUTHORIZATION alice", clientSession).getSetAuthorizationUser().get(),
                "alice");

        clientSession = defaultClientSessionBuilder()
                .principal(Optional.of("user2"))
                .user(Optional.of("user2"))
                .build();
        assertEquals(submitQuery("SET SESSION AUTHORIZATION bob", clientSession).getSetAuthorizationUser().get(),
                "bob");
    }

    @Test
    public void testInvalidSetSessionAuthorization()
    {
        ClientSession clientSession = defaultClientSessionBuilder()
                .principal(Optional.of("user"))
                .user(Optional.of("user"))
                .build();
        assertError(submitQuery("SET SESSION AUTHORIZATION user2", clientSession),
                PERMISSION_DENIED.toErrorCode(), "Access Denied: User user cannot impersonate user user2");
        assertError(submitQuery("SET SESSION AUTHORIZATION bob", clientSession),
                PERMISSION_DENIED.toErrorCode(), "Access Denied: User user cannot impersonate user bob");
        assertEquals(submitQuery("SET SESSION AUTHORIZATION alice", clientSession).getSetAuthorizationUser().get(), "alice");
        assertError(submitQuery("SET SESSION AUTHORIZATION charlie", clientSession),
                PERMISSION_DENIED.toErrorCode(), "Access Denied: User user cannot impersonate user charlie");
        StatementClient client = submitQuery("START TRANSACTION", clientSession);
        clientSession = ClientSession.builder(clientSession).transactionId(client.getStartedTransactionId()).build();
        assertError(submitQuery("SET SESSION AUTHORIZATION alice", clientSession),
                GENERIC_USER_ERROR.toErrorCode(), "Can't set authorization user in the middle of a transaction");
    }

    // If user A can impersonate user B, and B can impersonate C - but A cannot go to C,
    // then we can only go from A->B or B->C, but not A->B->C
    @Test
    public void testInvalidTransitiveSetSessionAuthorization()
    {
        ClientSession clientSession = defaultClientSessionBuilder()
                .principal(Optional.of("user"))
                .user(Optional.of("user"))
                .build();
        assertEquals(submitQuery("SET SESSION AUTHORIZATION alice", clientSession).getSetAuthorizationUser().get(), "alice");

        clientSession = defaultClientSessionBuilder()
                .principal(Optional.of("alice"))
                .user(Optional.of("alice"))
                .build();
        assertEquals(submitQuery("SET SESSION AUTHORIZATION charlie", clientSession).getSetAuthorizationUser().get(), "charlie");

        clientSession = defaultClientSessionBuilder()
                .principal(Optional.of("user"))
                .user(Optional.of("user"))
                .build();
        assertEquals(submitQuery("SET SESSION AUTHORIZATION alice", clientSession).getSetAuthorizationUser().get(), "alice");
        assertError(submitQuery("SET SESSION AUTHORIZATION charlie", clientSession),
                PERMISSION_DENIED.toErrorCode(), "Access Denied: User user cannot impersonate user charlie");
    }

    @Test
    public void testValidSessionAuthorizationExecution()
    {
        ClientSession clientSession = defaultClientSessionBuilder()
                .principal(Optional.of("user"))
                .user(Optional.of("user"))
                .authorizationUser(Optional.of("alice"))
                .build();
        assertEquals(submitQuery("SELECT 1+1", clientSession).currentStatusInfo().getError(), null);

        clientSession = defaultClientSessionBuilder()
                .principal(Optional.of("user"))
                .user(Optional.of("user"))
                .authorizationUser(Optional.of("user"))
                .build();
        assertEquals(submitQuery("SELECT 1+1", clientSession).currentStatusInfo().getError(), null);

        clientSession = defaultClientSessionBuilder()
                .principal(Optional.of("user"))
                .authorizationUser(Optional.of("alice"))
                .build();
        assertEquals(submitQuery("SELECT 1+1", clientSession).currentStatusInfo().getError(), null);
    }

    @Test
    public void testInvalidSessionAuthorizationExecution()
    {
        ClientSession clientSession = defaultClientSessionBuilder()
                .principal(Optional.of("user"))
                .user(Optional.of("user"))
                .authorizationUser(Optional.of("user2"))
                .build();
        assertError(submitQuery("SELECT 1+1", clientSession),
                PERMISSION_DENIED.toErrorCode(), "Access Denied: User user cannot impersonate user user2");

        clientSession = defaultClientSessionBuilder()
                .principal(Optional.of("user"))
                .user(Optional.of("user"))
                .authorizationUser(Optional.of("user3"))
                .build();
        assertError(submitQuery("SELECT 1+1", clientSession),
                PERMISSION_DENIED.toErrorCode(), "Access Denied: User user cannot impersonate user user3");

        clientSession = defaultClientSessionBuilder()
                .principal(Optional.of("user"))
                .user(Optional.of("user"))
                .authorizationUser(Optional.of("charlie"))
                .build();
        assertError(submitQuery("SELECT 1+1", clientSession),
                PERMISSION_DENIED.toErrorCode(), "Access Denied: User user cannot impersonate user charlie");
    }

    @Test
    public void testSelectCurrentUser()
    {
        ClientSession clientSession = defaultClientSessionBuilder()
                .principal(Optional.of("user"))
                .user(Optional.of("user"))
                .authorizationUser(Optional.of("alice"))
                .build();

        ImmutableList.Builder<List<Object>> data = ImmutableList.builder();
        submitQuery("SELECT CURRENT_USER", clientSession, data);
        List<List<Object>> rows = data.build();
        assertEquals((String) rows.get(0).get(0), "alice");
    }

    @Test
    public void testResetSessionAuthorization()
    {
        ClientSession clientSession = defaultClientSessionBuilder()
                .principal(Optional.of("user"))
                .user(Optional.of("user"))
                .build();
        assertResetAuthorizationUser(submitQuery("RESET SESSION AUTHORIZATION", clientSession));
        assertEquals(submitQuery("SET SESSION AUTHORIZATION alice", clientSession).getSetAuthorizationUser().get(), "alice");
        assertResetAuthorizationUser(submitQuery("RESET SESSION AUTHORIZATION", clientSession));
        StatementClient client = submitQuery("START TRANSACTION", clientSession);
        clientSession = ClientSession.builder(clientSession).transactionId(client.getStartedTransactionId()).build();
        assertError(submitQuery("RESET SESSION AUTHORIZATION", clientSession),
                GENERIC_USER_ERROR.toErrorCode(), "Can't reset authorization user in the middle of a transaction");
    }

    private void assertError(StatementClient client, ErrorCode errorCode, String errorMessage)
    {
        assertEquals(client.getSetAuthorizationUser(), Optional.empty());
        assertEquals(client.currentStatusInfo().getError().getErrorName(), errorCode.getName());
        assertEquals(client.currentStatusInfo().getError().getMessage(), errorMessage);
    }

    private void assertResetAuthorizationUser(StatementClient client)
    {
        assertEquals(client.isResetAuthorizationUser(), true);
        assertEquals(client.getSetAuthorizationUser().isEmpty(), true);
    }

    private ClientSession.Builder defaultClientSessionBuilder()
    {
        return ClientSession.builder()
                .server(getDistributedQueryRunner().getCoordinator().getBaseUrl())
                .source("source")
                .timeZone(ZoneId.of("America/Los_Angeles"))
                .locale(Locale.ENGLISH)
                .clientRequestTimeout(new Duration(2, MINUTES));
    }

    private StatementClient submitQuery(String query, ClientSession clientSession)
    {
        OkHttpClient httpClient = new OkHttpClient();
        try {
            try (StatementClient client = newStatementClient(httpClient, clientSession, query)) {
                // wait for query to be fully scheduled
                while (client.isRunning() && !client.currentStatusInfo().getStats().isScheduled()) {
                    client.advance();
                }
                return client;
            }
        }
        finally {
            // close the client since, query is not managed by the client protocol
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        }
    }

    private StatementClient submitQuery(String query, ClientSession clientSession, ImmutableList.Builder<List<Object>> data)
    {
        OkHttpClient httpClient = new OkHttpClient();
        try {
            try (StatementClient client = newStatementClient(httpClient, clientSession, query)) {
                while (client.isRunning() && !Thread.currentThread().isInterrupted()) {
                    QueryData results = client.currentData();
                    if (results.getData() != null) {
                        data.addAll(results.getData());
                    }
                    client.advance();
                }
                // wait for query to be fully scheduled
                while (client.isRunning() && !client.currentStatusInfo().getStats().isScheduled()) {
                    client.advance();
                }
                return client;
            }
        }
        finally {
            // close the client since, query is not managed by the client protocol
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        }
    }
}
