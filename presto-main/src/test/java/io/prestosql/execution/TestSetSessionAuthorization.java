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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.prestosql.client.ClientSession;
import io.prestosql.client.QueryError;
import io.prestosql.client.StatementClient;
import io.prestosql.plugin.base.security.FileBasedSystemAccessControl;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.testing.assertions.Assert;
import okhttp3.OkHttpClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.util.Locale;
import java.util.Optional;

import static io.prestosql.client.StatementClientFactory.newStatementClient;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestSetSessionAuthorization
{
    private TestingPrestoServer server;
    private OkHttpClient httpClient;

    @BeforeClass
    private void setUp()
    {
        server = TestingPrestoServer.builder()
                .setSystemAccessControls(ImmutableList.of(newFileBasedSystemAccessControl("set_session_authorization_permissions.json")))
                .build();
        httpClient = new OkHttpClient();
    }

    @AfterClass(alwaysRun = true)
    private void tearDown()
            throws Exception
    {
        server.close();
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    private SystemAccessControl newFileBasedSystemAccessControl(String resourceName)
    {
        return newFileBasedSystemAccessControl(ImmutableMap.of("security.config-file", getResourcePath(resourceName)));
    }

    private SystemAccessControl newFileBasedSystemAccessControl(ImmutableMap<String, String> config)
    {
        return new FileBasedSystemAccessControl.Factory().create(config);
    }

    private String getResourcePath(String resourceName)
    {
        return this.getClass().getClassLoader().getResource(resourceName).getPath();
    }

    @Test
    public void testSetSessionAuthorizationToSelf()
    {
        StatementClient client;
        client = submitQuery("SET SESSION AUTHORIZATION user", null, "user", null);
        Assert.assertEquals(client.getSetAuthorizationUser().get(), "user");

        client = submitQuery("SET SESSION AUTHORIZATION alice", client.getSetAuthorizationUser().orElse(null), "user", null);
        Assert.assertEquals(client.getSetAuthorizationUser().get(), "alice");

        client = submitQuery("SET SESSION AUTHORIZATION user", client.getSetAuthorizationUser().orElse(null), "user", null);
        Assert.assertEquals(client.getSetAuthorizationUser().get(), "user");
    }

    @Test
    public void testValidSetSessionAuthorization()
    {
        StatementClient client;
        client = submitQuery("SET SESSION AUTHORIZATION bob", null, "user2", null);
        Assert.assertEquals(client.getSetAuthorizationUser().get(), "bob");
        client = submitQuery("SET SESSION AUTHORIZATION alice", null, "user", null);
        Assert.assertEquals(client.getSetAuthorizationUser().get(), "alice");
    }

    @Test
    public void testInvalidSetSessionAuthorization()
    {
        StatementClient client = submitQuery("SET SESSION AUTHORIZATION user2", null, "user", null);
        QueryError error = client.currentStatusInfo().getError();
        Assert.assertEquals(client.getSetAuthorizationUser(), Optional.empty());
        assertError(error, PERMISSION_DENIED.toErrorCode(), "Access Denied: Principal user cannot become user user2");

        client = submitQuery("SET SESSION AUTHORIZATION bob", client.getSetAuthorizationUser().orElse(null), "user", null);
        error = client.currentStatusInfo().getError();
        Assert.assertEquals(client.getSetAuthorizationUser(), Optional.empty());
        assertError(error, PERMISSION_DENIED.toErrorCode(), "Access Denied: Principal user cannot become user bob");

        client = submitQuery("SET SESSION AUTHORIZATION alice", client.getSetAuthorizationUser().orElse(null), "user", null);
        Assert.assertEquals(client.getSetAuthorizationUser().get(), "alice");
        client = submitQuery("SET SESSION AUTHORIZATION user2", client.getSetAuthorizationUser().get(), "user", null);
        error = client.currentStatusInfo().getError();
        Assert.assertEquals(client.getSetAuthorizationUser(), Optional.empty());
        assertError(error, PERMISSION_DENIED.toErrorCode(), "Access Denied: Principal user cannot become user user2");

        client = submitQuery("SET SESSION AUTHORIZATION charlie", null, "user", null);
        error = client.currentStatusInfo().getError();
        Assert.assertEquals(client.getSetAuthorizationUser(), Optional.empty());
        assertError(error, PERMISSION_DENIED.toErrorCode(), "Access Denied: User user cannot impersonate user charlie");

        client = submitQuery("START TRANSACTION", null, "user", null);
        String transactionId = client.getStartedTransactionId();
        client = submitQuery("SET SESSION AUTHORIZATION alice", null, "user", transactionId);
        error = client.currentStatusInfo().getError();
        assertError(error, GENERIC_USER_ERROR.toErrorCode(), "Can't set authorization user in the middle of a transaction");
    }

    @Test
    public void testValidSessionAuthorizationExecution()
    {
        StatementClient client = submitQuery("SELECT 1", "alice", "user", null);
        QueryError error = client.currentStatusInfo().getError();
        Assert.assertEquals(error, null);

        client = submitQuery("SELECT 1", "user", "user", null);
        error = client.currentStatusInfo().getError();
        Assert.assertEquals(error, null);
    }

    @Test
    public void testInvalidSessionAuthorizationExecution()
    {
        StatementClient client = submitQuery("SELECT 1", "user2", "user", null);
        QueryError error = client.currentStatusInfo().getError();
        assertError(error, PERMISSION_DENIED.toErrorCode(), "Access Denied: Principal user cannot become user user2");

        client = submitQuery("SELECT 1", "user3", "user", null);
        error = client.currentStatusInfo().getError();
        assertError(error, PERMISSION_DENIED.toErrorCode(), "Access Denied: Principal user cannot become user user3");

        client = submitQuery("SELECT 1", "charlie", "user", null);
        error = client.currentStatusInfo().getError();
        assertError(error, PERMISSION_DENIED.toErrorCode(), "Access Denied: User user cannot impersonate user charlie");
    }

    private void assertError(QueryError error, ErrorCode errorCode, String errorMessage)
    {
        Assert.assertEquals(error.getErrorName(), errorCode.getName());
        Assert.assertEquals(error.getMessage(), errorMessage);
    }

    @Test
    public void testResetSessionAuthorization()
    {
        StatementClient client;
        client = submitQuery("RESET SESSION AUTHORIZATION", null, "user", null);
        Assert.assertEquals(client.isResetAuthorizationUser(), true);
        Assert.assertEquals(client.getSetAuthorizationUser().isEmpty(), true);

        client = submitQuery("SET SESSION AUTHORIZATION alice", null, "user", null);
        Assert.assertEquals(client.getSetAuthorizationUser().get(), "alice");
        client = submitQuery("RESET SESSION AUTHORIZATION", client.getSetAuthorizationUser().orElse(null), "user", null);
        Assert.assertEquals(client.isResetAuthorizationUser(), true);
        Assert.assertEquals(client.getSetAuthorizationUser().isEmpty(), true);

        client = submitQuery("START TRANSACTION", null, "user", null);
        String transactionId = client.getStartedTransactionId();
        client = submitQuery("RESET SESSION AUTHORIZATION", null, "user", transactionId);
        QueryError error = client.currentStatusInfo().getError();
        Assert.assertEquals(error.getMessage(), "Can't reset authorization user in the middle of a transaction");
    }

    private StatementClient submitQuery(String query, String authorizationUser, String user, String transactionId)
    {
        ClientSession.Builder clientSessionBuilder = ClientSession.builder()
                .withServer(server.getBaseUrl())
                .withUser(user)
                .withSource("source")
                .withTimeZone(ZoneId.of("America/Los_Angeles"))
                .withLocale(Locale.ENGLISH)
                .withClientRequestTimeout(new Duration(2, MINUTES))
                .withAuthorizationUser(Optional.ofNullable(authorizationUser));

        if (transactionId != null) {
            clientSessionBuilder.withTransactionId(transactionId);
        }

        // start query
        StatementClient client = newStatementClient(httpClient, clientSessionBuilder.build(), query);

        // wait for query to be fully scheduled
        while (client.isRunning() && !client.currentStatusInfo().getStats().isScheduled()) {
            client.advance();
        }
        return client;
    }
}
