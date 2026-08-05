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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.client.ClientSession;
import io.trino.client.StatementClient;
import io.trino.client.StatementClientFactory;
import io.trino.client.uri.TrinoUri;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.time.ZoneId;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.trino.client.uri.HttpClientFactory.toHttpClientBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestServerAbandonedQueries
{
    private TestingTrinoServer server;

    @BeforeAll
    public void setup()
    {
        server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.of("query.client.timeout", "1s"))
                .build();

        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
    }

    @Test
    public void testAbandonedQueries()
            throws InterruptedException
    {
        TrinoUri trinoUri = TrinoUri.builder()
                .setUri(server.getBaseUrl())
                .build();

        OkHttpClient httpClient = toHttpClientBuilder(trinoUri, "Trino Test").build();
        ClientSession session = ClientSession.builder()
                .server(server.getBaseUrl())
                .source("test")
                .timeZone(ZoneId.of("UTC"))
                .user(Optional.of("user"))
                .heartbeatInterval(new Duration(1, TimeUnit.SECONDS))
                .build();

        try (StatementClient client = StatementClientFactory.newStatementClient(httpClient, session, "SELECT * FROM tpch.sf1.nation")) {
            client.advance();
            // heartbeat is expected every 1 second, the check runs every second, plus one second padding
            Thread.sleep(3000);
            client.advance();
            assertThat(client.currentStatusInfo().getError().getMessage())
                    .contains("was abandoned by the client, as it may have exited or stopped checking for query results");
        }

        // Test query is not abandoned when client iterates over rows, even when one batch of results takes more than client timeout
        try (StatementClient client = StatementClientFactory.newStatementClient(httpClient, session, "SELECT * FROM tpch.sf1.nation")) {
            while (client.advance()) {
                client.currentRows().forEach(_ -> {
                    try {
                        Thread.sleep(100); // 25 rows * 100 ms = 2500 ms > client timeout
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
                assertThat(client.currentStatusInfo().getError()).isNull();
            }
        }
    }
}
