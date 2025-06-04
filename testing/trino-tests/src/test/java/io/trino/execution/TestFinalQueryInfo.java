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

import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.client.ClientSession;
import io.trino.client.StatementClient;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.QueryId;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.ZoneId;
import java.util.Locale;
import java.util.Optional;

import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFinalQueryInfo
{
    @Test
    @Timeout(240)
    public void testFinalQueryInfoSetOnAbort()
            throws Exception
    {
        try (QueryRunner queryRunner = createQueryRunner(TEST_SESSION)) {
            QueryId queryId = startQuery("SELECT COUNT(*) FROM tpch.sf1000.lineitem", queryRunner);
            SettableFuture<QueryInfo> finalQueryInfoFuture = SettableFuture.create();
            queryRunner.getCoordinator().addFinalQueryInfoListener(queryId, finalQueryInfoFuture::set);

            // wait 1s then kill query
            Thread.sleep(1_000);
            queryRunner.getCoordinator().getQueryManager().cancelQuery(queryId);

            // wait for final query info
            QueryInfo finalQueryInfo = tryGetFutureValue(finalQueryInfoFuture, 10, SECONDS)
                    .orElseThrow(() -> new AssertionError("Final query info never set"));
            assertThat(finalQueryInfo.isFinalQueryInfo()).isTrue();
        }
    }

    private static QueryId startQuery(String sql, QueryRunner queryRunner)
    {
        OkHttpClient httpClient = new OkHttpClient();
        try {
            ClientSession clientSession = ClientSession.builder()
                    .server(queryRunner.getCoordinator().getBaseUrl())
                    .user(Optional.of("user"))
                    .source("source")
                    .timeZone(ZoneId.of("America/Los_Angeles"))
                    .locale(Locale.ENGLISH)
                    .transactionId(null)
                    .clientRequestTimeout(new Duration(2, MINUTES))
                    .compressionDisabled(true)
                    .build();

            // start query
            StatementClient client = newStatementClient((Call.Factory) httpClient, clientSession, sql);

            // wait for query to be fully scheduled
            while (client.isRunning() && !client.currentStatusInfo().getStats().isScheduled()) {
                client.advance();
            }

            return new QueryId(client.currentStatusInfo().getId());
        }
        finally {
            // close the client since, query is not managed by the client protocol
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        }
    }

    public static QueryRunner createQueryRunner(Session session)
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setWorkerCount(1)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }
}
