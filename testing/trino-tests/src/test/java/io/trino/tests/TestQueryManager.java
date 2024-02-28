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

import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.execution.QueryState;
import io.trino.server.BasicQueryInfo;
import io.trino.server.SessionContext;
import io.trino.server.protocol.Slug;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.QueryRunnerUtil.createQuery;
import static io.trino.execution.QueryRunnerUtil.waitForQueryState;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.EXCEEDED_CPU_LIMIT;
import static io.trino.spi.StandardErrorCode.EXCEEDED_SCAN_LIMIT;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Arrays.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // run single threaded to avoid creating multiple query runners at once
public class TestQueryManager
{
    @Test
    @Timeout(60)
    public void testFailQuery()
            throws Exception
    {
        try (QueryRunner queryRunner = TpchQueryRunnerBuilder.builder().build()) {
            DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
            QueryId queryId = dispatchManager.createQueryId();
            dispatchManager.createQuery(
                            queryId,
                            Span.getInvalid(),
                            Slug.createNew(),
                            SessionContext.fromSession(TEST_SESSION),
                            "SELECT * FROM lineitem")
                    .get();

            // wait until query starts running
            while (true) {
                QueryState state = dispatchManager.getQueryInfo(queryId).getState();
                if (state.isDone()) {
                    fail("unexpected query state: " + state);
                }
                if (state == RUNNING) {
                    break;
                }
                Thread.sleep(100);
            }

            // cancel query
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            queryManager.failQuery(queryId, new TrinoException(GENERIC_INTERNAL_ERROR, "mock exception"));
            QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
            assertThat(queryInfo.getState()).isEqualTo(FAILED);
            assertThat(queryInfo.getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode());
            assertThat(queryInfo.getFailureInfo()).isNotNull();
            assertThat(queryInfo.getFailureInfo().getMessage()).isEqualTo("mock exception");
        }
    }

    @Test
    @Timeout(60)
    public void testQueryCpuLimit()
            throws Exception
    {
        try (QueryRunner queryRunner = TpchQueryRunnerBuilder.builder().addExtraProperty("query.max-cpu-time", "1ms").build()) {
            QueryId queryId = createQuery(queryRunner, TEST_SESSION, "SELECT COUNT(*) FROM lineitem");
            waitForQueryState(queryRunner, queryId, FAILED);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            BasicQueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            assertThat(queryInfo.getState()).isEqualTo(FAILED);
            assertThat(queryInfo.getErrorCode()).isEqualTo(EXCEEDED_CPU_LIMIT.toErrorCode());
        }
    }

    @Test
    @Timeout(60)
    public void testQueryScanExceeded()
            throws Exception
    {
        try (QueryRunner queryRunner = TpchQueryRunnerBuilder.builder().addExtraProperty("query.max-scan-physical-bytes", "0B").build()) {
            QueryId queryId = createQuery(queryRunner, TEST_SESSION, "SELECT * FROM system.runtime.nodes");
            waitForQueryState(queryRunner, queryId, FAILED);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            BasicQueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            assertThat(queryInfo.getState()).isEqualTo(FAILED);
            assertThat(queryInfo.getErrorCode()).isEqualTo(EXCEEDED_SCAN_LIMIT.toErrorCode());
        }
    }

    @Test
    @Timeout(60)
    public void testQueryScanExceededSession()
            throws Exception
    {
        try (QueryRunner queryRunner = TpchQueryRunnerBuilder.builder().build()) {
            Session session = testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema(TINY_SCHEMA_NAME)
                    .setClientCapabilities(stream(ClientCapabilities.values())
                            .map(ClientCapabilities::toString)
                            .collect(toImmutableSet()))
                    .setSystemProperty("query_max_scan_physical_bytes", "0B")
                    .build();
            QueryId queryId = createQuery(queryRunner, session, "SELECT * FROM system.runtime.nodes");
            waitForQueryState(queryRunner, queryId, FAILED);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            BasicQueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            assertThat(queryInfo.getState()).isEqualTo(FAILED);
            assertThat(queryInfo.getErrorCode()).isEqualTo(EXCEEDED_SCAN_LIMIT.toErrorCode());
        }
    }
}
