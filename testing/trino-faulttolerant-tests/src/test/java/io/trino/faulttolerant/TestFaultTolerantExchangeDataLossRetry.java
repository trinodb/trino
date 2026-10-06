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
package io.trino.faulttolerant;

import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.FailureInjector;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.execution.FailureInjector.InjectedErrorCode.INJECTED_EXTERNAL_ERROR;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_FAILURE;
import static io.trino.spi.StandardErrorCode.EXCHANGE_DATA_UNRECOVERABLE;
import static io.trino.tpch.TpchTable.NATION;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFaultTolerantExchangeDataLossRetry
        extends AbstractTestQueryFramework
{
    // final aggregation reading the output of the table scan stage through the exchange
    @Language("SQL")
    private static final String QUERY = "SELECT count(*) FROM nation";
    private static final int EXCHANGE_READING_STAGE_ID = 0;

    // shared by all servers of the query runner, as they run in the same JVM
    private final ErrorCodeFailureInjector failureInjector = new ErrorCodeFailureInjector();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setExtraProperties(FaultTolerantExecutionConnectorTestHelper.getExtraProperties())
                .withExchange("filesystem")
                .setInitialTables(List.of(NATION))
                .setAdditionalModule(binder -> newOptionalBinder(binder, FailureInjector.class).setBinding().toInstance(failureInjector))
                .build();
    }

    @Test
    public void testSingleExchangeDataLossIsRetried()
    {
        Session session = sessionFailingAttempts(EXCHANGE_DATA_UNRECOVERABLE.toErrorCode(), 0);

        assertThat(query(session, QUERY)).matches("VALUES BIGINT '25'");
    }

    @Test
    public void testRepeatedExchangeDataLossFailsQuery()
    {
        Session session = sessionFailingAttempts(EXCHANGE_DATA_UNRECOVERABLE.toErrorCode(), 0, 1);

        // attempts remain in the retry budget, but the second exchange data loss must not be retried
        assertThat(query(session, QUERY)).failure().hasErrorCode(EXCHANGE_DATA_UNRECOVERABLE);
    }

    @Test
    public void testRepeatedTransientFailureIsRetried()
    {
        // same injection pattern as above with a transient external error, to show the outcome depends on the error code
        Session session = sessionFailingAttempts(INJECTED_EXTERNAL_ERROR.toErrorCode(), 0, 1);

        assertThat(query(session, QUERY)).matches("VALUES BIGINT '25'");
    }

    private Session sessionFailingAttempts(ErrorCode errorCode, int... attemptIds)
    {
        String traceToken = UUID.randomUUID().toString();
        for (int attemptId : attemptIds) {
            failureInjector.injectTaskFailure(traceToken, EXCHANGE_READING_STAGE_ID, 0, attemptId, errorCode);
        }
        return Session.builder(getSession())
                .setTraceToken(Optional.of(traceToken))
                .build();
    }

    // TestingFailureInjector can only inject a generic error code per error type
    private static class ErrorCodeFailureInjector
            implements FailureInjector
    {
        private final Map<Key, ErrorCode> failures = new ConcurrentHashMap<>();

        public void injectTaskFailure(String traceToken, int stageId, int partitionId, int attemptId, ErrorCode errorCode)
        {
            failures.put(new Key(traceToken, stageId, partitionId, attemptId), errorCode);
        }

        @Override
        public void injectTaskFailure(String traceToken, int stageId, int partitionId, int attemptId, InjectedFailureType injectionType, Optional<ErrorType> errorType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<InjectedFailure> getInjectedFailure(String traceToken, int stageId, int partitionId, int attemptId)
        {
            return Optional.ofNullable(failures.get(new Key(traceToken, stageId, partitionId, attemptId)))
                    .map(errorCode -> new InjectedFailure(TASK_FAILURE, Optional.of(errorCode.getType()))
                    {
                        @Override
                        public Throwable getTaskFailureException()
                        {
                            return new TrinoException(() -> errorCode, FAILURE_INJECTION_MESSAGE);
                        }
                    });
        }

        @Override
        public Duration getRequestTimeout()
        {
            return new Duration(10, SECONDS);
        }

        private record Key(String traceToken, int stageId, int partitionId, int attemptId) {}
    }
}
