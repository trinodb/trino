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
import io.trino.Session;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.trino.SystemSessionProperties.QUERY_EXECUTION_PRIORITY;
import static io.trino.execution.executor.QueryExecutionPriority.LOW;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Comparator.comparing;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestQueryExecutionPriority
{
    private final Session lowPrioritySession = testSessionBuilder()
            .setSystemProperty(QUERY_EXECUTION_PRIORITY, LOW.name())
            .build();

    private final Session normalPrioritySession = testSession();

    @RepeatedTest(30)
    public void testLowPriorityQueriesDoNotBlockOthers()
            throws Exception
    {
        List<QueryStats> lowPriorityQueryStats = new CopyOnWriteArrayList<>();
        List<QueryStats> normalPriorityQueryStats = new CopyOnWriteArrayList<>();
        int numberOfLowPriorityQueries = 20;
        int numberOfNormalPriorityQueries = 10;

        try (ExecutorService lowPriorityExecutor = Executors.newFixedThreadPool(numberOfLowPriorityQueries);
                ExecutorService normalPriorityExecutor = Executors.newFixedThreadPool(2);
                QueryRunner queryRunner = createQueryRunner()) {
            String query = "SELECT COUNT(*) FROM tpch.sf1.lineitem l WHERE shipmode = 'MAIL'";
            List<Future<?>> queryFutures = new ArrayList<>();
            for (int i = 0; i < numberOfLowPriorityQueries; i++) {
                queryFutures.add(lowPriorityExecutor.submit(() -> {
                    MaterializedResultWithPlan result = queryRunner.executeWithPlan(lowPrioritySession, query);
                    lowPriorityQueryStats.add(queryRunner.getCoordinator().getFullQueryInfo(result.queryId()).getQueryStats());
                }));
            }
            for (int i = 0; i < numberOfNormalPriorityQueries; i++) {
                queryFutures.add(normalPriorityExecutor.submit(() -> {
                    MaterializedResultWithPlan result = queryRunner.executeWithPlan(normalPrioritySession, query);
                    normalPriorityQueryStats.add(queryRunner.getCoordinator().getFullQueryInfo(result.queryId()).getQueryStats());
                }));
            }
            // wait for all queries to finish
            for (Future queryFuture : queryFutures) {
                queryFuture.get();
            }
            QueryStats minLowPriorityEndTime = lowPriorityQueryStats.stream().min(comparing(QueryStats::getEndTime)).get();
            QueryStats maxNormalPriorityEndTime = normalPriorityQueryStats.stream().max(comparing(QueryStats::getEndTime)).get();
            assertThat(maxNormalPriorityEndTime.getEndTime())
                    .describedAs("minLowPriorityEndTime " + toString(minLowPriorityEndTime) + " maxNormalPriorityEndTime " + toString(maxNormalPriorityEndTime))
                    .isLessThan(minLowPriorityEndTime.getEndTime());
        }
    }

    private QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunner.builder()
                .setWorkerCount(0)
                .addExtraProperties(ImmutableMap.of(
                        "experimental.thread-per-driver-scheduler-enabled", "true"))
                .build();
    }

    private static String toString(QueryStats queryStats)
    {
        return "createTime: %s, executionStartTime: %s, endTime: %s".formatted(queryStats.getCreateTime(), queryStats.getExecutionStartTime(), queryStats.getEndTime());
    }
}
