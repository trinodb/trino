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

import io.airlift.units.Duration;
import io.trino.spi.QueryId;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.QueryRunnerUtil.createQuery;
import static io.trino.execution.QueryRunnerUtil.waitForQueryState;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestPendingStageState
{
    private QueryRunner queryRunner;

    @BeforeAll
    public void setup()
            throws Exception
    {
        queryRunner = TpchQueryRunner.builder()
                .withConnectorProperties(Map.of("tpch.splits-per-node", "10000"))
                .build();
    }

    @Test
    @Timeout(30)
    public void testPendingState()
            throws Exception
    {
        QueryId queryId = createQuery(queryRunner, TEST_SESSION, "SELECT * FROM tpch.sf1000.lineitem limit 1");
        waitForQueryState(queryRunner, queryId, RUNNING);

        // wait for the query to finish producing results, but don't poll them
        assertEventually(
                new Duration(10, SECONDS),
                () -> assertThat(queryRunner.getCoordinator().getFullQueryInfo(queryId).getOutputStage().get().getState()).isEqualTo(StageState.RUNNING));

        // wait for the sub stages to go to pending state
        assertEventually(
                new Duration(10, SECONDS),
                () -> assertThat(queryRunner.getCoordinator().getFullQueryInfo(queryId).getOutputStage().get().getSubStages().get(0).getState()).isEqualTo(StageState.PENDING));

        QueryInfo queryInfo = queryRunner.getCoordinator().getFullQueryInfo(queryId);
        assertThat(queryInfo.getState()).isEqualTo(RUNNING);
        assertThat(queryInfo.getOutputStage().get().getState()).isEqualTo(StageState.RUNNING);
        assertThat(queryInfo.getOutputStage().get().getSubStages()).hasSize(1);
        assertThat(queryInfo.getOutputStage().get().getSubStages().get(0).getState()).isEqualTo(StageState.PENDING);
    }

    @AfterAll
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }
}
