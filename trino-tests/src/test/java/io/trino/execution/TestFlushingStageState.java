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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.prestosql.spi.QueryId;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.execution.QueryState.RUNNING;
import static io.prestosql.execution.StageState.CANCELED;
import static io.prestosql.execution.StageState.FLUSHING;
import static io.prestosql.execution.TestQueryRunnerUtil.createQuery;
import static io.prestosql.execution.TestQueryRunnerUtil.waitForQueryState;
import static io.prestosql.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestFlushingStageState
{
    private DistributedQueryRunner queryRunner;

    @BeforeClass
    public void setup()
            throws Exception
    {
        queryRunner = TpchQueryRunnerBuilder.builder().buildWithoutCatalogs();
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of("tpch.splits-per-node", "10000"));
    }

    @Test(timeOut = 30_000)
    public void testFlushingState()
            throws Exception
    {
        QueryId queryId = createQuery(queryRunner, TEST_SESSION, "SELECT * FROM tpch.sf1000.lineitem limit 1");
        waitForQueryState(queryRunner, queryId, RUNNING);

        // wait for the query to finish producing results, but don't poll them
        assertEventually(
                new Duration(10, SECONDS),
                () -> assertEquals(queryRunner.getCoordinator().getFullQueryInfo(queryId).getOutputStage().get().getState(), FLUSHING));

        // wait for the sub stages to go to cancelled state
        assertEventually(
                new Duration(10, SECONDS),
                () -> assertEquals(queryRunner.getCoordinator().getFullQueryInfo(queryId).getOutputStage().get().getSubStages().get(0).getState(), CANCELED));

        QueryInfo queryInfo = queryRunner.getCoordinator().getFullQueryInfo(queryId);
        assertEquals(queryInfo.getState(), RUNNING);
        assertEquals(queryInfo.getOutputStage().get().getState(), FLUSHING);
        assertEquals(queryInfo.getOutputStage().get().getSubStages().size(), 1);
        assertEquals(queryInfo.getOutputStage().get().getSubStages().get(0).getState(), CANCELED);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
    }
}
