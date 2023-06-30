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
import io.trino.spi.QueryId;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.QueryRunnerUtil.createQuery;
import static io.trino.execution.QueryRunnerUtil.waitForQueryState;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_SPLITS_PER_NODE;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestPendingStageState
{
    private DistributedQueryRunner queryRunner;

    @BeforeClass
    public void setup()
            throws Exception
    {
        queryRunner = TpchQueryRunnerBuilder.builder().buildWithoutCatalogs();
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of(TPCH_SPLITS_PER_NODE, "10000"));
    }

    @Test(timeOut = 30_000)
    public void testPendingState()
            throws Exception
    {
        QueryId queryId = createQuery(queryRunner, TEST_SESSION, "SELECT * FROM tpch.sf1000.lineitem limit 1");
        waitForQueryState(queryRunner, queryId, RUNNING);

        // wait for the query to finish producing results, but don't poll them
        assertEventually(
                new Duration(10, SECONDS),
                () -> assertEquals(queryRunner.getCoordinator().getFullQueryInfo(queryId).getOutputStage().get().getState(), StageState.RUNNING));

        // wait for the sub stages to go to pending state
        assertEventually(
                new Duration(10, SECONDS),
                () -> assertEquals(queryRunner.getCoordinator().getFullQueryInfo(queryId).getOutputStage().get().getSubStages().get(0).getState(), StageState.PENDING));

        QueryInfo queryInfo = queryRunner.getCoordinator().getFullQueryInfo(queryId);
        assertEquals(queryInfo.getState(), RUNNING);
        assertEquals(queryInfo.getOutputStage().get().getState(), StageState.RUNNING);
        assertEquals(queryInfo.getOutputStage().get().getSubStages().size(), 1);
        assertEquals(queryInfo.getOutputStage().get().getSubStages().get(0).getState(), StageState.PENDING);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }
}
