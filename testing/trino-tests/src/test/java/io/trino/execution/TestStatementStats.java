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
import io.trino.client.StageStats;
import io.trino.client.StatementStats;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestStatementStats
{
    @Test
    public void testUniqueNodeCounts()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setCoordinatorProperties(ImmutableMap.of("query-manager.required-workers", "2"))
                .setNodeCount(2)
                .build()) {
            MaterializedResult result = queryRunner.execute(testSessionBuilder().setCatalog("tpch").setSchema("tiny").build(), "SELECT COUNT(*) from lineitem LIMIT 10");

            assertTrue(result.getStatementStats().isPresent());

            StatementStats stats = result.getStatementStats().get();
            // two unique nodes across all stages
            assertEquals(stats.getNodes(), 2);

            StageStats rootStage = stats.getRootStage();
            assertNotNull(rootStage);
            // root stage should be a single node gather
            assertEquals(rootStage.getNodes(), 1);

            // one child stage
            assertEquals(rootStage.getSubStages().size(), 1);
            // child stage has two unique nodes
            assertEquals(rootStage.getSubStages().get(0).getNodes(), 2);
        }
    }
}
