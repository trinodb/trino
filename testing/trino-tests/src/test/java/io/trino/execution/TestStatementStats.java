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
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStatementStats
{
    @Test
    public void testUniqueNodeCounts()
            throws Exception
    {
        try (QueryRunner queryRunner = TpchQueryRunner.builder()
                .setCoordinatorProperties(ImmutableMap.of("query-manager.required-workers", "2"))
                .setWorkerCount(1)
                .build()) {
            MaterializedResult result = queryRunner.execute(testSessionBuilder().setCatalog("tpch").setSchema("tiny").build(), "SELECT COUNT(*) from lineitem LIMIT 10");

            assertThat(result.getStatementStats().isPresent()).isTrue();

            StatementStats stats = result.getStatementStats().get();
            // two unique nodes across all stages
            assertThat(stats.getNodes()).isEqualTo(2);

            StageStats rootStage = stats.getRootStage();
            assertThat(rootStage).isNotNull();
            // root stage should be a single node gather
            assertThat(rootStage.getNodes()).isEqualTo(1);

            // one child stage
            assertThat(rootStage.getSubStages().size()).isEqualTo(1);
            // child stage has two unique nodes
            assertThat(rootStage.getSubStages().get(0).getNodes()).isEqualTo(2);
        }
    }
}
