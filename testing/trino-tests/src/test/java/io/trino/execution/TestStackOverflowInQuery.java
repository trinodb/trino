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

import io.trino.Session;
import io.trino.plugin.resourcegroups.ResourceGroupManagerPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.util.stream.IntStream;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStackOverflowInQuery
{
    private DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setSystemProperty("task_concurrency", "1")
                .setCatalog("tpch")
                .setSchema("tiny")
                .setClientInfo("{\"clientVersion\":\"testVersion\"}")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.createCatalog("tpch", "tpch");
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
        return queryRunner;
    }

    private String queryThrowingStackOverflowErrorDuringPlanning()
    {
        return "SELECT count(1) FROM tpch.tiny.nation WHERE " +
                IntStream.range(1, 2000)
                        .mapToObj(num -> "nationkey != " + num)
                        .collect(joining(" OR "));
    }

    @Test
    public void testSystemRuntimeQueriesCanBeAccessedAfterExplainQueryThrowingStackOverflowError()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner()) {
            // schedule EXPLAIN query which throws StackOverflowError during analysis
            assertThatThrownBy(() -> queryRunner.execute("EXPLAIN " + queryThrowingStackOverflowErrorDuringPlanning()))
                    .hasMessageContaining("StackOverflowError");

            // verify we can still query system.runtime.queries table (regression test)
            assertThat(queryRunner.execute("SELECT * FROM system.runtime.queries WHERE state = 'FAILED'")).hasSize(1);
        }
    }

    // TODO add a test which verify if resource groups scheduling works after observing EXPLAIN query which throws StackOverflowError during analysis
}
