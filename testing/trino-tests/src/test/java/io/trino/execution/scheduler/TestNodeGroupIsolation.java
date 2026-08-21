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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.execution.StageInfo;
import io.trino.execution.resourcegroups.InternalResourceGroupManager;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.resourcegroups.ResourceGroupManagerPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestNodeGroupIsolation
{
    private static final String ETL_GROUP = "etl";
    private static final String ADHOC_GROUP = "adhoc";

    private QueryRunner queryRunner;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        queryRunner = TpchQueryRunner.builder()
                .setWorkerProperties(ImmutableList.of(
                        ImmutableMap.of("node.groups", ETL_GROUP),
                        ImmutableMap.of("node.groups", ETL_GROUP),
                        ImmutableMap.of("node.groups", ADHOC_GROUP),
                        ImmutableMap.of("node.groups", ADHOC_GROUP),
                        // a worker that declares no group, as during a partial rollout
                        ImmutableMap.of()))
                // node groups give no isolation from the coordinator while it also takes worker splits
                .addExtraProperty("node-scheduler.include-coordinator", "false")
                .build();
        queryRunner.installPlugin(new BlackHolePlugin());
        queryRunner.createCatalog("blackhole", "blackhole");
        queryRunner.installPlugin(new ResourceGroupManagerPlugin());
        resourceGroupManager().setConfigurationManager("file", ImmutableMap.of(
                "resource-groups.config-file", getClass().getClassLoader().getResource("resource_groups_node_group.json").getPath()));

        // enough splits that an unrestricted query reaches every worker
        queryRunner.execute(session("other_user"), "CREATE TABLE blackhole.default.test_table (value bigint) " +
                "WITH (split_count = 40, pages_per_split = 1, rows_per_page = 1)");
    }

    @AfterAll
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Test
    public void testQueriesRunOnlyOnTheNodesOfTheirGroup()
    {
        Set<String> etlNodes = nodesInGroup(ETL_GROUP);
        Set<String> adhocNodes = nodesInGroup(ADHOC_GROUP);
        assertThat(etlNodes).hasSize(2);
        assertThat(adhocNodes).hasSize(2);

        assertThat(nodesUsedByQuery("etl_user")).isEqualTo(etlNodes);
        assertThat(nodesUsedByQuery("adhoc_user")).isEqualTo(adhocNodes);

        // a resource group without a node group reaches every worker, including the ungrouped one
        assertThat(nodesUsedByQuery("other_user")).isEqualTo(allWorkers());
        assertThat(allWorkers()).hasSize(5);
    }

    @Test
    public void testWorkerWithoutANodeGroupServesOnlyUnrestrictedQueries()
    {
        Set<String> ungrouped = Sets.difference(allWorkers(), Sets.union(nodesInGroup(ETL_GROUP), nodesInGroup(ADHOC_GROUP)));
        assertThat(ungrouped).hasSize(1);

        // it is reachable by a query that is not restricted to a node group
        assertThat(nodesUsedByQuery("other_user")).containsAll(ungrouped);
        // but declaring no group means belonging to none, so no restricted query can use it
        assertThat(nodesUsedByQuery("etl_user")).doesNotContainAnyElementsOf(ungrouped);
        assertThat(nodesUsedByQuery("adhoc_user")).doesNotContainAnyElementsOf(ungrouped);
    }

    @Test
    public void testConnectorPinningPartitionsToNodesIsRejected()
    {
        // tpch pins partitions to nodes without knowing about node groups, so the query is refused
        assertThatThrownBy(() -> queryRunner.execute(session("etl_user"), "SELECT count(*) FROM tpch.tiny.lineitem"))
                .hasMessageContaining("assigns partitions to specific nodes")
                .hasMessageContaining("node group 'etl'");

        // the same query is fine when the resource group declares no node group
        assertThat(queryRunner.execute(session("other_user"), "SELECT count(*) FROM tpch.tiny.lineitem").getRowCount())
                .isEqualTo(1);
    }

    private Set<String> nodesUsedByQuery(String user)
    {
        MaterializedResultWithPlan result = queryRunner.executeWithPlan(session(user), "SELECT count(*) FROM blackhole.default.test_table");
        QueryInfo queryInfo = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(result.queryId());
        return queryInfo.getStages().orElseThrow().getStages().stream()
                .filter(stage -> !stage.coordinatorOnly())
                .map(StageInfo::tasks)
                .flatMap(List::stream)
                .map(taskInfo -> taskInfo.taskStatus().nodeId())
                .collect(toImmutableSet());
    }

    private Set<String> allWorkers()
    {
        return queryRunner.execute("SELECT node_id FROM system.runtime.nodes WHERE NOT coordinator")
                .getOnlyColumnAsSet().stream()
                .map(String.class::cast)
                .collect(toImmutableSet());
    }

    private Set<String> nodesInGroup(String nodeGroup)
    {
        return queryRunner.execute("SELECT node_id FROM system.runtime.nodes WHERE contains(node_groups, '" + nodeGroup + "')")
                .getOnlyColumnAsSet().stream()
                .map(String.class::cast)
                .collect(toImmutableSet());
    }

    private static Session session(String user)
    {
        return testSessionBuilder()
                .setIdentity(Identity.ofUser(user))
                .build();
    }

    private InternalResourceGroupManager<?> resourceGroupManager()
    {
        return queryRunner.getCoordinator().getResourceGroupManager()
                .orElseThrow(() -> new IllegalArgumentException("no resource group manager"));
    }
}
