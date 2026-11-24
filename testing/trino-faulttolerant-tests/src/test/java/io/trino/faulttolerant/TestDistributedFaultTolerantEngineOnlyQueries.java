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

import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.execution.StageInfo;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractDistributedEngineOnlyQueries;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.execution.StageState.ABORTED;
import static io.trino.execution.scheduler.faulttolerant.EventDrivenFaultTolerantQueryScheduler.NO_FINAL_TASK_INFO_CHECK_INTERVAL;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDistributedFaultTolerantEngineOnlyQueries
        extends AbstractDistributedEngineOnlyQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = MemoryQueryRunner.builder()
                .setExtraProperties(FaultTolerantExecutionConnectorTestHelper.getExtraProperties())
                .withExchange("filesystem")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();

        queryRunner.getCoordinator().getSessionPropertyManager().addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        try {
            queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withSessionProperties(TEST_CATALOG_PROPERTIES)
                    .build()));
            queryRunner.createCatalog(TESTING_CATALOG, "mock");
            queryRunner.installPlugin(new BlackHolePlugin());
            queryRunner.createCatalog("blackhole", "blackhole");
        }
        catch (RuntimeException e) {
            throw closeAllSuppress(e, queryRunner);
        }
        return queryRunner;
    }

    @Override
    @Test
    @Disabled
    public void testExplainAnalyzeVerbose()
    {
        // Spooling exchange does not prove output buffer utilization histogram
    }

    @Test
    @Override
    @Disabled
    public void testSelectiveLimit()
    {
        // FTE mode does not terminate query when limit is reached
    }

    @Test
    public void testIssue18383()
    {
        String tableName = "test_issue_18383_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id VARCHAR)");

        assertQueryReturnsEmptyResult(
                """
                WITH
                t1 AS (
                    SELECT NULL AS address_id FROM %s i1
                        INNER JOIN %s i2 ON i1.id = i2.id),
                t2 AS (
                    SELECT id AS address_id FROM %s
                    UNION
                    SELECT * FROM t1)
                SELECT * FROM t2
                    INNER JOIN %s i ON i.id = t2.address_id
                """.formatted(tableName, tableName, tableName, tableName));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testIssue25080()
    {
        // regression test for verifying logic for catching queries with taks missing final info works correctly.
        // https://github.com/trinodb/trino/pull/25080
        assertUpdate(
                """
                CREATE TABLE blackhole.default.fast (dummy BIGINT)
                WITH (split_count = 1,
                      pages_per_split = 1,
                      rows_per_page = 1)
                """);
        assertUpdate(
                """
                CREATE TABLE blackhole.default.delay (dummy BIGINT)
                WITH (split_count = 1,
                      pages_per_split = 1,
                      rows_per_page = 1,
                      page_processing_delay = '%ss')
                """.formatted(((int) NO_FINAL_TASK_INFO_CHECK_INTERVAL.getValue(SECONDS)) + 5));
        assertThat(query("SELECT * FROM blackhole.default.delay UNION ALL SELECT * FROM blackhole.default.fast"))
                .succeeds()
                .matches("VALUES BIGINT '0', BIGINT '0'");
    }

    @Test
    public void testTablesOfStageWhichWasNotCreated()
    {
        // the stage which scans nation is not created, as the stage it reads the other side of the join from fails
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .build();
        assertThatThrownBy(() -> getQueryRunner().execute(
                session,
                "SELECT count(*) FROM nation n JOIN (SELECT regionkey / (regionkey - regionkey) AS regionkey FROM region) r ON n.regionkey = r.regionkey"))
                .isInstanceOfSatisfying(QueryFailedException.class, exception -> {
                    List<StageInfo> stages = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(exception.getQueryId())
                            .getStages().orElseThrow()
                            .getStages();
                    assertThat(stages).anyMatch(stage -> stage.state() == ABORTED && stage.tasks().isEmpty() && !stage.tables().isEmpty());
                    assertThat(stages).allSatisfy(stage -> assertThat(stage.tables().keySet()).isEqualTo(
                            searchFrom(stage.plan().getRoot())
                                    .where(TableScanNode.class::isInstance)
                                    .findAll().stream()
                                    .map(PlanNode::getId)
                                    .collect(toImmutableSet())));
                });
    }
}
