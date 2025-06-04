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
package io.trino.execution.resourcegroups.db;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.execution.resourcegroups.InternalResourceGroupManager;
import io.trino.plugin.resourcegroups.db.DbResourceGroupConfigurationManager;
import io.trino.plugin.resourcegroups.db.H2ResourceGroupsDao;
import io.trino.server.BasicQueryInfo;
import io.trino.server.ResourceGroupInfo;
import io.trino.spi.QueryId;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.Identity;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SystemSessionProperties.QUERY_MAX_EXECUTION_TIME;
import static io.trino.execution.QueryRunnerUtil.cancelQuery;
import static io.trino.execution.QueryRunnerUtil.createQuery;
import static io.trino.execution.QueryRunnerUtil.waitForQueryState;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.execution.TestQueues.createResourceGroupId;
import static io.trino.execution.resourcegroups.db.H2TestUtil.TEST_ENVIRONMENT;
import static io.trino.execution.resourcegroups.db.H2TestUtil.adhocSession;
import static io.trino.execution.resourcegroups.db.H2TestUtil.createQueryRunner;
import static io.trino.execution.resourcegroups.db.H2TestUtil.dashboardSession;
import static io.trino.execution.resourcegroups.db.H2TestUtil.getDao;
import static io.trino.execution.resourcegroups.db.H2TestUtil.getDbConfigUrl;
import static io.trino.execution.resourcegroups.db.H2TestUtil.getSelectors;
import static io.trino.execution.resourcegroups.db.H2TestUtil.rejectingSession;
import static io.trino.execution.resourcegroups.db.H2TestUtil.waitForCompleteQueryCount;
import static io.trino.execution.resourcegroups.db.H2TestUtil.waitForRunningQueryCount;
import static io.trino.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static io.trino.spi.StandardErrorCode.INVALID_RESOURCE_GROUP;
import static io.trino.spi.StandardErrorCode.QUERY_QUEUE_FULL;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // run single threaded to avoid creating multiple query runners at once
public class TestQueuesDb
{
    // Copy of TestQueues with tests for db reconfiguration of resource groups
    private static final String LONG_LASTING_QUERY = "SELECT COUNT(*) FROM lineitem";
    private QueryRunner queryRunner;
    private H2ResourceGroupsDao dao;

    @BeforeEach
    public void setup()
            throws Exception
    {
        String dbConfigUrl = getDbConfigUrl();
        dao = getDao(dbConfigUrl);
        queryRunner = createQueryRunner(dbConfigUrl, dao);
    }

    @AfterEach
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    @Timeout(60)
    public void testRunningQuery()
            throws Exception
    {
        queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
        while (true) {
            ResourceGroupInfo global = queryRunner.getCoordinator().getResourceGroupManager().get()
                    .tryGetResourceGroupInfo(new ResourceGroupId(new ResourceGroupId("global"), "bi-user"))
                    .orElseThrow(() -> new IllegalStateException("Resource group not found"));
            if (global.softMemoryLimit().toBytes() > 0) {
                break;
            }
            TimeUnit.SECONDS.sleep(2);
        }
    }

    @Test
    @Timeout(60)
    public void testBasic()
            throws Exception
    {
        // submit first "dashboard" query
        QueryId firstDashboardQuery = createQuery(queryRunner, dashboardSession(), LONG_LASTING_QUERY);
        // wait for the first "dashboard" query to start
        waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);
        waitForRunningQueryCount(queryRunner, 1);
        // submit second "dashboard" query
        QueryId secondDashboardQuery = createQuery(queryRunner, dashboardSession(), LONG_LASTING_QUERY);
        MILLISECONDS.sleep(2000);
        // wait for the second "dashboard" query to be queued ("dashboard.${USER}" queue strategy only allows one "dashboard" query to be accepted for execution)
        waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);
        waitForRunningQueryCount(queryRunner, 1);
        // Update db to allow for 1 more running query in dashboard resource group
        dao.updateResourceGroup(3, "user-${USER}", "1MB", 3, 4, 4, null, null, null, null, null, 1L, TEST_ENVIRONMENT);
        dao.updateResourceGroup(5, "dashboard-${USER}", "1MB", 1, 2, 2, null, null, null, null, null, 3L, TEST_ENVIRONMENT);
        waitForQueryState(queryRunner, secondDashboardQuery, RUNNING);
        QueryId thirdDashboardQuery = createQuery(queryRunner, dashboardSession(), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, thirdDashboardQuery, QUEUED);
        waitForRunningQueryCount(queryRunner, 2);
        // submit first non "dashboard" query
        QueryId firstNonDashboardQuery = createQuery(queryRunner, adhocSession(), LONG_LASTING_QUERY);
        // wait for the first non "dashboard" query to start
        waitForQueryState(queryRunner, firstNonDashboardQuery, RUNNING);
        waitForRunningQueryCount(queryRunner, 3);
        // submit second non "dashboard" query
        QueryId secondNonDashboardQuery = createQuery(queryRunner, adhocSession(), LONG_LASTING_QUERY);
        // wait for the second non "dashboard" query to start
        waitForQueryState(queryRunner, secondNonDashboardQuery, RUNNING);
        waitForRunningQueryCount(queryRunner, 4);
        // cancel first "dashboard" query, the second "dashboard" query and second non "dashboard" query should start running
        cancelQuery(queryRunner, firstDashboardQuery);
        waitForQueryState(queryRunner, firstDashboardQuery, FAILED);
        waitForQueryState(queryRunner, thirdDashboardQuery, RUNNING);
        waitForRunningQueryCount(queryRunner, 4);
        waitForCompleteQueryCount(queryRunner, 1);
    }

    @Test
    @Timeout(60)
    public void testTwoQueriesAtSameTime()
            throws Exception
    {
        QueryId firstDashboardQuery = createQuery(queryRunner, dashboardSession(), LONG_LASTING_QUERY);
        QueryId secondDashboardQuery = createQuery(queryRunner, dashboardSession(), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);
        waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);
    }

    @Test
    @Timeout(90)
    public void testTooManyQueries()
            throws Exception
    {
        QueryId firstDashboardQuery = createQuery(queryRunner, dashboardSession(), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);

        QueryId secondDashboardQuery = createQuery(queryRunner, dashboardSession(), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);

        QueryId thirdDashboardQuery = createQuery(queryRunner, dashboardSession(), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, thirdDashboardQuery, FAILED);

        // Allow one more query to run and resubmit third query
        dao.updateResourceGroup(3, "user-${USER}", "1MB", 3, 4, 4, null, null, null, null, null, 1L, TEST_ENVIRONMENT);
        dao.updateResourceGroup(5, "dashboard-${USER}", "1MB", 1, 2, 2, null, null, null, null, null, 3L, TEST_ENVIRONMENT);

        InternalResourceGroupManager<?> manager = queryRunner.getCoordinator().getResourceGroupManager().get();
        DbResourceGroupConfigurationManager dbConfigurationManager = (DbResourceGroupConfigurationManager) manager.getConfigurationManager();

        // Trigger reload to make the test more deterministic
        dbConfigurationManager.load();
        waitForQueryState(queryRunner, secondDashboardQuery, RUNNING);
        thirdDashboardQuery = createQuery(queryRunner, dashboardSession(), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, thirdDashboardQuery, QUEUED);

        // Lower running queries in dashboard resource groups and reload the config
        dao.updateResourceGroup(5, "dashboard-${USER}", "1MB", 1, 1, 1, null, null, null, null, null, 3L, TEST_ENVIRONMENT);
        dbConfigurationManager.load();

        // Cancel query and verify that third query is still queued
        cancelQuery(queryRunner, firstDashboardQuery);
        waitForQueryState(queryRunner, firstDashboardQuery, FAILED);
        MILLISECONDS.sleep(2000);
        waitForQueryState(queryRunner, thirdDashboardQuery, QUEUED);
    }

    @Test
    @Timeout(60)
    public void testRejection()
            throws Exception
    {
        InternalResourceGroupManager<?> manager = queryRunner.getCoordinator().getResourceGroupManager().get();
        DbResourceGroupConfigurationManager dbConfigurationManager = (DbResourceGroupConfigurationManager) manager.getConfigurationManager();
        // Verify the query cannot be submitted
        QueryId queryId = createQuery(queryRunner, rejectingSession(), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, queryId, FAILED);
        DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
        assertThat(dispatchManager.getQueryInfo(queryId).getErrorCode()).isEqualTo(QUERY_REJECTED.toErrorCode());
        int selectorCount = getSelectors(queryRunner).size();
        dao.insertSelector(4, 100_000, "user.*", null, null, null, "(?i).*reject.*", null, null, null);
        dbConfigurationManager.load();
        assertThat(getSelectors(queryRunner)).hasSize(selectorCount + 1);
        // Verify the query can be submitted
        queryId = createQuery(queryRunner, rejectingSession(), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, queryId, RUNNING);
        dao.deleteSelector(4, "user.*", null, null, null, "(?i).*reject.*", null);
        dbConfigurationManager.load();
        // Verify the query cannot be submitted
        queryId = createQuery(queryRunner, rejectingSession(), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, queryId, FAILED);
    }

    @Test
    @Timeout(60)
    public void testQuerySystemTableResourceGroup()
            throws Exception
    {
        QueryId firstQuery = createQuery(queryRunner, dashboardSession(), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, firstQuery, RUNNING);
        MaterializedResult result = queryRunner.execute("SELECT resource_group_id FROM system.runtime.queries WHERE source = 'dashboard'");
        assertThat(result.getOnlyValue()).isEqualTo(ImmutableList.of("global", "user-user", "dashboard-user"));
    }

    @Test
    @Timeout(60)
    public void testSelectorPriority()
            throws Exception
    {
        InternalResourceGroupManager<?> manager = queryRunner.getCoordinator().getResourceGroupManager().get();
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        DbResourceGroupConfigurationManager dbConfigurationManager = (DbResourceGroupConfigurationManager) manager.getConfigurationManager();

        QueryId firstQuery = createQuery(queryRunner, dashboardSession(), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, firstQuery, RUNNING);

        Optional<ResourceGroupId> resourceGroup = queryManager.getFullQueryInfo(firstQuery).getResourceGroupId();
        assertThat(resourceGroup).isPresent();
        assertThat(resourceGroup.get().toString()).isEqualTo("global.user-user.dashboard-user");

        // create a new resource group that rejects all queries submitted to it
        dao.insertResourceGroup(8, "reject-all-queries", "1MB", 0, 0, 0, null, null, null, null, null, 3L, TEST_ENVIRONMENT);

        // add a new selector that has a higher priority than the existing dashboard selector and that routes queries to the "reject-all-queries" resource group
        dao.insertSelector(8, 200, "user.*", null, null, null, "(?i).*dashboard.*", null, null, null);

        // reload the configuration
        dbConfigurationManager.load();

        QueryId secondQuery = createQuery(queryRunner, dashboardSession(), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, secondQuery, FAILED);

        DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
        BasicQueryInfo basicQueryInfo = dispatchManager.getQueryInfo(secondQuery);
        if (!QUERY_QUEUE_FULL.toErrorCode().equals(basicQueryInfo.getErrorCode())) {
            AssertionError failure = new AssertionError("Expected query to fail with QUERY_QUEUE_FULL error code, but got: %s".formatted(basicQueryInfo.getErrorCode()));
            dispatchManager.getFullQueryInfo(secondQuery)
                    .map(QueryInfo::getFailureInfo) // nullable
                    .map(ExecutionFailureInfo::toException)
                    .ifPresent(failure::addSuppressed);
            throw failure;
        }
    }

    @Test
    @Timeout(60)
    public void testQueryExecutionTimeLimit()
            throws Exception
    {
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        InternalResourceGroupManager<?> manager = queryRunner.getCoordinator().getResourceGroupManager().get();
        DbResourceGroupConfigurationManager dbConfigurationManager = (DbResourceGroupConfigurationManager) manager.getConfigurationManager();
        QueryId firstQuery = createQuery(
                queryRunner,
                testSessionBuilder()
                        .setCatalog("tpch")
                        .setSchema("sf100000")
                        .setSource("dashboard")
                        .setSystemProperty(QUERY_MAX_EXECUTION_TIME, "1ms")
                        .build(),
                LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, firstQuery, FAILED);
        assertThat(queryManager.getFullQueryInfo(firstQuery).getErrorCode()).isEqualTo(EXCEEDED_TIME_LIMIT.toErrorCode());
        assertThat(queryManager.getFullQueryInfo(firstQuery).getFailureInfo().getMessage()).contains("Query exceeded the maximum execution time limit of 1.00ms");
        // set max running queries to 0 for the dashboard resource group so that new queries get queued immediately
        dao.updateResourceGroup(5, "dashboard-${USER}", "1MB", 1, null, 0, null, null, null, null, null, 3L, TEST_ENVIRONMENT);
        dbConfigurationManager.load();
        QueryId secondQuery = createQuery(
                queryRunner,
                testSessionBuilder()
                        .setCatalog("tpch")
                        .setSchema("sf100000")
                        .setSource("dashboard")
                        .setSystemProperty(QUERY_MAX_EXECUTION_TIME, "1ms")
                        .build(),
                LONG_LASTING_QUERY);
        //this query should immediately get queued
        waitForQueryState(queryRunner, secondQuery, QUEUED);
        // after a 5s wait this query should still be QUEUED, not FAILED as the max execution time should be enforced after the query starts running
        Thread.sleep(5_000);
        DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
        assertThat(dispatchManager.getQueryInfo(secondQuery).getState()).isEqualTo(QUEUED);
        // reconfigure the resource group to run the second query
        dao.updateResourceGroup(5, "dashboard-${USER}", "1MB", 1, null, 1, null, null, null, null, null, 3L, TEST_ENVIRONMENT);
        dbConfigurationManager.load();
        // cancel the first one and let the second one start
        dispatchManager.cancelQuery(firstQuery);
        // wait until the second one is FAILED
        waitForQueryState(queryRunner, secondQuery, FAILED);
    }

    @Test
    public void testQueryTypeBasedSelection()
            throws InterruptedException
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .build();
        QueryId queryId = createQuery(queryRunner, session, "EXPLAIN " + LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, queryId, ImmutableSet.of(RUNNING, FINISHED));
        Optional<ResourceGroupId> resourceGroupId = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getResourceGroupId();
        assertThat(resourceGroupId.isPresent())
                .describedAs("Query should have a resource group")
                .isTrue();
        assertThat(resourceGroupId.get()).isEqualTo(createResourceGroupId("explain"));
    }

    @Test
    public void testClientTagsBasedSelection()
            throws InterruptedException
    {
        assertResourceGroupWithClientTags(ImmutableSet.of("tag1"), createResourceGroupId("global", "bi-user"));
        assertResourceGroupWithClientTags(ImmutableSet.of("tag1", "tag2"), createResourceGroupId("global", "user-user", "adhoc-user"));
    }

    @Test
    public void testNonLeafGroup()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setSource("non-leaf")
                .build();
        InternalResourceGroupManager<?> manager = queryRunner.getCoordinator().getResourceGroupManager().get();
        DbResourceGroupConfigurationManager dbConfigurationManager = (DbResourceGroupConfigurationManager) manager.getConfigurationManager();
        int originalSize = getSelectors(queryRunner).size();
        // Add a selector for a non leaf group
        dao.insertSelector(3, 100, "user.*", null, null, null, "(?i).*non-leaf.*", null, null, null);
        dbConfigurationManager.load();
        while (getSelectors(queryRunner).size() != originalSize + 1) {
            MILLISECONDS.sleep(500);
        }
        // Submit query with side effect of creating resource groups
        QueryId firstDashboardQuery = createQuery(queryRunner, dashboardSession(), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);
        cancelQuery(queryRunner, firstDashboardQuery);
        waitForQueryState(queryRunner, firstDashboardQuery, FAILED);
        // Submit a query to a non-leaf resource group
        QueryId invalidResourceGroupQuery = createQuery(queryRunner, session, LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, invalidResourceGroupQuery, FAILED);
        assertThat(queryRunner.getCoordinator().getDispatchManager().getQueryInfo(invalidResourceGroupQuery).getErrorCode()).isEqualTo(INVALID_RESOURCE_GROUP.toErrorCode());
    }

    @Test
    public void testUpdateSoftMemoryLimit()
    {
        // trigger resource group creation
        queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
        InternalResourceGroupManager<?> manager = queryRunner.getCoordinator().getResourceGroupManager().orElseThrow();
        DbResourceGroupConfigurationManager dbConfigurationManager = (DbResourceGroupConfigurationManager) manager.getConfigurationManager();

        dao.updateResourceGroup(2, "bi-${USER}", "100%", 3, 2, 2, null, null, null, null, null, 1L, TEST_ENVIRONMENT);
        dbConfigurationManager.load();
        assertThat(manager.tryGetResourceGroupInfo(new ResourceGroupId(new ResourceGroupId("global"), "bi-user"))
                .orElseThrow(() -> new IllegalStateException("Resource group not found"))
                .softMemoryLimit()
                .toBytes()).isEqualTo(queryRunner.getCoordinator().getClusterMemoryManager().getClusterMemoryBytes());

        dao.updateResourceGroup(2, "bi-${USER}", "123MB", 3, 2, 2, null, null, null, null, null, 1L, TEST_ENVIRONMENT);
        dbConfigurationManager.load();

        // wait for SqlQueryManager which enforce memory limits per second
        assertEventually(
                new Duration(2, TimeUnit.SECONDS),
                new Duration(100, TimeUnit.MILLISECONDS),
                () -> assertThat(manager.tryGetResourceGroupInfo(new ResourceGroupId(new ResourceGroupId("global"), "bi-user"))
                        .orElseThrow(() -> new IllegalStateException("Resource group not found"))
                        .softMemoryLimit()).isEqualTo(DataSize.of(123, MEGABYTE)));
    }

    @Test
    @Timeout(60)
    public void testAddSubGroup()
            throws InterruptedException
    {
        InternalResourceGroupManager<?> manager = queryRunner.getCoordinator().getResourceGroupManager().orElseThrow();
        DbResourceGroupConfigurationManager dbConfigurationManager = (DbResourceGroupConfigurationManager) manager.getConfigurationManager();

        dao.insertResourceGroup(10, "queued", "80%", 10, null, 1, null, null, null, null, null, null, TEST_ENVIRONMENT);
        dao.insertSelector(10, 1, null, null, null, null, null, null, "[\"queued\"]", null);
        dao.insertResourceGroup(11, "running", "80%", 10, null, 2, null, null, null, null, null, null, TEST_ENVIRONMENT);
        dao.insertSelector(11, 1, null, null, null, null, null, null, "[\"running\"]", null);
        dbConfigurationManager.load();

        QueryId firstQueryFromQueuedGroup = createQuery(queryRunner, session("alice", "queued"), LONG_LASTING_QUERY);
        QueryId secondQueryFromQueuedGroup = createQuery(queryRunner, session("alice", "queued"), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, firstQueryFromQueuedGroup, RUNNING);
        waitForQueryState(queryRunner, secondQueryFromQueuedGroup, QUEUED);

        QueryId firstQueryFromRunningGroup = createQuery(queryRunner, session("alice", "running"), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, firstQueryFromRunningGroup, RUNNING);

        dao.deleteSelectors(10);
        dao.insertResourceGroup(12, "subgroup", "80%", 10, null, 1, null, null, null, null, null, 10L, TEST_ENVIRONMENT);
        dao.insertSelector(12, 1, null, null, null, null, null, null, "[\"queued\"]", null);
        dao.deleteSelectors(11);
        dao.insertResourceGroup(13, "subgroup", "80%", 10, null, 1, null, null, null, null, null, 11L, TEST_ENVIRONMENT);
        dao.insertSelector(13, 1, null, null, null, null, null, null, "[\"running\"]", null);
        dbConfigurationManager.load();

        QueryId thirdQueryFromQueuedGroup = createQuery(queryRunner, session("alice", "queued"), LONG_LASTING_QUERY);
        QueryId secondQueryFromRunningGroup = createQuery(queryRunner, session("alice", "running"), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, firstQueryFromQueuedGroup, RUNNING);
        waitForQueryState(queryRunner, secondQueryFromQueuedGroup, QUEUED);
        waitForQueryState(queryRunner, thirdQueryFromQueuedGroup, FAILED);
        assertFailureMessage(thirdQueryFromQueuedGroup, "Cannot add sub group to 'queued' while queries are queued");
        waitForQueryState(queryRunner, firstQueryFromRunningGroup, RUNNING);
        waitForQueryState(queryRunner, secondQueryFromRunningGroup, RUNNING);
    }

    @Test
    @Timeout(60)
    public void testDisableSubGroup()
            throws InterruptedException
    {
        InternalResourceGroupManager<?> manager = queryRunner.getCoordinator().getResourceGroupManager().orElseThrow();
        DbResourceGroupConfigurationManager dbConfigurationManager = (DbResourceGroupConfigurationManager) manager.getConfigurationManager();

        dao.insertResourceGroup(10, "queued", "80%", 10, null, 3, null, null, null, null, null, null, TEST_ENVIRONMENT);
        dao.insertResourceGroup(11, "subgroup", "80%", 10, null, 1, null, null, null, null, null, 10L, TEST_ENVIRONMENT);
        dao.insertSelector(11, 1, null, null, null, null, null, null, "[\"queued\"]", null);
        dao.insertResourceGroup(12, "running", "80%", 10, null, 3, null, null, null, null, null, null, TEST_ENVIRONMENT);
        dao.insertResourceGroup(13, "subgroup", "80%", 10, null, 1, null, null, null, null, null, 12L, TEST_ENVIRONMENT);
        dao.insertSelector(13, 1, null, null, null, null, null, null, "[\"running\"]", null);
        dbConfigurationManager.load();

        QueryId firstQueryFromQueuedGroup = createQuery(queryRunner, session("alice", "queued"), LONG_LASTING_QUERY);
        QueryId secondQueryFromQueuedGroup = createQuery(queryRunner, session("alice", "queued"), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, firstQueryFromQueuedGroup, RUNNING);
        waitForQueryState(queryRunner, secondQueryFromQueuedGroup, QUEUED);

        QueryId firstQueryFromRunningGroup = createQuery(queryRunner, session("alice", "running"), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, firstQueryFromRunningGroup, RUNNING);

        dao.deleteSelectors(11);
        dao.deleteResourceGroup(11);
        dao.insertSelector(10, 1, null, null, null, null, null, null, "[\"queued\"]", null);
        dao.deleteSelectors(13);
        dao.deleteResourceGroup(13);
        dao.insertSelector(12, 1, null, null, null, null, null, null, "[\"running\"]", null);
        dbConfigurationManager.load();

        QueryId thirdQueryFromQueuedGroup = createQuery(queryRunner, session("alice", "queued"), LONG_LASTING_QUERY);
        QueryId secondQueryFromRunningGroup = createQuery(queryRunner, session("alice", "running"), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, firstQueryFromQueuedGroup, RUNNING);
        waitForQueryState(queryRunner, secondQueryFromQueuedGroup, QUEUED);
        waitForQueryState(queryRunner, thirdQueryFromQueuedGroup, FAILED);
        assertFailureMessage(thirdQueryFromQueuedGroup, "Cannot add queries to 'queued'. It is not a leaf group.");
        waitForQueryState(queryRunner, firstQueryFromRunningGroup, RUNNING);
        waitForQueryState(queryRunner, secondQueryFromRunningGroup, RUNNING);
    }

    @Test
    @Timeout(60)
    public void testSwitchGroupNameBetweenTemplateAndFixedValue()
            throws InterruptedException
    {
        InternalResourceGroupManager<?> manager = queryRunner.getCoordinator().getResourceGroupManager().orElseThrow();
        DbResourceGroupConfigurationManager dbConfigurationManager = (DbResourceGroupConfigurationManager) manager.getConfigurationManager();

        dao.insertResourceGroup(10, "${USER}", "80%", 0, null, 0, null, null, null, null, null, null, TEST_ENVIRONMENT);
        dao.insertSelector(10, 1, null, null, null, null, null, null, "[\"tag\"]", null);
        dbConfigurationManager.load();

        QueryId firstQuery = createQuery(queryRunner, session("admin", "tag"), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, firstQuery, FAILED);
        assertFailureMessage(firstQuery, "Too many queued queries for \"admin\"");

        dao.updateResourceGroup(10, "admin", "80%", 1, null, 0, null, null, null, null, null, null, TEST_ENVIRONMENT);
        dbConfigurationManager.load();

        QueryId secondQuery = createQuery(queryRunner, session("admin", "tag"), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, secondQuery, QUEUED);

        dao.updateResourceGroup(10, "${USER}", "80%", 1, null, 2, null, null, null, null, null, null, TEST_ENVIRONMENT);
        dbConfigurationManager.load();

        QueryId thirdQuery = createQuery(queryRunner, session("admin", "tag"), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, secondQuery, RUNNING);
        waitForQueryState(queryRunner, thirdQuery, RUNNING);
    }

    @Test
    @Timeout(60)
    public void testGroupHasSingleConfig()
            throws InterruptedException
    {
        InternalResourceGroupManager<?> manager = queryRunner.getCoordinator().getResourceGroupManager().orElseThrow();
        DbResourceGroupConfigurationManager dbConfigurationManager = (DbResourceGroupConfigurationManager) manager.getConfigurationManager();

        dao.insertResourceGroup(10, "${USER}", "80%", 100, null, 100, null, null, null, null, null, null, TEST_ENVIRONMENT);
        dao.insertSelector(10, 100, null, null, null, null, null, null, "[\"tag\"]", null);
        dbConfigurationManager.load();

        // create a resource group using config '${USER}'
        QueryId firstQuery = createQuery(queryRunner, session("admin", "tag"), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, firstQuery, RUNNING);

        dao.updateResourceGroup(10, "admin", "80%", 100, null, 100, null, null, null, null, null, null, TEST_ENVIRONMENT);
        dbConfigurationManager.load();

        // associate the resource group with config 'admin'
        QueryId secondQuery = createQuery(queryRunner, session("admin", "tag"), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, secondQuery, RUNNING);

        dao.insertResourceGroup(11, "${USER}", "80%", 0, null, 0, null, null, null, null, null, null, TEST_ENVIRONMENT);
        dao.insertSelector(11, 101, null, null, null, null, null, null, "[\"tag\"]", null);
        dbConfigurationManager.load();

        // since the config 'admin' exists the group should not be configured using '${USER}'
        QueryId thirdQuery = createQuery(queryRunner, session("admin", "tag"), LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, thirdQuery, RUNNING);
    }

    private static Session session(String user, String clientTag)
    {
        return testSessionBuilder()
                .setIdentity(Identity.ofUser(user))
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setClientTags(ImmutableSet.of(clientTag))
                .build();
    }

    private void assertFailureMessage(QueryId queryId, String message)
    {
        Optional<QueryInfo> fullQueryInfo = queryRunner.getCoordinator().getDispatchManager().getFullQueryInfo(queryId);
        assertThat(fullQueryInfo.isPresent()).isTrue();
        ExecutionFailureInfo failureInfo = fullQueryInfo.get().getFailureInfo();
        assertThat(failureInfo).isNotNull();
        assertThat(failureInfo.getMessage()).isEqualTo(message);
    }

    private void assertResourceGroupWithClientTags(Set<String> clientTags, ResourceGroupId expectedResourceGroup)
            throws InterruptedException
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setSource("client_tags")
                .setClientTags(clientTags)
                .build();
        QueryId queryId = createQuery(queryRunner, session, LONG_LASTING_QUERY);
        waitForQueryState(queryRunner, queryId, ImmutableSet.of(RUNNING, FINISHED));
        Optional<ResourceGroupId> resourceGroupId = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getResourceGroupId();
        assertThat(resourceGroupId.isPresent())
                .describedAs("Query should have a resource group")
                .isTrue();
        assertThat(resourceGroupId.get())
                .describedAs(format("Expected: '%s' resource group, found: %s", expectedResourceGroup, resourceGroupId.get()))
                .isEqualTo(expectedResourceGroup);
    }
}
