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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.dispatcher.DispatchManager;
import io.trino.plugin.resourcegroups.ResourceGroupManagerPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.QueryId;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.Identity;
import io.trino.spi.session.ResourceEstimates;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;
import java.util.Set;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.Unit.TERABYTE;
import static io.trino.SystemSessionProperties.MAX_HASH_PARTITION_COUNT;
import static io.trino.execution.QueryRunnerUtil.cancelQuery;
import static io.trino.execution.QueryRunnerUtil.createQuery;
import static io.trino.execution.QueryRunnerUtil.waitForQueryState;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.execution.QueryState.FINISHING;
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // run single threaded to avoid creating multiple query runners at once
public class TestQueues
{
    private static final String LONG_LASTING_QUERY = "SELECT COUNT(*) FROM lineitem";

    @Test
    @Timeout(240)
    public void testResourceGroupManager()
            throws Exception
    {
        try (QueryRunner queryRunner = createQueryRunner()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

            // submit first "dashboard" query
            QueryId firstDashboardQuery = createDashboardQuery(queryRunner);

            // wait for the first "dashboard" query to start
            waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);

            // submit second "dashboard" query
            QueryId secondDashboardQuery = createDashboardQuery(queryRunner);

            // wait for the second "dashboard" query to be queued ("dashboard.${USER}" queue strategy only allows one "dashboard" query to be accepted for execution)
            waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);

            // submit first non "dashboard" query
            QueryId firstNonDashboardQuery = createAdHocQuery(queryRunner);

            // wait for the first non "dashboard" query to start
            waitForQueryState(queryRunner, firstNonDashboardQuery, RUNNING);

            // submit second non "dashboard" query
            QueryId secondNonDashboardQuery = createAdHocQuery(queryRunner);

            // wait for the second non "dashboard" query to start
            waitForQueryState(queryRunner, secondNonDashboardQuery, RUNNING);

            // cancel first "dashboard" query, second "dashboard" query and second non "dashboard" query should start running
            cancelQuery(queryRunner, firstDashboardQuery);
            waitForQueryState(queryRunner, firstDashboardQuery, FAILED);
            waitForQueryState(queryRunner, secondDashboardQuery, RUNNING);
        }
    }

    @Test
    @Timeout(240)
    public void testExceedSoftLimits()
            throws Exception
    {
        try (QueryRunner queryRunner = createQueryRunner()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_soft_limits.json")));

            QueryId scheduled1 = createScheduledQuery(queryRunner);
            waitForQueryState(queryRunner, scheduled1, RUNNING);

            QueryId scheduled2 = createScheduledQuery(queryRunner);
            waitForQueryState(queryRunner, scheduled2, RUNNING);

            QueryId scheduled3 = createScheduledQuery(queryRunner);
            waitForQueryState(queryRunner, scheduled3, RUNNING);

            // cluster is now 'at capacity' - scheduled is running 3 (i.e. over soft limit)

            QueryId backfill1 = createBackfill(queryRunner);
            QueryId scheduled4 = createScheduledQuery(queryRunner);

            cancelQuery(queryRunner, scheduled1);

            // backfill should be chosen to run next
            waitForQueryState(queryRunner, backfill1, RUNNING);

            cancelQuery(queryRunner, scheduled2);
            cancelQuery(queryRunner, scheduled3);
            cancelQuery(queryRunner, scheduled4);

            QueryId backfill2 = createBackfill(queryRunner);
            waitForQueryState(queryRunner, backfill2, RUNNING);

            QueryId backfill3 = createBackfill(queryRunner);
            waitForQueryState(queryRunner, backfill3, RUNNING);

            // cluster is now 'at capacity' - backfills is running 3 (i.e. over soft limit)

            QueryId backfill4 = createBackfill(queryRunner);
            QueryId scheduled5 = createScheduledQuery(queryRunner);
            cancelQuery(queryRunner, backfill1);

            // scheduled should be chosen to run next
            waitForQueryState(queryRunner, scheduled5, RUNNING);
            cancelQuery(queryRunner, backfill2);
            cancelQuery(queryRunner, backfill3);
            cancelQuery(queryRunner, backfill4);
            cancelQuery(queryRunner, scheduled5);

            waitForQueryState(queryRunner, scheduled5, FAILED);
        }
    }

    private QueryId createBackfill(QueryRunner queryRunner)
    {
        return createQuery(queryRunner, newSession("backfill", ImmutableSet.of(), null), LONG_LASTING_QUERY);
    }

    private QueryId createScheduledQuery(QueryRunner queryRunner)
    {
        return createQuery(queryRunner, newSession("scheduled", ImmutableSet.of(), null), LONG_LASTING_QUERY);
    }

    @Test
    @Timeout(240)
    public void testResourceGroupManagerWithTwoDashboardQueriesRequestedAtTheSameTime()
            throws Exception
    {
        try (QueryRunner queryRunner = createQueryRunner()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

            QueryId firstDashboardQuery = createDashboardQuery(queryRunner);
            QueryId secondDashboardQuery = createDashboardQuery(queryRunner);

            Set<QueryState> queuedOrRunning = ImmutableSet.of(QUEUED, RUNNING);
            waitForQueryState(queryRunner, firstDashboardQuery, queuedOrRunning);
            waitForQueryState(queryRunner, secondDashboardQuery, queuedOrRunning);
        }
    }

    @Test
    @Timeout(240)
    public void testResourceGroupManagerWithTooManyQueriesScheduled()
            throws Exception
    {
        try (QueryRunner queryRunner = createQueryRunner()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

            QueryId firstDashboardQuery = createDashboardQuery(queryRunner);
            waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);

            QueryId secondDashboardQuery = createDashboardQuery(queryRunner);
            waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);

            QueryId thirdDashboardQuery = createDashboardQuery(queryRunner);
            waitForQueryState(queryRunner, thirdDashboardQuery, FAILED);
        }
    }

    @Test
    @Timeout(240)
    public void testResourceGroupManagerRejection()
            throws Exception
    {
        testRejection();
    }

    @Test
    @Timeout(240)
    public void testClientTagsBasedSelection()
            throws Exception
    {
        try (QueryRunner queryRunner = createQueryRunner()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get()
                    .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_client_tags_based_config.json")));
            assertResourceGroup(queryRunner, newSessionWithTags(ImmutableSet.of("a")), LONG_LASTING_QUERY, createResourceGroupId("global", "a", "default"));
            assertResourceGroup(queryRunner, newSessionWithTags(ImmutableSet.of("b")), LONG_LASTING_QUERY, createResourceGroupId("global", "b"));
            assertResourceGroup(queryRunner, newSessionWithTags(ImmutableSet.of("a", "c")), LONG_LASTING_QUERY, createResourceGroupId("global", "a", "c"));
        }
    }

    @Test
    public void testOriginalUserBasedSelection()
            throws Exception
    {
        try (QueryRunner queryRunner = createQueryRunner()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get()
                    .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_original_user.json")));
            // match both
            assertResourceGroup(queryRunner, newSessionWithUsers("usr-foo", Optional.of("usr-foo-original")), LONG_LASTING_QUERY, createResourceGroupId("global", "a"));
            // match only "user"
            assertResourceGroup(queryRunner, newSessionWithUsers("usr-foo", Optional.empty()), LONG_LASTING_QUERY, createResourceGroupId("global", "c"));
            assertResourceGroup(queryRunner, newSessionWithUsers("usr-foo", Optional.of("other")), LONG_LASTING_QUERY, createResourceGroupId("global", "c"));
            // match only "originalUser"
            assertResourceGroup(queryRunner, newSessionWithUsers("other", Optional.of("usr-foo-original")), LONG_LASTING_QUERY, createResourceGroupId("global", "b"));
        }
    }

    @Test
    @Timeout(240)
    public void testSelectorResourceEstimateBasedSelection()
            throws Exception
    {
        try (QueryRunner queryRunner = createQueryRunner()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get()
                    .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_resource_estimate_based_config.json")));

            assertResourceGroup(
                    queryRunner,
                    newSessionWithResourceEstimates(new ResourceEstimates(
                            Optional.of(java.time.Duration.ofMinutes(4)),
                            Optional.empty(),
                            Optional.of(DataSize.of(400, MEGABYTE).toBytes()))),
                    LONG_LASTING_QUERY,
                    createResourceGroupId("global", "small"));

            assertResourceGroup(
                    queryRunner,
                    newSessionWithResourceEstimates(new ResourceEstimates(
                            Optional.of(java.time.Duration.ofMinutes(4)),
                            Optional.empty(),
                            Optional.of(DataSize.of(600, MEGABYTE).toBytes()))),
                    LONG_LASTING_QUERY,
                    createResourceGroupId("global", "other"));

            assertResourceGroup(
                    queryRunner,
                    newSessionWithResourceEstimates(new ResourceEstimates(
                            Optional.of(java.time.Duration.ofMinutes(4)),
                            Optional.empty(),
                            Optional.empty())),
                    LONG_LASTING_QUERY,
                    createResourceGroupId("global", "other"));

            assertResourceGroup(
                    queryRunner,
                    newSessionWithResourceEstimates(new ResourceEstimates(
                            Optional.of(java.time.Duration.ofSeconds(1)),
                            Optional.of(java.time.Duration.ofSeconds(1)),
                            Optional.of(DataSize.of(6, TERABYTE).toBytes()))),
                    LONG_LASTING_QUERY,
                    createResourceGroupId("global", "huge_memory"));

            assertResourceGroup(
                    queryRunner,
                    newSessionWithResourceEstimates(new ResourceEstimates(
                            Optional.of(java.time.Duration.ofHours(100)),
                            Optional.empty(),
                            Optional.of(DataSize.of(4, TERABYTE).toBytes()))),
                    LONG_LASTING_QUERY,
                    createResourceGroupId("global", "other"));
        }
    }

    @Test
    @Timeout(240)
    public void testQueryTypeBasedSelection()
            throws Exception
    {
        try (QueryRunner queryRunner = TpchQueryRunner.builder().build()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get()
                    .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_query_type_based_config.json")));
            assertResourceGroup(queryRunner, newAdhocSession(), LONG_LASTING_QUERY, createResourceGroupId("global", "select"));
            assertResourceGroup(queryRunner, newAdhocSession(), "SHOW TABLES", createResourceGroupId("global", "describe"));
            assertResourceGroup(queryRunner, newAdhocSession(), "EXPLAIN " + LONG_LASTING_QUERY, createResourceGroupId("global", "explain"));
            assertResourceGroup(queryRunner, newAdhocSession(), "DESCRIBE lineitem", createResourceGroupId("global", "describe"));
            assertResourceGroup(queryRunner, newAdhocSession(), "RESET SESSION " + MAX_HASH_PARTITION_COUNT, createResourceGroupId("global", "data_definition"));
        }
    }

    private void assertResourceGroup(QueryRunner queryRunner, Session session, String query, ResourceGroupId expectedResourceGroup)
            throws InterruptedException
    {
        QueryId queryId = createQuery(queryRunner, session, query);
        waitForQueryState(queryRunner, queryId, ImmutableSet.of(RUNNING, FINISHING, FINISHED));
        Optional<ResourceGroupId> resourceGroupId = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getResourceGroupId();
        assertThat(resourceGroupId.isPresent())
                .describedAs("Query should have a resource group")
                .isTrue();
        assertThat(resourceGroupId.get())
                .describedAs(format("Expected: '%s' resource group, found: %s", expectedResourceGroup, resourceGroupId.get()))
                .isEqualTo(expectedResourceGroup);
    }

    private void testRejection()
            throws Exception
    {
        try (QueryRunner queryRunner = createQueryRunner()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

            QueryId queryId = createQuery(queryRunner, newRejectionSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, queryId, FAILED);
            DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
            assertThat(dispatchManager.getQueryInfo(queryId).getErrorCode()).isEqualTo(QUERY_REJECTED.toErrorCode());
        }
    }

    private static QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build())
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    private QueryId createDashboardQuery(QueryRunner queryRunner)
    {
        return createQuery(queryRunner, newSession("dashboard", ImmutableSet.of(), null), LONG_LASTING_QUERY);
    }

    private QueryId createAdHocQuery(QueryRunner queryRunner)
    {
        return createQuery(queryRunner, newAdhocSession(), LONG_LASTING_QUERY);
    }

    private static Session newAdhocSession()
    {
        return newSession("adhoc", ImmutableSet.of(), null);
    }

    private static Session newRejectionSession()
    {
        return newSession("reject", ImmutableSet.of(), null);
    }

    private static Session newSessionWithTags(Set<String> clientTags)
    {
        return newSession("sessionWithTags", clientTags, null);
    }

    private static Session newSessionWithResourceEstimates(ResourceEstimates resourceEstimates)
    {
        return newSession("sessionWithTags", ImmutableSet.of(), resourceEstimates);
    }

    private static Session newSessionWithUsers(String user, Optional<String> originalUser)
    {
        Session.SessionBuilder builder = newSessionBuilder()
                .setIdentity(Identity.ofUser(user));
        originalUser.ifPresent(usr -> builder.setOriginalIdentity(Identity.ofUser(usr)));
        return builder.build();
    }

    private static Session newSession(String source, Set<String> clientTags, ResourceEstimates resourceEstimates)
    {
        return newSessionBuilder()
                .setSource(source)
                .setClientTags(clientTags)
                .setResourceEstimates(resourceEstimates)
                .build();
    }

    private static Session.SessionBuilder newSessionBuilder()
    {
        return testSessionBuilder().setCatalog("tpch").setSchema("sf100000");
    }

    public static ResourceGroupId createResourceGroupId(String root, String... subGroups)
    {
        return new ResourceGroupId(ImmutableList.<String>builder()
                .add(requireNonNull(root, "root is null"))
                .addAll(asList(subGroups))
                .build());
    }
}
