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
package io.trino.plugin.resourcegroups.db;

import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.execution.resourcegroups.InternalResourceGroup;
import io.trino.plugin.resourcegroups.ResourceGroupIdTemplate;
import io.trino.plugin.resourcegroups.ResourceGroupSelector;
import io.trino.plugin.resourcegroups.ResourceGroupSpec;
import io.trino.plugin.resourcegroups.StaticSelector;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.resourcegroups.SchedulingPolicy;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;
import io.trino.spi.session.ResourceEstimates;
import org.h2.jdbc.JdbcException;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.execution.resourcegroups.InternalResourceGroup.DEFAULT_WEIGHT;
import static io.trino.spi.resourcegroups.SchedulingPolicy.FAIR;
import static io.trino.spi.resourcegroups.SchedulingPolicy.WEIGHTED;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDbResourceGroupConfigurationManager
{
    private static final String ENVIRONMENT = "test";
    private static final ResourceEstimates EMPTY_RESOURCE_ESTIMATES = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty());

    static H2DaoProvider setup(String prefix)
    {
        DbResourceGroupConfig config = new DbResourceGroupConfig().setConfigDbUrl("jdbc:h2:mem:test_" + prefix + System.nanoTime() + ThreadLocalRandom.current().nextLong() + ";NON_KEYWORDS=KEY,VALUE");
        return new H2DaoProvider(config);
    }

    @Test
    public void testEnvironments()
    {
        H2DaoProvider daoProvider = setup("test_configuration");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        String prodEnvironment = "prod";
        String devEnvironment = "dev";
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        // two resource groups are the same except the group for the prod environment has a larger softMemoryLimit
        dao.insertResourceGroup(1, "prod_global", "10MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, prodEnvironment);
        dao.insertResourceGroup(2, "dev_global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, devEnvironment);
        dao.insertSelector(1, 1, ".*prod_user.*", null, null, null, null, null, null, null);
        dao.insertSelector(2, 2, ".*dev_user.*", null, null, null, null, null, null, null);

        // check the prod configuration
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(listener -> {}, new DbResourceGroupConfig(), daoProvider.get(), prodEnvironment);
        List<ResourceGroupSpec> groups = manager.getRootGroups();
        assertThat(groups).hasSize(1);
        InternalResourceGroup prodGlobal = new InternalResourceGroup("prod_global", (group, export) -> {}, directExecutor());
        manager.configure(prodGlobal, new SelectionContext<>(prodGlobal.getId(), new ResourceGroupIdTemplate("prod_global")));
        assertEqualsResourceGroup(prodGlobal, "10MB", 1000, 100, 100, WEIGHTED, DEFAULT_WEIGHT, true, Duration.ofHours(1), Duration.ofDays(1));
        assertThat(manager.getSelectors()).hasSize(1);
        ResourceGroupSelector prodSelector = manager.getSelectors().get(0);
        ResourceGroupId prodResourceGroupId = prodSelector.match(new SelectionCriteria(true, "prod_user", ImmutableSet.of(), "prod_user", Optional.empty(), Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.empty())).get().getResourceGroupId();
        assertThat(prodResourceGroupId.toString()).isEqualTo("prod_global");

        // check the dev configuration
        manager = new DbResourceGroupConfigurationManager(listener -> {}, new DbResourceGroupConfig(), daoProvider.get(), devEnvironment);
        assertThat(groups).hasSize(1);
        InternalResourceGroup devGlobal = new InternalResourceGroup("dev_global", (group, export) -> {}, directExecutor());
        manager.configure(devGlobal, new SelectionContext<>(prodGlobal.getId(), new ResourceGroupIdTemplate("dev_global")));
        assertEqualsResourceGroup(devGlobal, "1MB", 1000, 100, 100, WEIGHTED, DEFAULT_WEIGHT, true, Duration.ofHours(1), Duration.ofDays(1));
        assertThat(manager.getSelectors()).hasSize(1);
        ResourceGroupSelector devSelector = manager.getSelectors().get(0);
        ResourceGroupId devResourceGroupId = devSelector.match(new SelectionCriteria(true, "dev_user", ImmutableSet.of(), "dev_user", Optional.empty(), Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.empty())).get().getResourceGroupId();
        assertThat(devResourceGroupId.toString()).isEqualTo("dev_global");
    }

    @Test
    public void testConfiguration()
    {
        H2DaoProvider daoProvider = setup("test_configuration");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, 1L, ENVIRONMENT);
        dao.insertSelector(2, 1, null, null, null, null, null, null, null, null);
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(listener -> {}, new DbResourceGroupConfig(), daoProvider.get(), ENVIRONMENT);
        InternalResourceGroup global = new InternalResourceGroup("global", (group, export) -> {}, directExecutor());
        manager.configure(global, new SelectionContext<>(global.getId(), new ResourceGroupIdTemplate("global")));
        assertEqualsResourceGroup(global, "1MB", 1000, 100, 100, WEIGHTED, DEFAULT_WEIGHT, true, Duration.ofHours(1), Duration.ofDays(1));
        InternalResourceGroup sub = global.getOrCreateSubGroup("sub");
        manager.configure(sub, new SelectionContext<>(sub.getId(), new ResourceGroupIdTemplate("global.sub")));
        assertEqualsResourceGroup(sub, "2MB", 4, 3, 3, FAIR, 5, false, Duration.ofMillis(Long.MAX_VALUE), Duration.ofMillis(Long.MAX_VALUE));
    }

    @Test
    public void testDuplicateRoots()
    {
        H2DaoProvider daoProvider = setup("test_dup_roots");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, null, null, null, null, null, null, ENVIRONMENT);
        assertThatThrownBy(() -> dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, null, null, null, null, null, null, ENVIRONMENT))
                .isInstanceOfSatisfying(UnableToExecuteStatementException.class, ex -> {
                    assertThat(ex.getCause()).isInstanceOf(JdbcException.class);
                    assertThat(ex.getCause().getMessage()).startsWith("Unique index or primary key violation");
                });
        dao.insertSelector(1, 1, null, null, null, null, null, null, null, null);
    }

    @Test
    public void testDuplicateGroups()
    {
        H2DaoProvider daoProvider = setup("test_dup_subs");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, null, null, null, null, null, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "1MB", 1000, 100, 100, null, null, null, null, null, 1L, ENVIRONMENT);
        assertThatThrownBy(() -> dao.insertResourceGroup(2, "sub", "1MB", 1000, 100, 100, null, null, null, null, null, 1L, ENVIRONMENT))
                .isInstanceOfSatisfying(UnableToExecuteStatementException.class, ex -> {
                    assertThat(ex.getCause()).isInstanceOf(JdbcException.class);
                    assertThat(ex.getCause().getMessage()).startsWith("Unique index or primary key violation");
                });
        dao.insertSelector(2, 2, null, null, null, null, null, null, null, null);
    }

    @Test
    public void testMissing()
    {
        H2DaoProvider daoProvider = setup("test_missing");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, 1L, ENVIRONMENT);
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        dao.insertSelector(2, 1, null, null, null, null, null, null, null, null);
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(listener -> {}, new DbResourceGroupConfig(), daoProvider.get(), ENVIRONMENT);
        InternalResourceGroup missing = new InternalResourceGroup("missing", (group, export) -> {}, directExecutor());

        assertThatThrownBy(() -> manager.configure(missing, new SelectionContext<>(missing.getId(), new ResourceGroupIdTemplate("missing"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("No matching configuration found for [missing] using [missing]");
    }

    @Test
    @Timeout(60)
    public void testReconfig()
            throws Exception
    {
        H2DaoProvider daoProvider = setup("test_reconfig");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, 1L, ENVIRONMENT);
        dao.insertSelector(2, 1, null, null, null, null, null, null, null, null);
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(listener -> {}, new DbResourceGroupConfig(), daoProvider.get(), ENVIRONMENT);
        manager.start();
        InternalResourceGroup global = new InternalResourceGroup("global", (group, export) -> {}, directExecutor());
        manager.configure(global, new SelectionContext<>(global.getId(), new ResourceGroupIdTemplate("global")));
        InternalResourceGroup globalSub = global.getOrCreateSubGroup("sub");
        manager.configure(globalSub, new SelectionContext<>(globalSub.getId(), new ResourceGroupIdTemplate("global.sub")));
        // Verify record exists
        assertEqualsResourceGroup(globalSub, "2MB", 4, 3, 3, FAIR, 5, false, Duration.ofMillis(Long.MAX_VALUE), Duration.ofMillis(Long.MAX_VALUE));
        dao.updateResourceGroup(2, "sub", "3MB", 2, 1, 1, "weighted", 6, true, "1h", "1d", 1L, ENVIRONMENT);
        do {
            MILLISECONDS.sleep(500);
        }
        while (globalSub.getJmxExport() == false);
        // Verify update
        assertEqualsResourceGroup(globalSub, "3MB", 2, 1, 1, WEIGHTED, 6, true, Duration.ofHours(1), Duration.ofDays(1));
        // Verify delete
        dao.deleteSelectors(2);
        dao.deleteResourceGroup(2);
        do {
            MILLISECONDS.sleep(500);
        }
        while (!globalSub.isDisabled());
    }

    @Test
    public void testExactMatchSelector()
    {
        H2DaoProvider daoProvider = setup("test_exact_match_selector");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.createExactMatchSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, 1L, ENVIRONMENT);
        dao.insertSelector(2, 1, null, null, null, null, null, null, null, null);
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        DbResourceGroupConfig config = new DbResourceGroupConfig();
        config.setExactMatchSelectorEnabled(true);
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(listener -> {}, config, daoProvider.get(), ENVIRONMENT);
        manager.load();
        assertThat(manager.getSelectors()).hasSize(2);
        assertThat(manager.getSelectors().get(0)).isInstanceOf(DbSourceExactMatchSelector.class);

        config.setExactMatchSelectorEnabled(false);
        manager = new DbResourceGroupConfigurationManager(listener -> {}, config, daoProvider.get(), ENVIRONMENT);
        manager.load();
        assertThat(manager.getSelectors()).hasSize(1);
        assertThat(manager.getSelectors().get(0) instanceof DbSourceExactMatchSelector).isFalse();
    }

    @Test
    public void testSelectorPriority()
    {
        H2DaoProvider daoProvider = setup("selectors");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "100%", 100, 100, 100, null, null, null, null, null, null, ENVIRONMENT);

        final int numberOfUsers = 100;
        List<String> expectedUsers = new ArrayList<>();

        int[] randomPriorities = ThreadLocalRandom.current()
                .ints(0, 1000)
                .distinct()
                .limit(numberOfUsers)
                .toArray();

        // insert several selectors with unique random priority where userRegex is equal to the priority
        for (int i = 0; i < numberOfUsers; i++) {
            int priority = randomPriorities[i];
            String user = String.valueOf(priority);
            dao.insertSelector(1, priority, user, null, null, null, ".*", null, null, null);
            expectedUsers.add(user);
        }

        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(listener -> {}, new DbResourceGroupConfig(), daoProvider.get(), ENVIRONMENT);
        manager.load();

        List<ResourceGroupSelector> selectors = manager.getSelectors();
        assertThat(selectors).hasSize(expectedUsers.size());

        // when we load the selectors we expect the selector list to be ordered by priority
        expectedUsers.sort(Comparator.<String>comparingInt(Integer::parseInt).reversed());

        for (int i = 0; i < numberOfUsers; i++) {
            Optional<Pattern> user = ((StaticSelector) selectors.get(i)).getUserRegex();
            assertThat(user).isPresent();
            assertThat(user.get().pattern()).isEqualTo(expectedUsers.get(i));
        }
    }

    @Test
    public void testInvalidConfiguration()
    {
        H2DaoProvider daoProvider = setup("selectors");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "100%", 100, 100, 100, null, null, null, null, null, null, ENVIRONMENT);

        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(
                listener -> {},
                new DbResourceGroupConfig().setMaxRefreshInterval(new io.airlift.units.Duration(2, MILLISECONDS)).setRefreshInterval(new io.airlift.units.Duration(1, MILLISECONDS)),
                daoProvider.get(),
                ENVIRONMENT);

        assertThatThrownBy(manager::getSelectors)
                .isInstanceOf(TrinoException.class)
                .hasMessage("No selectors are configured");
    }

    @Test
    public void testRefreshInterval()
    {
        H2DaoProvider daoProvider = setup("selectors");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "100%", 100, 100, 100, null, null, null, null, null, null, ENVIRONMENT);

        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(
                listener -> {},
                new DbResourceGroupConfig().setMaxRefreshInterval(new io.airlift.units.Duration(2, MILLISECONDS)).setRefreshInterval(new io.airlift.units.Duration(1, MILLISECONDS)),
                daoProvider.get(),
                ENVIRONMENT);

        dao.dropSelectorsTable();
        manager.load();

        assertTrinoExceptionThrownBy(manager::getSelectors)
                .hasMessage("Selectors cannot be fetched from database");

        assertTrinoExceptionThrownBy(manager::getRootGroups)
                .hasMessage("Root groups cannot be fetched from database");

        manager.destroy();
    }

    @Test
    public void testMatchByUserGroups()
    {
        H2DaoProvider daoProvider = setup("selectors");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "group", "100%", 100, 100, 100, null, null, null, null, null, null, ENVIRONMENT);
        dao.insertSelector(1, 1, null, "first matching|second matching", null, null, null, null, null, null);

        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(
                listener -> {},
                new DbResourceGroupConfig().setMaxRefreshInterval(new io.airlift.units.Duration(2, MILLISECONDS)).setRefreshInterval(new io.airlift.units.Duration(1, MILLISECONDS)),
                daoProvider.get(),
                ENVIRONMENT);

        assertThat(manager.match(userGroupsSelectionCriteria("not matching"))).isEmpty();
        assertThat(manager.match(userGroupsSelectionCriteria("first matching")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("group")));
    }

    @Test
    public void testMatchByUsersAndGroups()
    {
        H2DaoProvider daoProvider = setup("selectors");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "group", "100%", 100, 100, 100, null, null, null, null, null, null, ENVIRONMENT);
        dao.insertSelector(1, 1, "Matching user", "Matching group", null, null, null, null, null, null);

        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(
                listener -> {},
                new DbResourceGroupConfig().setMaxRefreshInterval(new io.airlift.units.Duration(2, MILLISECONDS)).setRefreshInterval(new io.airlift.units.Duration(1, MILLISECONDS)),
                daoProvider.get(),
                ENVIRONMENT);

        assertThat(manager.match(userAndUserGroupsSelectionCriteria("Matching user", "Not matching group"))).isEmpty();
        assertThat(manager.match(userAndUserGroupsSelectionCriteria("Not matching user", "Matching group"))).isEmpty();
        assertThat(manager.match(userAndUserGroupsSelectionCriteria("Matching user", "Matching group")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("group")));
    }

    @Test
    public void testMatchByOriginalUser()
    {
        H2DaoProvider daoProvider = setup("selectors");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "group", "100%", 100, 100, 100, null, null, null, null, null, null, ENVIRONMENT);
        dao.insertSelector(1, 1, null, null, "foo.+", null, null, null, null, null);

        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(
                listener -> {},
                new DbResourceGroupConfig().setMaxRefreshInterval(new io.airlift.units.Duration(2, MILLISECONDS)).setRefreshInterval(new io.airlift.units.Duration(1, MILLISECONDS)),
                daoProvider.get(),
                ENVIRONMENT);

        assertThat(manager.match(identitySelectionCriteria("foo-usr", "other-usr", Optional.empty()))).isEmpty();
        assertThat(manager.match(identitySelectionCriteria("foo-usr", "other-usr", Optional.of("foo-usr")))).isEmpty();
        assertThat(manager.match(identitySelectionCriteria("other-usr", "foo-usr", Optional.empty())))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("group")));
    }

    @Test
    public void testMatchByAuthenticatedUser()
    {
        H2DaoProvider daoProvider = setup("selectors");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "group", "100%", 100, 100, 100, null, null, null, null, null, null, ENVIRONMENT);
        dao.insertSelector(1, 1, null, null, null, "foo.+", null, null, null, null);

        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(
                listener -> {},
                new DbResourceGroupConfig().setMaxRefreshInterval(new io.airlift.units.Duration(2, MILLISECONDS)).setRefreshInterval(new io.airlift.units.Duration(1, MILLISECONDS)),
                daoProvider.get(),
                ENVIRONMENT);

        assertThat(manager.match(identitySelectionCriteria("foo-usr", "foo-usr", Optional.empty()))).isEmpty();
        assertThat(manager.match(identitySelectionCriteria("foo-usr", "foo-usr", Optional.of("other-usr")))).isEmpty();
        assertThat(manager.match(identitySelectionCriteria("other-usr", "other-usr", Optional.of("foo-usr"))))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("group")));
    }

    @RepeatedTest(10)
    public void testConfigurationUpdateIsNotLost()
    {
        // This test attempts to reproduce the following sequence:
        // 1. Load resource group configuration C1, which includes template T1.
        // 2. For query Q1, select template T1 and expand it into resource group R1.
        // 3. For query Q1, obtain C1 in DbResourceGroupConfigurationManager#configure.
        // 4. Load resource group configuration C2 with modified parameters for T1.
        //
        // If everything works correctly, C2 should eventually be applied to R1.
        // We want to avoid the following scenarios:
        // - C1, obtained in step 3, overwrites C2 applied to R1 in step 4, and no subsequent
        //   'load' applies C2 again.
        // - The 'load' in step 4 doesn't apply C2 to R1 because 'configure' hasn't created a
        //   mapping between T1 and R1 yet, and no subsequent 'load' detects the configuration change for T1.

        H2DaoProvider daoProvider = setup("test_lost_update");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "80%", 10, null, 1, null, null, null, null, null, null, ENVIRONMENT);
        dao.insertSelector(1, 1, null, "userGroup", null, null, null, null, null, null);
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(_ -> {}, new DbResourceGroupConfig(), daoProvider.get(), ENVIRONMENT);

        Optional<SelectionContext<ResourceGroupIdTemplate>> userGroup = manager.match(userGroupsSelectionCriteria("userGroup"));
        assertThat(userGroup.isPresent()).isTrue();
        SelectionContext<ResourceGroupIdTemplate> selectionContext = userGroup.get();

        InternalResourceGroup resourceGroup = new InternalResourceGroup("global", (_, _) -> {}, directExecutor());

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            dao.updateResourceGroup(1, "global", "80%", 10, null, 10, null, null, null, null, null, null, ENVIRONMENT);

            executor.submit(() -> {
                synchronized (resourceGroup) {
                    // Wait while holding the lock to increase the likelihood that 'load' and 'configure'
                    // will attempt to update the configuration simultaneously. Both need to acquire this
                    // lock to update the resource group.
                    Thread.sleep(10);
                }
                return null;
            });

            executor.submit(manager::load);

            manager.configure(resourceGroup, selectionContext);

            assertEventually(() -> {
                manager.load();
                assertThat(resourceGroup.getHardConcurrencyLimit()).isEqualTo(10);
            });
        }
    }

    private static void assertEqualsResourceGroup(
            InternalResourceGroup group,
            String softMemoryLimit,
            int maxQueued,
            int hardConcurrencyLimit,
            int softConcurrencyLimit,
            SchedulingPolicy schedulingPolicy,
            int schedulingWeight,
            boolean jmxExport,
            Duration softCpuLimit,
            Duration hardCpuLimit)
    {
        assertThat(group.getSoftMemoryLimitBytes()).isEqualTo(DataSize.valueOf(softMemoryLimit).toBytes());
        assertThat(group.getMaxQueuedQueries()).isEqualTo(maxQueued);
        assertThat(group.getHardConcurrencyLimit()).isEqualTo(hardConcurrencyLimit);
        assertThat(group.getSoftConcurrencyLimit()).isEqualTo(softConcurrencyLimit);
        assertThat(group.getSchedulingPolicy()).isEqualTo(schedulingPolicy);
        assertThat(group.getSchedulingWeight()).isEqualTo(schedulingWeight);
        assertThat(group.getJmxExport()).isEqualTo(jmxExport);
        assertThat(group.getSoftCpuLimit()).isEqualTo(softCpuLimit);
        assertThat(group.getHardCpuLimit()).isEqualTo(hardCpuLimit);
    }

    private static SelectionCriteria userGroupsSelectionCriteria(String... groups)
    {
        return new SelectionCriteria(true, "test_user", ImmutableSet.copyOf(groups), "test_user", Optional.empty(), Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.empty());
    }

    private static SelectionCriteria userAndUserGroupsSelectionCriteria(String user, String group, String... groups)
    {
        return new SelectionCriteria(
                true,
                user,
                ImmutableSet.<String>builder()
                        .add(group)
                        .add(groups).build(),
                user,
                Optional.empty(),
                Optional.empty(),
                ImmutableSet.of(),
                EMPTY_RESOURCE_ESTIMATES,
                Optional.empty());
    }

    private static SelectionCriteria identitySelectionCriteria(String user, String originalUser, Optional<String> authenticatedUser)
    {
        return new SelectionCriteria(true, user, ImmutableSet.of(), originalUser, authenticatedUser, Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.empty());
    }
}
