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
package io.trino.plugin.resourcegroups;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.spi.memory.MemoryPoolInfo;
import io.trino.spi.resourcegroups.ResourceGroup;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;
import io.trino.spi.session.ResourceEstimates;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.io.Resources.getResource;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.resourcegroups.TestingResourceGroups.groupIdTemplate;
import static io.trino.plugin.resourcegroups.TestingResourceGroups.managerSpec;
import static io.trino.plugin.resourcegroups.TestingResourceGroups.resourceGroupSpec;
import static io.trino.plugin.resourcegroups.TestingResourceGroups.selectorSpec;
import static io.trino.spi.resourcegroups.SchedulingPolicy.WEIGHTED;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFileResourceGroupConfigurationManager
{
    private static final ResourceEstimates EMPTY_RESOURCE_ESTIMATES = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty());
    private static final long MEMORY_POOL_SIZE = 31415926535900L; // arbitrary uneven value for testing

    @Test
    public void testInvalid()
    {
        assertFails("resource_groups_config_bad_root.json", "Duplicated root group: global");
        assertFails("resource_groups_config_bad_sub_group.json", "Duplicated sub group: sub");
        assertFails("resource_groups_config_bad_group_id.json", "Invalid resource group name. 'glo.bal' contains a '.'");
        assertFails("resource_groups_config_bad_soft_memory_limit.json", "softMemoryLimit percentage is over 100%");
        assertFails("resource_groups_config_bad_weighted_scheduling_policy.json", "Must specify scheduling weight for all sub-groups of 'requests' or none of them");
        assertFails("resource_groups_config_unused_field.json", "Unknown property at line 8:6: maxFoo");
        assertFails("resource_groups_config_bad_query_priority_scheduling_policy.json",
                "Must use 'weighted' or 'weighted_fair' scheduling policy if specifying scheduling weight for 'requests'");
        assertFails("resource_groups_config_bad_extract_variable.json", "Invalid resource group name.*");
        assertFails("resource_groups_config_bad_query_type.json", "Selector specifies an invalid query type: invalid_query_type");
        assertFails("resource_groups_config_bad_selector.json", "Selector refers to nonexistent group: a.b.c.X");
    }

    @Test
    public void testQueryTypeConfiguration()
    {
        FileResourceGroupConfigurationManager manager = parse("resource_groups_config_query_type.json");
        assertMatch(manager, queryTypeSelectionCriteria("select"), "global.select");
        assertMatch(manager, queryTypeSelectionCriteria("explain"), "global.explain");
        assertMatch(manager, queryTypeSelectionCriteria("insert"), "global.insert");
        assertMatch(manager, queryTypeSelectionCriteria("delete"), "global.delete");
        assertMatch(manager, queryTypeSelectionCriteria("describe"), "global.describe");
        assertMatch(manager, queryTypeSelectionCriteria("data_definition"), "global.data_definition");
        assertMatch(manager, queryTypeSelectionCriteria("sth_else"), "global.other");
    }

    @Test
    public void testMatchByUserGroups()
    {
        ManagerSpec managerSpec = managerSpec(
                resourceGroupSpec("group"),
                ImmutableList.of(selectorSpec(groupIdTemplate("group"))
                        .userGroups("first matching", "second matching")));

        FileResourceGroupConfigurationManager groupManager = new FileResourceGroupConfigurationManager(listener -> {}, managerSpec);

        assertThat(groupManager.match(userGroupsSelectionCriteria("not matching"))).isEmpty();
        assertThat(groupManager.match(userGroupsSelectionCriteria("first matching")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(groupIdTemplate("group")));
    }

    @Test
    public void testMatchByUsers()
    {
        ManagerSpec managerSpec = managerSpec(
                resourceGroupSpec("group"),
                ImmutableList.of(selectorSpec(groupIdTemplate("group"))
                        .users("First matching user", "Second matching user")));

        FileResourceGroupConfigurationManager groupManager = new FileResourceGroupConfigurationManager(listener -> {}, managerSpec);

        assertThat(groupManager.match(userSelectionCriteria("Not matching user"))).isEmpty();
        assertThat(groupManager.match(userSelectionCriteria("First matching user")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(groupIdTemplate("group")));
    }

    @Test
    public void testMatchByUsersAndGroups()
    {
        ManagerSpec managerSpec = managerSpec(
                resourceGroupSpec("group"),
                ImmutableList.of(selectorSpec(groupIdTemplate("group"))
                        .userGroups("Matching group")
                        .users("Matching user")));

        FileResourceGroupConfigurationManager groupManager = new FileResourceGroupConfigurationManager(listener -> {}, managerSpec);

        assertThat(groupManager.match(userAndUserGroupsSelectionCriteria("Matching user", "Not matching group"))).isEmpty();
        assertThat(groupManager.match(userAndUserGroupsSelectionCriteria("Not matching user", "Matching group"))).isEmpty();
        assertThat(groupManager.match(userAndUserGroupsSelectionCriteria("Matching user", "Matching group")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(groupIdTemplate("group")));
    }

    @Test
    public void testMatchByOriginalUser()
    {
        ManagerSpec managerSpec = managerSpec(
                resourceGroupSpec("group"),
                ImmutableList.of(selectorSpec(groupIdTemplate("group")).originalUserPattern("foo.+")));

        FileResourceGroupConfigurationManager manager = new FileResourceGroupConfigurationManager(listener -> {}, managerSpec);

        assertThat(manager.match(identitySelectionCriteria("foo-usr", "other-usr", Optional.empty()))).isEmpty();
        assertThat(manager.match(identitySelectionCriteria("foo-usr", "other-usr", Optional.of("foo-usr")))).isEmpty();
        assertThat(manager.match(identitySelectionCriteria("other-usr", "foo-usr", Optional.empty())))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(groupIdTemplate("group")));
    }

    @Test
    public void testMatchByAuthenticatedUser()
    {
        ManagerSpec managerSpec = managerSpec(
                resourceGroupSpec("group"),
                ImmutableList.of(selectorSpec(groupIdTemplate("group")).authenticatedUserPattern("foo.+")));

        FileResourceGroupConfigurationManager manager = new FileResourceGroupConfigurationManager(listener -> {}, managerSpec);

        assertThat(manager.match(identitySelectionCriteria("foo-usr", "foo-usr", Optional.empty()))).isEmpty();
        assertThat(manager.match(identitySelectionCriteria("foo-usr", "foo-usr", Optional.of("other-usr")))).isEmpty();
        assertThat(manager.match(identitySelectionCriteria("other-usr", "other-usr", Optional.of("foo-usr"))))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(groupIdTemplate("group")));
    }

    @Test
    public void testUserGroupsConfiguration()
    {
        ManagerSpec spec = parseManagerSpec("resource_groups_config_user_groups.json");

        assertThat(spec.getSelectors()
                .stream()
                .map(SelectorSpec::getUserGroupRegex)
                .map(pattern -> pattern.map(Pattern::pattern)))
                .containsOnly(Optional.of("groupA"));
    }

    @Test
    public void testOriginalAndAuthenticatedUserConfiguration()
    {
        ManagerSpec spec = parseManagerSpec("resource_groups_config_original_auth_user.json");

        assertThat(spec.getSelectors())
                .hasSize(2)
                .anySatisfy(selector -> {
                    assertThat(selector.getGroup()).hasToString("global.original");
                    assertThat(selector.getOriginalUserRegex().map(Pattern::pattern)).contains("usr-original");
                    assertThat(selector.getAuthenticatedUserRegex()).isEmpty();
                })
                .anySatisfy(selector -> {
                    assertThat(selector.getGroup()).hasToString("global.auth");
                    assertThat(selector.getOriginalUserRegex()).isEmpty();
                    assertThat(selector.getAuthenticatedUserRegex().map(Pattern::pattern)).contains("usr-auth");
                });
    }

    @Test
    public void testConfiguration()
    {
        FileResourceGroupConfigurationManager manager = parse("resource_groups_config.json");
        ResourceGroupId globalId = new ResourceGroupId("global");
        ResourceGroup global = new TestingResourceGroup(globalId);
        manager.configure(global, new SelectionContext<>(globalId, new ResourceGroupIdTemplate("global")));
        assertThat(global.getSoftMemoryLimitBytes()).isEqualTo(DataSize.of(1, MEGABYTE).toBytes());
        assertThat(global.getSoftCpuLimit()).isEqualTo(Duration.ofHours(1));
        assertThat(global.getHardCpuLimit()).isEqualTo(Duration.ofDays(1));
        assertThat(global.getCpuQuotaGenerationMillisPerSecond()).isEqualTo(1000 * 24);
        assertThat(global.getMaxQueuedQueries()).isEqualTo(1000);
        assertThat(global.getHardConcurrencyLimit()).isEqualTo(100);
        assertThat(global.getSchedulingPolicy()).isEqualTo(WEIGHTED);
        assertThat(global.getSchedulingWeight()).isEqualTo(0);
        assertThat(global.getJmxExport()).isTrue();

        ResourceGroupId subId = new ResourceGroupId(globalId, "sub");
        ResourceGroup sub = new TestingResourceGroup(subId);
        manager.configure(sub, new SelectionContext<>(subId, new ResourceGroupIdTemplate("global.sub")));
        assertThat(sub.getSoftMemoryLimitBytes()).isEqualTo(DataSize.of(2, MEGABYTE).toBytes());
        assertThat(sub.getHardConcurrencyLimit()).isEqualTo(3);
        assertThat(sub.getMaxQueuedQueries()).isEqualTo(4);
        assertThat(sub.getSchedulingPolicy()).isNull();
        assertThat(sub.getSchedulingWeight()).isEqualTo(5);
        assertThat(sub.getJmxExport()).isFalse();

        ResourceGroupId subIdNoSoftMemoryLimit = new ResourceGroupId(globalId, "sub_no_soft_memory_limit");
        ResourceGroup subNoSoftMemoryLimit = new TestingResourceGroup(subIdNoSoftMemoryLimit);
        manager.configure(subNoSoftMemoryLimit, new SelectionContext<>(subIdNoSoftMemoryLimit, new ResourceGroupIdTemplate("global.sub_no_soft_memory_limit")));
        assertThat(subNoSoftMemoryLimit.getSoftMemoryLimitBytes()).isEqualTo(MEMORY_POOL_SIZE);
        assertThat(subNoSoftMemoryLimit.getHardConcurrencyLimit()).isEqualTo(4);
        assertThat(subNoSoftMemoryLimit.getSchedulingWeight()).isEqualTo(1);
    }

    @Test
    public void testExtractVariableConfiguration()
    {
        FileResourceGroupConfigurationManager manager = parse("resource_groups_config_extract_variable.json");

        SelectionContext<ResourceGroupIdTemplate> selectionContext = match(manager, userAndSourceSelectionCriteria("someuser@presto.io", "scheduler.us_east.12"));
        assertThat(selectionContext.getResourceGroupId().toString()).isEqualTo("global.presto:us_east:12");
        TestingResourceGroup resourceGroup = new TestingResourceGroup(selectionContext.getResourceGroupId());
        manager.configure(resourceGroup, selectionContext);
        assertThat(resourceGroup.getHardConcurrencyLimit()).isEqualTo(3);

        selectionContext = match(manager, userAndSourceSelectionCriteria("nobody", "rg-abcdefghijkl"));
        assertThat(selectionContext.getResourceGroupId().toString()).isEqualTo("global.abcdefghijkl");
        resourceGroup = new TestingResourceGroup(selectionContext.getResourceGroupId());
        manager.configure(resourceGroup, selectionContext);
        assertThat(resourceGroup.getHardConcurrencyLimit()).isEqualTo(115);
    }

    @Test
    public void testDocsExample()
    {
        FileResourceGroupConfigurationManager manager = new FileResourceGroupConfigurationManager(
                (listener) -> listener.accept(new MemoryPoolInfo(MEMORY_POOL_SIZE, 0, 0, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of())),
                new FileResourceGroupConfig()
                        // TODO: figure out a better way to validate documentation
                        .setConfigFile("../../docs/src/main/sphinx/admin/resource-groups-example.json"));

        SelectionContext<ResourceGroupIdTemplate> selectionContext = match(manager, new SelectionCriteria(
                true,
                "Alice",
                ImmutableSet.of(),
                "Alice",
                Optional.empty(),
                Optional.of("jdbc#powerfulbi"),
                ImmutableSet.of("hipri"),
                EMPTY_RESOURCE_ESTIMATES,
                Optional.of("select")));
        assertThat(selectionContext.getResourceGroupId().toString()).isEqualTo("global.adhoc.bi-powerfulbi.Alice");
        TestingResourceGroup resourceGroup = new TestingResourceGroup(selectionContext.getResourceGroupId());
        manager.configure(resourceGroup, selectionContext);
        assertThat(resourceGroup.getHardConcurrencyLimit()).isEqualTo(3);
        assertThat(resourceGroup.getMaxQueuedQueries()).isEqualTo(10);
        assertThat(resourceGroup.getSoftMemoryLimitBytes()).isEqualTo(MEMORY_POOL_SIZE / 10);
    }

    @Test
    public void testLegacyConfiguration()
    {
        FileResourceGroupConfigurationManager manager = parse("resource_groups_config_legacy.json");
        ResourceGroupId globalId = new ResourceGroupId("global");
        ResourceGroup global = new TestingResourceGroup(globalId);
        manager.configure(global, new SelectionContext<>(globalId, new ResourceGroupIdTemplate("global")));
        assertThat(global.getSoftMemoryLimitBytes()).isEqualTo(DataSize.of(3, MEGABYTE).toBytes());
        assertThat(global.getMaxQueuedQueries()).isEqualTo(99);
        assertThat(global.getHardConcurrencyLimit()).isEqualTo(42);
    }

    private static void assertMatch(FileResourceGroupConfigurationManager manager, SelectionCriteria criteria, String expectedResourceGroup)
    {
        ResourceGroupId resourceGroupId = match(manager, criteria).getResourceGroupId();
        assertThat(resourceGroupId.toString())
                .describedAs(format("Expected: '%s' resource group, found: %s", expectedResourceGroup, resourceGroupId))
                .isEqualTo(expectedResourceGroup);
    }

    private static SelectionContext<ResourceGroupIdTemplate> match(FileResourceGroupConfigurationManager manager, SelectionCriteria criteria)
    {
        return manager.match(criteria)
                .orElseThrow(() -> new IllegalStateException("No match"));
    }

    private static void assertFails(String fileName, String expectedPattern)
    {
        assertThatThrownBy(() -> parse(fileName)).hasMessageMatching(expectedPattern);
    }

    private static FileResourceGroupConfigurationManager parse(String fileName)
    {
        FileResourceGroupConfig config = new FileResourceGroupConfig();
        config.setConfigFile(getResource(fileName).getPath());
        return new FileResourceGroupConfigurationManager(
                listener -> listener.accept(new MemoryPoolInfo(MEMORY_POOL_SIZE, 0, 0, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of())),
                config);
    }

    private static ManagerSpec parseManagerSpec(String fileName)
    {
        FileResourceGroupConfig config = new FileResourceGroupConfig();
        config.setConfigFile(getResource(fileName).getPath());
        return FileResourceGroupConfigurationManager.parseManagerSpec(config);
    }

    private static SelectionCriteria userAndSourceSelectionCriteria(String user, String source)
    {
        return new SelectionCriteria(true, user, ImmutableSet.of(), user, Optional.empty(), Optional.of(source), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.empty());
    }

    private static SelectionCriteria userSelectionCriteria(String user)
    {
        return userAndSourceSelectionCriteria(user, "source");
    }

    private static SelectionCriteria identitySelectionCriteria(String user, String originalUser, Optional<String> authenticatedUser)
    {
        return new SelectionCriteria(true, user, ImmutableSet.of(), originalUser, authenticatedUser, Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.empty());
    }

    private static SelectionCriteria queryTypeSelectionCriteria(String queryType)
    {
        return new SelectionCriteria(true, "test_user", ImmutableSet.of(), "test_user", Optional.empty(), Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.of(queryType));
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
}
