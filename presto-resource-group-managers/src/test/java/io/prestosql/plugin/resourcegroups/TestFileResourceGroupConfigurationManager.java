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
package io.prestosql.plugin.resourcegroups;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.spi.resourcegroups.ResourceGroup;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.resourcegroups.SelectionContext;
import io.prestosql.spi.resourcegroups.SelectionCriteria;
import io.prestosql.spi.session.ResourceEstimates;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.spi.resourcegroups.SchedulingPolicy.WEIGHTED;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestFileResourceGroupConfigurationManager
{
    private static final ResourceEstimates EMPTY_RESOURCE_ESTIMATES = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty());

    @Test
    public void testInvalid()
    {
        assertFails("resource_groups_config_bad_root.json", "Duplicated root group: global");
        assertFails("resource_groups_config_bad_sub_group.json", "Duplicated sub group: sub");
        assertFails("resource_groups_config_bad_group_id.json", "Invalid resource group name. 'glo.bal' contains a '.'");
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
    public void testConfiguration()
    {
        FileResourceGroupConfigurationManager manager = parse("resource_groups_config.json");
        ResourceGroupId globalId = new ResourceGroupId("global");
        ResourceGroup global = new TestingResourceGroup(globalId);
        manager.configure(global, new SelectionContext<>(globalId, new VariableMap(ImmutableMap.of("USER", "user"))));
        assertEquals(global.getSoftMemoryLimit(), new DataSize(1, MEGABYTE));
        assertEquals(global.getSoftCpuLimit(), new Duration(1, HOURS));
        assertEquals(global.getHardCpuLimit(), new Duration(1, DAYS));
        assertEquals(global.getCpuQuotaGenerationMillisPerSecond(), 1000 * 24);
        assertEquals(global.getMaxQueuedQueries(), 1000);
        assertEquals(global.getHardConcurrencyLimit(), 100);
        assertEquals(global.getSchedulingPolicy(), WEIGHTED);
        assertEquals(global.getSchedulingWeight(), 0);
        assertTrue(global.getJmxExport());

        ResourceGroupId subId = new ResourceGroupId(globalId, "sub");
        ResourceGroup sub = new TestingResourceGroup(subId);
        manager.configure(sub, new SelectionContext<>(subId, new VariableMap(ImmutableMap.of("USER", "user"))));
        assertEquals(sub.getSoftMemoryLimit(), new DataSize(2, MEGABYTE));
        assertEquals(sub.getHardConcurrencyLimit(), 3);
        assertEquals(sub.getMaxQueuedQueries(), 4);
        assertNull(sub.getSchedulingPolicy());
        assertEquals(sub.getSchedulingWeight(), 5);
        assertFalse(sub.getJmxExport());
    }

    @Test
    public void testExtractVariableConfiguration()
    {
        FileResourceGroupConfigurationManager manager = parse("resource_groups_config_extract_variable.json");

        VariableMap variableMap = new VariableMap(ImmutableMap.of("USER", "user", "domain", "presto", "region", "us_east", "cluster", "12"));

        ResourceGroupId globalId = new ResourceGroupId("global");
        manager.configure(new TestingResourceGroup(globalId), new SelectionContext<>(globalId, variableMap));

        ResourceGroupId childId = new ResourceGroupId(new ResourceGroupId("global"), "presto:us_east:12");
        TestingResourceGroup child = new TestingResourceGroup(childId);
        manager.configure(child, new SelectionContext<>(childId, variableMap));

        assertEquals(child.getHardConcurrencyLimit(), 3);
    }

    @Test
    public void testLegacyConfiguration()
    {
        FileResourceGroupConfigurationManager manager = parse("resource_groups_config_legacy.json");
        ResourceGroupId globalId = new ResourceGroupId("global");
        ResourceGroup global = new TestingResourceGroup(globalId);
        manager.configure(global, new SelectionContext<>(globalId, new VariableMap(ImmutableMap.of("USER", "user"))));
        assertEquals(global.getSoftMemoryLimit(), new DataSize(3, MEGABYTE));
        assertEquals(global.getMaxQueuedQueries(), 99);
        assertEquals(global.getHardConcurrencyLimit(), 42);
    }

    private static void assertMatch(FileResourceGroupConfigurationManager manager, SelectionCriteria context, String expectedResourceGroup)
    {
        Optional<ResourceGroupId> group = manager.match(context)
                .map(SelectionContext::getResourceGroupId);
        assertTrue(group.isPresent(), "match expected");
        assertEquals(group.get().toString(), expectedResourceGroup, format("Expected: '%s' resource group, found: %s", expectedResourceGroup, group.get()));
    }

    private static void assertFails(String fileName, String expectedPattern)
    {
        assertThatThrownBy(() -> parse(fileName)).hasMessageMatching(expectedPattern);
    }

    private static FileResourceGroupConfigurationManager parse(String fileName)
    {
        FileResourceGroupConfig config = new FileResourceGroupConfig();
        config.setConfigFile(getResource(fileName).getPath());
        return new FileResourceGroupConfigurationManager((poolId, listener) -> {}, config);
    }

    private static SelectionCriteria queryTypeSelectionCriteria(String queryType)
    {
        return new SelectionCriteria(true, "test_user", Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.of(queryType));
    }
}
