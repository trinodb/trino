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
package io.trino.execution.resourcegroups;

import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.execution.MockManagedQueryExecution;
import io.trino.execution.MockManagedQueryExecution.MockManagedQueryExecutionBuilder;
import io.trino.server.QueryStateInfo;
import io.trino.server.ResourceGroupInfo;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.spi.resourcegroups.ResourceGroupState.CAN_QUEUE;
import static io.trino.spi.resourcegroups.ResourceGroupState.CAN_RUN;
import static io.trino.spi.resourcegroups.SchedulingPolicy.FAIR;
import static io.trino.spi.resourcegroups.SchedulingPolicy.QUERY_PRIORITY;
import static io.trino.spi.resourcegroups.SchedulingPolicy.WEIGHTED;
import static io.trino.spi.resourcegroups.SchedulingPolicy.WEIGHTED_FAIR;
import static java.util.Collections.reverse;
import static org.assertj.core.api.Assertions.assertThat;

public class TestResourceGroups
{
    @Test
    @Timeout(10)
    public void testQueueFull()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        root.setMaxQueuedQueries(1);
        root.setHardConcurrencyLimit(1);
        MockManagedQueryExecution query1 = new MockManagedQueryExecutionBuilder().build();
        root.run(query1);
        assertThat(query1.getState()).isEqualTo(RUNNING);
        MockManagedQueryExecution query2 = new MockManagedQueryExecutionBuilder().build();
        root.run(query2);
        assertThat(query2.getState()).isEqualTo(QUEUED);
        MockManagedQueryExecution query3 = new MockManagedQueryExecutionBuilder().build();
        root.run(query3);
        assertThat(query3.getState()).isEqualTo(FAILED);
        assertThat(query3.getThrowable().getMessage()).isEqualTo("Too many queued queries for \"root\"");
    }

    @Test
    @Timeout(10)
    public void testFairEligibility()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        root.setMaxQueuedQueries(4);
        root.setHardConcurrencyLimit(1);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group1.setMaxQueuedQueries(4);
        group1.setHardConcurrencyLimit(1);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group2.setMaxQueuedQueries(4);
        group2.setHardConcurrencyLimit(1);
        InternalResourceGroup group3 = root.getOrCreateSubGroup("3");
        group3.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group3.setMaxQueuedQueries(4);
        group3.setHardConcurrencyLimit(1);
        MockManagedQueryExecution query1a = new MockManagedQueryExecutionBuilder().build();
        group1.run(query1a);
        assertThat(query1a.getState()).isEqualTo(RUNNING);
        MockManagedQueryExecution query1b = new MockManagedQueryExecutionBuilder().build();
        group1.run(query1b);
        assertThat(query1b.getState()).isEqualTo(QUEUED);
        MockManagedQueryExecution query2a = new MockManagedQueryExecutionBuilder().build();
        group2.run(query2a);
        assertThat(query2a.getState()).isEqualTo(QUEUED);
        MockManagedQueryExecution query2b = new MockManagedQueryExecutionBuilder().build();
        group2.run(query2b);
        assertThat(query2b.getState()).isEqualTo(QUEUED);
        MockManagedQueryExecution query3a = new MockManagedQueryExecutionBuilder().build();
        group3.run(query3a);
        assertThat(query3a.getState()).isEqualTo(QUEUED);

        query1a.complete();
        // 2a and not 1b should have started, as group1 was not eligible to start a second query
        assertThat(query1b.getState()).isEqualTo(QUEUED);
        assertThat(query2a.getState()).isEqualTo(RUNNING);
        assertThat(query2b.getState()).isEqualTo(QUEUED);
        assertThat(query3a.getState()).isEqualTo(QUEUED);

        query2a.complete();
        assertThat(query3a.getState()).isEqualTo(RUNNING);
        assertThat(query2b.getState()).isEqualTo(QUEUED);
        assertThat(query1b.getState()).isEqualTo(QUEUED);

        query3a.complete();
        assertThat(query1b.getState()).isEqualTo(RUNNING);
        assertThat(query2b.getState()).isEqualTo(QUEUED);
    }

    @Test
    public void testSetSchedulingPolicy()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        root.setMaxQueuedQueries(4);
        root.setHardConcurrencyLimit(1);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group1.setMaxQueuedQueries(4);
        group1.setHardConcurrencyLimit(2);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group2.setMaxQueuedQueries(4);
        group2.setHardConcurrencyLimit(2);
        MockManagedQueryExecution query1a = new MockManagedQueryExecutionBuilder().build();
        group1.run(query1a);
        assertThat(query1a.getState()).isEqualTo(RUNNING);
        MockManagedQueryExecution query1b = new MockManagedQueryExecutionBuilder().build();
        group1.run(query1b);
        assertThat(query1b.getState()).isEqualTo(QUEUED);
        MockManagedQueryExecution query1c = new MockManagedQueryExecutionBuilder().build();
        group1.run(query1c);
        assertThat(query1c.getState()).isEqualTo(QUEUED);
        MockManagedQueryExecution query2a = new MockManagedQueryExecutionBuilder().build();
        group2.run(query2a);
        assertThat(query2a.getState()).isEqualTo(QUEUED);

        assertThat(root.getInfo().numEligibleSubGroups()).isEqualTo(2);
        assertThat(root.getOrCreateSubGroup("1").getQueuedQueries()).isEqualTo(2);
        assertThat(root.getOrCreateSubGroup("2").getQueuedQueries()).isEqualTo(1);
        assertThat(root.getSchedulingPolicy()).isEqualTo(FAIR);
        root.setSchedulingPolicy(QUERY_PRIORITY);
        assertThat(root.getInfo().numEligibleSubGroups()).isEqualTo(2);
        assertThat(root.getOrCreateSubGroup("1").getQueuedQueries()).isEqualTo(2);
        assertThat(root.getOrCreateSubGroup("2").getQueuedQueries()).isEqualTo(1);

        assertThat(root.getSchedulingPolicy()).isEqualTo(QUERY_PRIORITY);
        assertThat(root.getOrCreateSubGroup("1").getSchedulingPolicy()).isEqualTo(QUERY_PRIORITY);
        assertThat(root.getOrCreateSubGroup("2").getSchedulingPolicy()).isEqualTo(QUERY_PRIORITY);
    }

    @Test
    @Timeout(10)
    public void testFairQueuing()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        root.setMaxQueuedQueries(4);
        root.setHardConcurrencyLimit(1);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group1.setMaxQueuedQueries(4);
        group1.setHardConcurrencyLimit(2);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group2.setMaxQueuedQueries(4);
        group2.setHardConcurrencyLimit(2);
        MockManagedQueryExecution query1a = new MockManagedQueryExecutionBuilder().build();
        group1.run(query1a);
        assertThat(query1a.getState()).isEqualTo(RUNNING);
        MockManagedQueryExecution query1b = new MockManagedQueryExecutionBuilder().build();
        group1.run(query1b);
        assertThat(query1b.getState()).isEqualTo(QUEUED);
        MockManagedQueryExecution query1c = new MockManagedQueryExecutionBuilder().build();
        group1.run(query1c);
        assertThat(query1c.getState()).isEqualTo(QUEUED);
        MockManagedQueryExecution query2a = new MockManagedQueryExecutionBuilder().build();
        group2.run(query2a);
        assertThat(query2a.getState()).isEqualTo(QUEUED);

        query1a.complete();
        // 1b and not 2a should have started, as it became queued first and group1 was eligible to run more
        assertThat(query1b.getState()).isEqualTo(RUNNING);
        assertThat(query1c.getState()).isEqualTo(QUEUED);
        assertThat(query2a.getState()).isEqualTo(QUEUED);

        // 2a and not 1c should have started, as all eligible sub groups get fair sharing
        query1b.complete();
        assertThat(query2a.getState()).isEqualTo(RUNNING);
        assertThat(query1c.getState()).isEqualTo(QUEUED);
    }

    @Test
    @Timeout(10)
    public void testMemoryLimit()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimitBytes(1);
        root.setMaxQueuedQueries(4);
        root.setHardConcurrencyLimit(3);
        MockManagedQueryExecution query1 = new MockManagedQueryExecutionBuilder().withInitialMemoryUsage(DataSize.ofBytes(2)).build();
        root.run(query1);
        // Process the group to refresh stats
        root.updateGroupsAndProcessQueuedQueries();
        assertThat(query1.getState()).isEqualTo(RUNNING);
        MockManagedQueryExecution query2 = new MockManagedQueryExecutionBuilder().build();
        root.run(query2);
        assertThat(query2.getState()).isEqualTo(QUEUED);
        MockManagedQueryExecution query3 = new MockManagedQueryExecutionBuilder().build();
        root.run(query3);
        assertThat(query3.getState()).isEqualTo(QUEUED);

        query1.complete();
        assertThat(query2.getState()).isEqualTo(RUNNING);
        assertThat(query3.getState()).isEqualTo(RUNNING);
    }

    @Test
    public void testSubgroupMemoryLimit()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimitBytes(10);
        root.setMaxQueuedQueries(4);
        root.setHardConcurrencyLimit(3);
        InternalResourceGroup subgroup = root.getOrCreateSubGroup("subgroup");
        subgroup.setSoftMemoryLimitBytes(1);
        subgroup.setMaxQueuedQueries(4);
        subgroup.setHardConcurrencyLimit(3);

        MockManagedQueryExecution query1 = new MockManagedQueryExecutionBuilder().withInitialMemoryUsage(DataSize.ofBytes(2)).build();
        subgroup.run(query1);
        // Process the group to refresh stats
        root.updateGroupsAndProcessQueuedQueries();
        assertThat(query1.getState()).isEqualTo(RUNNING);
        MockManagedQueryExecution query2 = new MockManagedQueryExecutionBuilder().build();
        subgroup.run(query2);
        assertThat(query2.getState()).isEqualTo(QUEUED);
        MockManagedQueryExecution query3 = new MockManagedQueryExecutionBuilder().build();
        subgroup.run(query3);
        assertThat(query3.getState()).isEqualTo(QUEUED);

        query1.complete();
        assertThat(query2.getState()).isEqualTo(RUNNING);
        assertThat(query3.getState()).isEqualTo(RUNNING);
    }

    @Test
    @Timeout(10)
    public void testSoftCpuLimit()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimitBytes(1);
        root.setSoftCpuLimit(Duration.ofSeconds(1));
        root.setHardCpuLimit(Duration.ofSeconds(2));
        root.setCpuQuotaGenerationMillisPerSecond(2000);
        root.setMaxQueuedQueries(1);
        root.setHardConcurrencyLimit(2);

        MockManagedQueryExecution query1 = new MockManagedQueryExecutionBuilder()
                .withInitialMemoryUsage(DataSize.ofBytes(1))
                .withQueryId("query_id")
                .withInitialCpuUsageMillis(1000)
                .build();

        root.run(query1);
        assertThat(query1.getState()).isEqualTo(RUNNING);

        MockManagedQueryExecution query2 = new MockManagedQueryExecutionBuilder().build();
        root.run(query2);
        assertThat(query2.getState()).isEqualTo(RUNNING);

        MockManagedQueryExecution query3 = new MockManagedQueryExecutionBuilder().build();
        root.run(query3);
        assertThat(query3.getState()).isEqualTo(QUEUED);

        query1.complete();
        assertThat(query2.getState()).isEqualTo(RUNNING);
        assertThat(query3.getState()).isEqualTo(QUEUED);

        root.generateCpuQuota(2);
        root.updateGroupsAndProcessQueuedQueries();
        assertThat(query2.getState()).isEqualTo(RUNNING);
        assertThat(query3.getState()).isEqualTo(RUNNING);
    }

    @Test
    @Timeout(10)
    public void testHardCpuLimit()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimitBytes(1);
        root.setHardCpuLimit(Duration.ofSeconds(1));
        root.setCpuQuotaGenerationMillisPerSecond(2000);
        root.setMaxQueuedQueries(1);
        root.setHardConcurrencyLimit(1);

        MockManagedQueryExecution query1 = new MockManagedQueryExecutionBuilder()
                .withInitialMemoryUsage(DataSize.ofBytes(1))
                .withQueryId("query_id")
                .withInitialCpuUsageMillis(2000)
                .build();

        root.run(query1);
        assertThat(query1.getState()).isEqualTo(RUNNING);
        MockManagedQueryExecution query2 = new MockManagedQueryExecutionBuilder().build();
        root.run(query2);
        assertThat(query2.getState()).isEqualTo(QUEUED);

        query1.complete();
        root.updateGroupsAndProcessQueuedQueries();
        assertThat(query2.getState()).isEqualTo(QUEUED);

        root.generateCpuQuota(2);
        root.updateGroupsAndProcessQueuedQueries();
        assertThat(query2.getState()).isEqualTo(RUNNING);
    }

    /**
     * Test resource group CPU usage update by manually invoking the CPU quota regeneration and queue processing methods
     * that are invoked periodically by the resource group manager
     */
    @Test
    @Timeout(10)
    public void testCpuUsageUpdateForRunningQuery()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        InternalResourceGroup child = root.getOrCreateSubGroup("child");

        Stream.of(root, child).forEach(group -> {
            group.setCpuQuotaGenerationMillisPerSecond(1);
            group.setHardCpuLimit(Duration.ofMillis(3));
            group.setSoftCpuLimit(Duration.ofMillis(3));
            group.setHardConcurrencyLimit(100);
            group.setMaxQueuedQueries(100);
        });

        MockManagedQueryExecution q1 = new MockManagedQueryExecutionBuilder().build();
        child.run(q1);
        assertThat(q1.getState()).isEqualTo(RUNNING);
        q1.consumeCpuTimeMillis(4);

        root.updateGroupsAndProcessQueuedQueries();
        Stream.of(root, child).forEach(group -> assertExceedsCpuLimit(group, 4));

        // q2 gets queued, because the cached usage is greater than the limit
        MockManagedQueryExecution q2 = new MockManagedQueryExecutionBuilder().build();
        child.run(q2);
        assertThat(q2.getState()).isEqualTo(QUEUED);

        // Generating CPU quota before the query finishes. This assertion verifies CPU update during quota generation.
        root.generateCpuQuota(2);
        Stream.of(root, child).forEach(group -> assertWithinCpuLimit(group, 2));

        // An incoming query starts running right away.
        MockManagedQueryExecution q3 = new MockManagedQueryExecutionBuilder().build();
        child.run(q3);
        assertThat(q3.getState()).isEqualTo(RUNNING);

        // A queued query starts running only after invoking `updateGroupsAndProcessQueuedQueries`.
        assertThat(q2.getState()).isEqualTo(QUEUED);
        root.updateGroupsAndProcessQueuedQueries();
        assertThat(q2.getState()).isEqualTo(RUNNING);
    }

    @Test
    @Timeout(10)
    public void testCpuUsageUpdateAtQueryCompletion()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        InternalResourceGroup child = root.getOrCreateSubGroup("child");

        Stream.of(root, child).forEach(group -> {
            group.setCpuQuotaGenerationMillisPerSecond(1);
            group.setHardCpuLimit(Duration.ofMillis(3));
            group.setSoftCpuLimit(Duration.ofMillis(3));
            group.setHardConcurrencyLimit(100);
            group.setMaxQueuedQueries(100);
        });

        MockManagedQueryExecution q1 = new MockManagedQueryExecutionBuilder().build();
        child.run(q1);
        assertThat(q1.getState()).isEqualTo(RUNNING);

        q1.consumeCpuTimeMillis(4);
        q1.complete();

        // Query completion updates the cached usage to 2s. q1 will be removed from runningQueries at this point.
        // Therefore, updateGroupsAndProcessQueuedQueries invocation will not be able to update its CPU usage later.
        Stream.of(root, child).forEach(group -> assertExceedsCpuLimit(group, 4));

        // q2 gets queued since cached usage exceeds the limit.
        MockManagedQueryExecution q2 = new MockManagedQueryExecutionBuilder().build();
        child.run(q2);
        assertThat(q2.getState()).isEqualTo(QUEUED);

        root.generateCpuQuota(2);
        Stream.of(root, child).forEach(group -> assertWithinCpuLimit(group, 2));
        assertThat(q2.getState()).isEqualTo(QUEUED);

        // q2 should run after groups are updated. CPU usage should not be double counted.
        root.updateGroupsAndProcessQueuedQueries();
        Stream.of(root, child).forEach(group -> assertWithinCpuLimit(group, 2));
        assertThat(q2.getState()).isEqualTo(RUNNING);
    }

    @Test
    @Timeout(10)
    public void testCpuUsageUpdateWhenParentGroupHasRunningQueries()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (_, _) -> {}, directExecutor());
        root.setCpuQuotaGenerationMillisPerSecond(1);
        root.setHardCpuLimit(Duration.ofMillis(3));
        root.setSoftCpuLimit(Duration.ofMillis(3));
        root.setHardConcurrencyLimit(100);
        root.setMaxQueuedQueries(100);

        MockManagedQueryExecution q1 = new MockManagedQueryExecutionBuilder().build();
        root.run(q1);
        assertThat(q1.getState()).isEqualTo(RUNNING);
        q1.consumeCpuTimeMillis(2);

        InternalResourceGroup child = root.getOrCreateSubGroup("child");
        child.setCpuQuotaGenerationMillisPerSecond(1);
        child.setHardCpuLimit(Duration.ofMillis(3));
        child.setSoftCpuLimit(Duration.ofMillis(3));
        child.setHardConcurrencyLimit(100);
        child.setMaxQueuedQueries(100);

        MockManagedQueryExecution q2 = new MockManagedQueryExecutionBuilder().build();
        child.run(q2);
        assertThat(q2.getState()).isEqualTo(RUNNING);
        q2.consumeCpuTimeMillis(2);

        root.updateGroupsAndProcessQueuedQueries();
        assertExceedsCpuLimit(root, 4);
        assertWithinCpuLimit(child, 2);
    }

    @Test
    @Timeout(10)
    public void testMemoryUsageUpdateWhenParentGroupHasRunningQueries()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (_, _) -> {}, directExecutor());
        root.setHardConcurrencyLimit(100);
        root.setMaxQueuedQueries(100);
        root.setSoftMemoryLimitBytes(3);

        MockManagedQueryExecution q1 = new MockManagedQueryExecutionBuilder().build();
        root.run(q1);
        assertThat(q1.getState()).isEqualTo(RUNNING);
        q1.setMemoryUsage(DataSize.ofBytes(2));

        InternalResourceGroup child = root.getOrCreateSubGroup("child");
        child.setHardConcurrencyLimit(100);
        child.setMaxQueuedQueries(100);
        child.setSoftMemoryLimitBytes(3);

        MockManagedQueryExecution q2 = new MockManagedQueryExecutionBuilder().build();
        child.run(q2);
        assertThat(q2.getState()).isEqualTo(RUNNING);
        q2.setMemoryUsage(DataSize.ofBytes(2));

        root.updateGroupsAndProcessQueuedQueries();
        assertExceedsMemoryLimit(root, 4);
        assertWithinMemoryLimit(child, 2);
    }

    @Test
    @Timeout(10)
    public void testCpuUsageUpdateForDisabledGroup()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (_, _) -> {}, directExecutor());
        InternalResourceGroup child = root.getOrCreateSubGroup("child");

        Stream.of(root, child).forEach(group -> {
            group.setCpuQuotaGenerationMillisPerSecond(1);
            group.setHardCpuLimit(Duration.ofMillis(3));
            group.setSoftCpuLimit(Duration.ofMillis(3));
            group.setHardConcurrencyLimit(100);
            group.setMaxQueuedQueries(100);
        });

        MockManagedQueryExecution q1 = new MockManagedQueryExecutionBuilder().build();
        child.run(q1);
        assertThat(q1.getState()).isEqualTo(RUNNING);
        q1.consumeCpuTimeMillis(2);

        root.updateGroupsAndProcessQueuedQueries();
        Stream.of(root, child).forEach(group -> assertWithinCpuLimit(group, 2));

        child.setDisabled(true);

        MockManagedQueryExecution q2 = new MockManagedQueryExecutionBuilder().build();
        root.run(q2);
        assertThat(q2.getState()).isEqualTo(RUNNING);
        q2.consumeCpuTimeMillis(2);

        root.updateGroupsAndProcessQueuedQueries();
        assertWithinCpuLimit(child, 2);
        assertExceedsCpuLimit(root, 4);

        MockManagedQueryExecution q3 = new MockManagedQueryExecutionBuilder().build();
        root.run(q3);
        assertThat(q3.getState()).isEqualTo(QUEUED);
    }

    @Test
    @Timeout(10)
    public void testMemoryUsageUpdateForDisabledGroup()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (_, _) -> {}, directExecutor());
        InternalResourceGroup child = root.getOrCreateSubGroup("child");

        Stream.of(root, child).forEach(group -> {
            group.setHardConcurrencyLimit(100);
            group.setMaxQueuedQueries(100);
            group.setSoftMemoryLimitBytes(3);
        });

        MockManagedQueryExecution q1 = new MockManagedQueryExecutionBuilder().build();
        child.run(q1);
        assertThat(q1.getState()).isEqualTo(RUNNING);
        q1.setMemoryUsage(DataSize.ofBytes(2));

        Stream.of(root, child).forEach(group -> assertWithinMemoryLimit(group, 0));
        root.updateGroupsAndProcessQueuedQueries();
        Stream.of(root, child).forEach(group -> assertWithinMemoryLimit(group, 2));

        child.setDisabled(true);

        MockManagedQueryExecution q2 = new MockManagedQueryExecutionBuilder().build();
        root.run(q2);
        assertThat(q2.getState()).isEqualTo(RUNNING);
        q2.setMemoryUsage(DataSize.ofBytes(2));

        root.updateGroupsAndProcessQueuedQueries();
        assertWithinMemoryLimit(child, 2);
        assertExceedsMemoryLimit(root, 4);

        MockManagedQueryExecution q3 = new MockManagedQueryExecutionBuilder().build();
        root.run(q3);
        assertThat(q3.getState()).isEqualTo(QUEUED);
    }

    @Test
    @Timeout(10)
    public void testMemoryUsageUpdateForRunningQuery()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        InternalResourceGroup child = root.getOrCreateSubGroup("child");

        Stream.of(root, child).forEach(group -> {
            group.setHardConcurrencyLimit(100);
            group.setMaxQueuedQueries(100);
            group.setSoftMemoryLimitBytes(3);
        });

        MockManagedQueryExecution q1 = new MockManagedQueryExecutionBuilder().build();
        child.run(q1);
        assertThat(q1.getState()).isEqualTo(RUNNING);
        q1.setMemoryUsage(DataSize.ofBytes(4));

        Stream.of(root, child).forEach(group -> assertWithinMemoryLimit(group, 0));
        root.updateGroupsAndProcessQueuedQueries();
        Stream.of(root, child).forEach(group -> assertExceedsMemoryLimit(group, 4));

        // A new query gets queued since the current usage exceeds the limit.
        MockManagedQueryExecution q2 = new MockManagedQueryExecutionBuilder().build();
        child.run(q2);
        assertThat(q2.getState()).isEqualTo(QUEUED);

        q1.setMemoryUsage(DataSize.ofBytes(2));

        // A new incoming query q3 gets queued since cached usage still exceeds the limit.
        MockManagedQueryExecution q3 = new MockManagedQueryExecutionBuilder().build();
        child.run(q3);
        assertThat(q3.getState()).isEqualTo(QUEUED);

        // q2 and q3 start running when cached usage is updated and queued queries are processed.
        root.updateGroupsAndProcessQueuedQueries();
        Stream.of(root, child).forEach(group -> assertWithinMemoryLimit(group, 2));
        assertThat(q2.getState()).isEqualTo(RUNNING);
        assertThat(q3.getState()).isEqualTo(RUNNING);
    }

    @Test
    @Timeout(10)
    public void testMemoryUsageUpdateAtQueryCompletion()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        InternalResourceGroup child = root.getOrCreateSubGroup("child");

        Stream.of(root, child).forEach(group -> {
            group.setHardConcurrencyLimit(100);
            group.setMaxQueuedQueries(100);
            group.setSoftMemoryLimitBytes(3);
        });

        MockManagedQueryExecution q1 = new MockManagedQueryExecutionBuilder().build();
        child.run(q1);
        assertThat(q1.getState()).isEqualTo(RUNNING);
        q1.setMemoryUsage(DataSize.ofBytes(4));

        Stream.of(root, child).forEach(group -> assertWithinMemoryLimit(group, 0));
        root.updateGroupsAndProcessQueuedQueries();
        Stream.of(root, child).forEach(group -> assertExceedsMemoryLimit(group, 4));

        // Query completion should reduce the cached memory usage to 0B.
        q1.complete();
        Stream.of(root, child).forEach(group -> assertWithinMemoryLimit(group, 0));

        // q2 starts running since usage is within the limit.
        MockManagedQueryExecution q2 = new MockManagedQueryExecutionBuilder().build();
        child.run(q2);
        assertThat(q2.getState()).isEqualTo(RUNNING);
    }

    /**
     * A test for correct CPU usage update aggregation and propagation in non-leaf nodes. It uses in a multi
     * level resource group tree, with non-leaf resource groups having more than one child.
     */
    @Test
    @Timeout(10)
    public void testRecursiveCpuUsageUpdate()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        InternalResourceGroup rootChild1 = root.getOrCreateSubGroup("rootChild1");
        InternalResourceGroup rootChild2 = root.getOrCreateSubGroup("rootChild2");
        InternalResourceGroup rootChild1Child1 = rootChild1.getOrCreateSubGroup("rootChild1Child1");
        InternalResourceGroup rootChild1Child2 = rootChild1.getOrCreateSubGroup("rootChild1Child2");

        // Set the same values in all the groups for some configurations
        Stream.of(root, rootChild1, rootChild2, rootChild1Child1, rootChild1Child2).forEach(group -> {
            group.setCpuQuotaGenerationMillisPerSecond(1);
            group.setHardConcurrencyLimit(100);
            group.setMaxQueuedQueries(100);
        });

        root.setHardCpuLimit(Duration.ofMillis(16));
        rootChild1.setHardCpuLimit(Duration.ofMillis(6));

        // Setting a higher limit for leaf nodes to make sure they are always in the limit
        rootChild1Child1.setHardCpuLimit(Duration.ofMillis(100));
        rootChild1Child2.setHardCpuLimit(Duration.ofMillis(100));
        rootChild2.setHardCpuLimit(Duration.ofMillis(100));

        MockManagedQueryExecution q1 = new MockManagedQueryExecutionBuilder().build();
        MockManagedQueryExecution q2 = new MockManagedQueryExecutionBuilder().build();
        MockManagedQueryExecution q3 = new MockManagedQueryExecutionBuilder().build();

        rootChild1Child1.run(q1);
        rootChild1Child2.run(q2);
        rootChild2.run(q3);

        assertThat(q1.getState()).isEqualTo(RUNNING);
        assertThat(q2.getState()).isEqualTo(RUNNING);
        assertThat(q3.getState()).isEqualTo(RUNNING);

        q1.consumeCpuTimeMillis(4);
        q2.consumeCpuTimeMillis(10);
        q3.consumeCpuTimeMillis(4);

        // This invocation will update the cached usage for the nodes
        root.updateGroupsAndProcessQueuedQueries();

        assertExceedsCpuLimit(root, 18);
        assertExceedsCpuLimit(rootChild1, 14);
        assertWithinCpuLimit(rootChild2, 4);
        assertWithinCpuLimit(rootChild1Child1, 4);
        assertWithinCpuLimit(rootChild1Child2, 10);

        // q4 submitted in rootChild2 gets queued because root's CPU usage exceeds the limit
        MockManagedQueryExecution q4 = new MockManagedQueryExecutionBuilder().build();
        rootChild2.run(q4);
        assertThat(q4.getState()).isEqualTo(QUEUED);

        // q5 submitted in rootChild1Child1 gets queued because root's CPU usage exceeds the limit
        MockManagedQueryExecution q5 = new MockManagedQueryExecutionBuilder().build();
        rootChild1Child1.run(q5);
        assertThat(q5.getState()).isEqualTo(QUEUED);

        // Assert CPU usage update after quota regeneration
        root.generateCpuQuota(4);
        assertWithinCpuLimit(root, 14);
        assertExceedsCpuLimit(rootChild1, 10);
        assertWithinCpuLimit(rootChild2, 0);
        assertWithinCpuLimit(rootChild1Child1, 0);
        assertWithinCpuLimit(rootChild1Child2, 6);

        root.updateGroupsAndProcessQueuedQueries();

        // q4 gets dequeued, because CPU usages of root and rootChild2 are below their limits.
        assertThat(q4.getState()).isEqualTo(RUNNING);
        // q5 does not get dequeued, because rootChild1's CPU usage exceeds the limit.
        assertThat(q5.getState()).isEqualTo(QUEUED);

        q2.consumeCpuTimeMillis(3);
        q2.complete();

        // Query completion updates cached CPU usage of root, rootChild1 and rootChild1Child2.
        assertExceedsCpuLimit(root, 17);
        assertExceedsCpuLimit(rootChild1, 13);
        assertWithinCpuLimit(rootChild1Child2, 9);

        // q6 in rootChild2 gets queued because root's CPU usage exceeds the limit.
        MockManagedQueryExecution q6 = new MockManagedQueryExecutionBuilder().build();
        rootChild2.run(q6);
        assertThat(q6.getState()).isEqualTo(QUEUED);

        // Assert usage after regeneration
        root.generateCpuQuota(6);
        assertWithinCpuLimit(root, 11);
        assertExceedsCpuLimit(rootChild1, 7);
        assertWithinCpuLimit(rootChild2, 0);
        assertWithinCpuLimit(rootChild1Child1, 0);
        assertWithinCpuLimit(rootChild1Child2, 3);

        root.updateGroupsAndProcessQueuedQueries();

        // q5 is queued, because rootChild1's usage still exceeds the limit.
        assertThat(q5.getState()).isEqualTo(QUEUED);
        // q6 starts running, because usage in rootChild2 and root are within their limits.
        assertThat(q6.getState()).isEqualTo(RUNNING);

        // q5 starts running after rootChild1's usage comes within the limit
        root.generateCpuQuota(2);
        assertWithinCpuLimit(rootChild1, 5);
        root.updateGroupsAndProcessQueuedQueries();
        assertThat(q5.getState()).isEqualTo(RUNNING);
    }

    /**
     * A test for correct memory usage update aggregation and propagation in non-leaf nodes. It uses in a multi
     * level resource group tree, with non-leaf resource groups having more than one child.
     */
    @Test
    @Timeout(10)
    public void testMemoryUpdateRecursively()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor())
        {
            @Override
            public void triggerProcessQueuedQueries()
            {
                // No op to allow the test fine-grained control about when to trigger the next query.
            }
        };
        InternalResourceGroup rootChild1 = root.getOrCreateSubGroup("rootChild1");
        InternalResourceGroup rootChild2 = root.getOrCreateSubGroup("rootChild2");
        InternalResourceGroup rootChild1Child1 = rootChild1.getOrCreateSubGroup("rootChild1Child1");
        InternalResourceGroup rootChild1Child2 = rootChild1.getOrCreateSubGroup("rootChild1Child2");

        // Set the same values in all the groups for some configurations
        Stream.of(root, rootChild1, rootChild2, rootChild1Child1, rootChild1Child2).forEach(group -> {
            group.setHardConcurrencyLimit(100);
            group.setMaxQueuedQueries(100);
        });

        root.setSoftMemoryLimitBytes(8);
        rootChild1.setSoftMemoryLimitBytes(3);
        // Setting a higher limit for leaf nodes
        rootChild2.setSoftMemoryLimitBytes(100);
        rootChild1Child1.setSoftMemoryLimitBytes(100);
        rootChild1Child2.setSoftMemoryLimitBytes(100);

        MockManagedQueryExecution q1 = new MockManagedQueryExecutionBuilder().build();
        MockManagedQueryExecution q2 = new MockManagedQueryExecutionBuilder().build();
        MockManagedQueryExecution q3 = new MockManagedQueryExecutionBuilder().build();

        rootChild1Child1.run(q1);
        rootChild1Child2.run(q2);
        rootChild2.run(q3);

        assertThat(q1.getState()).isEqualTo(RUNNING);
        assertThat(q2.getState()).isEqualTo(RUNNING);
        assertThat(q3.getState()).isEqualTo(RUNNING);

        q1.setMemoryUsage(DataSize.ofBytes(2));
        q2.setMemoryUsage(DataSize.ofBytes(5));
        q3.setMemoryUsage(DataSize.ofBytes(2));

        // The cached memory usage gets updated for the tree
        root.updateGroupsAndProcessQueuedQueries();
        assertExceedsMemoryLimit(root, 9);
        assertExceedsMemoryLimit(rootChild1, 7);
        assertWithinMemoryLimit(rootChild2, 2);
        assertWithinMemoryLimit(rootChild1Child1, 2);
        assertWithinMemoryLimit(rootChild1Child2, 5);

        // q4 submitted in rootChild2 gets queued because root's memory usage exceeds the limit
        MockManagedQueryExecution q4 = new MockManagedQueryExecutionBuilder().build();
        rootChild2.run(q4);
        assertThat(q4.getState()).isEqualTo(QUEUED);

        // q5 submitted in rootChild1Child1 gets queued because root's memory usage) exceeds the limit
        MockManagedQueryExecution q5 = new MockManagedQueryExecutionBuilder().build();
        rootChild1Child1.run(q5);
        assertThat(q5.getState()).isEqualTo(QUEUED);

        q1.setMemoryUsage(DataSize.ofBytes(0));

        root.updateGroupsAndProcessQueuedQueries();
        assertWithinMemoryLimit(root, 7);
        assertExceedsMemoryLimit(rootChild1, 5);
        assertWithinMemoryLimit(rootChild1Child1, 0);

        // q4 starts running since usage in root and rootChild2 is within the limits
        assertThat(q4.getState()).isEqualTo(RUNNING);
        // q5 is queued since usage in rootChild1 exceeds the limit.
        assertThat(q5.getState()).isEqualTo(QUEUED);

        // q2's completion triggers memory updates
        q2.complete();
        assertWithinMemoryLimit(root, 2);
        assertWithinMemoryLimit(rootChild1, 0);

        // An incoming query starts running
        MockManagedQueryExecution q6 = new MockManagedQueryExecutionBuilder().build();
        rootChild1Child2.run(q6);
        assertThat(q6.getState()).isEqualTo(RUNNING);

        // queued queries will start running after the update
        assertThat(q5.getState()).isEqualTo(QUEUED);
        root.updateGroupsAndProcessQueuedQueries();
        assertThat(q5.getState()).isEqualTo(RUNNING);
    }

    @Test
    @Timeout(10)
    public void testPriorityScheduling()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        root.setMaxQueuedQueries(100);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(QUERY_PRIORITY);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group1.setMaxQueuedQueries(100);
        group1.setHardConcurrencyLimit(1);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group2.setMaxQueuedQueries(100);
        group2.setHardConcurrencyLimit(1);

        SortedMap<Integer, MockManagedQueryExecution> queries = new TreeMap<>();

        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int priority;
            do {
                priority = random.nextInt(1_000_000) + 1;
            }
            while (queries.containsKey(priority));

            MockManagedQueryExecution query = new MockManagedQueryExecutionBuilder()
                    .withQueryId("query_id")
                    .withPriority(priority)
                    .build();

            if (random.nextBoolean()) {
                group1.run(query);
            }
            else {
                group2.run(query);
            }
            queries.put(priority, query);
        }

        root.setHardConcurrencyLimit(1);

        List<MockManagedQueryExecution> orderedQueries = new ArrayList<>(queries.values());
        reverse(orderedQueries);

        for (MockManagedQueryExecution query : orderedQueries) {
            root.updateGroupsAndProcessQueuedQueries();
            assertThat(query.getState()).isEqualTo(RUNNING);
            query.complete();
        }
    }

    @Test
    @Timeout(10)
    public void testWeightedScheduling()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor())
        {
            @Override
            public void triggerProcessQueuedQueries()
            {
                // No op to allow the test fine-grained control about when to trigger the next query.
            }
        };
        root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        root.setMaxQueuedQueries(4);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED);
        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group1.setMaxQueuedQueries(2);
        group1.setHardConcurrencyLimit(2);
        group1.setSoftConcurrencyLimit(2);
        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group2.setMaxQueuedQueries(2);
        group2.setHardConcurrencyLimit(2);
        group2.setSoftConcurrencyLimit(2);
        group2.setSchedulingWeight(2);

        Set<MockManagedQueryExecution> group1Queries = fillGroupTo(group1, ImmutableSet.of(), 2);
        Set<MockManagedQueryExecution> group2Queries = fillGroupTo(group2, ImmutableSet.of(), 2);
        root.setHardConcurrencyLimit(1);

        int group2Ran = 0;
        for (int i = 0; i < 1000; i++) {
            for (Iterator<MockManagedQueryExecution> iterator = group1Queries.iterator(); iterator.hasNext(); ) {
                MockManagedQueryExecution query = iterator.next();
                if (query.getState() == RUNNING) {
                    query.complete();
                    iterator.remove();
                }
            }
            group2Ran += completeGroupQueries(group2Queries);
            root.updateGroupsAndProcessQueuedQueries();
            group1Queries = fillGroupTo(group1, group1Queries, 2);
            group2Queries = fillGroupTo(group2, group2Queries, 2);
        }

        // group1 has a weight of 1 and group2 has a weight of 2, so group2 should account for (2 / (1 + 2)) of the queries.
        // since this is stochastic, we check that the result of 1000 trials are 2/3 with 99.9999% confidence
        BinomialDistribution binomial = new BinomialDistribution(1000, 2.0 / 3.0);
        int lowerBound = binomial.inverseCumulativeProbability(0.000001);
        int upperBound = binomial.inverseCumulativeProbability(0.999999);
        assertThat(group2Ran).isLessThan(upperBound);
        assertThat(group2Ran).isGreaterThan(lowerBound);
    }

    @Test
    @Timeout(10)
    public void testWeightedFairScheduling()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor())
        {
            @Override
            public void triggerProcessQueuedQueries()
            {
                // No op to allow the test fine-grained control about when to trigger the next query.
            }
        };
        root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        root.setMaxQueuedQueries(50);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED_FAIR);

        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group1.setMaxQueuedQueries(50);
        group1.setHardConcurrencyLimit(2);
        group1.setSoftConcurrencyLimit(2);
        group1.setSchedulingWeight(1);

        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group2.setMaxQueuedQueries(50);
        group2.setHardConcurrencyLimit(2);
        group2.setSoftConcurrencyLimit(2);
        group2.setSchedulingWeight(2);

        Set<MockManagedQueryExecution> group1Queries = fillGroupTo(group1, ImmutableSet.of(), 4);
        Set<MockManagedQueryExecution> group2Queries = fillGroupTo(group2, ImmutableSet.of(), 4);
        root.setHardConcurrencyLimit(3);

        int group1Ran = 0;
        int group2Ran = 0;
        for (int i = 0; i < 1000; i++) {
            group1Ran += completeGroupQueries(group1Queries);
            group2Ran += completeGroupQueries(group2Queries);
            root.updateGroupsAndProcessQueuedQueries();
            group1Queries = fillGroupTo(group1, group1Queries, 4);
            group2Queries = fillGroupTo(group2, group2Queries, 4);
        }

        // group1 has a weight of 1 and group2 has a weight of 2, so group2 should account for (2 / (1 + 2)) * 3000 queries.
        assertThat(group1Ran).isBetween(995, 1000);
        assertThat(group2Ran).isBetween(1995, 2000);
    }

    @Test
    @Timeout(10)
    public void testWeightedFairSchedulingEqualWeights()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor())
        {
            @Override
            public void triggerProcessQueuedQueries()
            {
                // No op to allow the test fine-grained control about when to trigger the next query.
            }
        };
        root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        root.setMaxQueuedQueries(50);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED_FAIR);

        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group1.setMaxQueuedQueries(50);
        group1.setHardConcurrencyLimit(2);
        group1.setSoftConcurrencyLimit(2);
        group1.setSchedulingWeight(1);

        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group2.setMaxQueuedQueries(50);
        group2.setHardConcurrencyLimit(2);
        group2.setSoftConcurrencyLimit(2);
        group2.setSchedulingWeight(1);

        InternalResourceGroup group3 = root.getOrCreateSubGroup("3");
        group3.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group3.setMaxQueuedQueries(50);
        group3.setHardConcurrencyLimit(2);
        group3.setSoftConcurrencyLimit(2);
        group3.setSchedulingWeight(2);

        Set<MockManagedQueryExecution> group1Queries = fillGroupTo(group1, ImmutableSet.of(), 4);
        Set<MockManagedQueryExecution> group2Queries = fillGroupTo(group2, ImmutableSet.of(), 4);
        Set<MockManagedQueryExecution> group3Queries = fillGroupTo(group3, ImmutableSet.of(), 4);
        root.setHardConcurrencyLimit(4);

        int group1Ran = 0;
        int group2Ran = 0;
        int group3Ran = 0;
        for (int i = 0; i < 1000; i++) {
            group1Ran += completeGroupQueries(group1Queries);
            group2Ran += completeGroupQueries(group2Queries);
            group3Ran += completeGroupQueries(group3Queries);
            root.updateGroupsAndProcessQueuedQueries();
            group1Queries = fillGroupTo(group1, group1Queries, 4);
            group2Queries = fillGroupTo(group2, group2Queries, 4);
            group3Queries = fillGroupTo(group3, group3Queries, 4);
        }

        // group 3 should run approximately 2x the number of queries of 1 and 2
        BinomialDistribution binomial = new BinomialDistribution(4000, 1.0 / 4.0);
        int lowerBound = binomial.inverseCumulativeProbability(0.000001);
        int upperBound = binomial.inverseCumulativeProbability(0.999999);

        assertThat(group1Ran).isBetween(lowerBound, upperBound);
        assertThat(group2Ran).isBetween(lowerBound, upperBound);
        assertThat(group3Ran).isBetween(2 * lowerBound, 2 * upperBound);
    }

    @Test
    @Timeout(10)
    public void testWeightedFairSchedulingNoStarvation()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor())
        {
            @Override
            public void triggerProcessQueuedQueries()
            {
                // No op to allow the test fine-grained control about when to trigger the next query.
            }
        };
        root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        root.setMaxQueuedQueries(50);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED_FAIR);

        InternalResourceGroup group1 = root.getOrCreateSubGroup("1");
        group1.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group1.setMaxQueuedQueries(50);
        group1.setHardConcurrencyLimit(2);
        group1.setSoftConcurrencyLimit(2);
        group1.setSchedulingWeight(1);

        InternalResourceGroup group2 = root.getOrCreateSubGroup("2");
        group2.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        group2.setMaxQueuedQueries(50);
        group2.setHardConcurrencyLimit(2);
        group2.setSoftConcurrencyLimit(2);
        group2.setSchedulingWeight(2);

        Set<MockManagedQueryExecution> group1Queries = fillGroupTo(group1, ImmutableSet.of(), 4);
        Set<MockManagedQueryExecution> group2Queries = fillGroupTo(group2, ImmutableSet.of(), 4);
        root.setHardConcurrencyLimit(1);

        int group1Ran = 0;
        for (int i = 0; i < 2000; i++) {
            group1Ran += completeGroupQueries(group1Queries);
            completeGroupQueries(group2Queries);
            root.updateGroupsAndProcessQueuedQueries();
            group1Queries = fillGroupTo(group1, group1Queries, 4);
            group2Queries = fillGroupTo(group2, group2Queries, 4);
        }

        assertThat(group1Ran).isEqualTo(1000);
        assertThat(group1Ran).isEqualTo(1000);
    }

    @Test
    public void testGetInfo()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor())
        {
            @Override
            public void triggerProcessQueuedQueries()
            {
                // No op to allow the test fine-grained control about when to trigger the next query.
            }
        };
        root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        root.setMaxQueuedQueries(40);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED);

        InternalResourceGroup rootA = root.getOrCreateSubGroup("a");
        rootA.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootA.setMaxQueuedQueries(20);
        rootA.setHardConcurrencyLimit(2);

        InternalResourceGroup rootB = root.getOrCreateSubGroup("b");
        rootB.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootB.setMaxQueuedQueries(20);
        rootB.setHardConcurrencyLimit(2);
        rootB.setSchedulingWeight(2);
        rootB.setSchedulingPolicy(QUERY_PRIORITY);

        InternalResourceGroup rootAX = rootA.getOrCreateSubGroup("x");
        rootAX.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootAX.setMaxQueuedQueries(10);
        rootAX.setHardConcurrencyLimit(10);

        InternalResourceGroup rootAY = rootA.getOrCreateSubGroup("y");
        rootAY.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootAY.setMaxQueuedQueries(10);
        rootAY.setHardConcurrencyLimit(10);

        InternalResourceGroup rootBX = rootB.getOrCreateSubGroup("x");
        rootBX.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootBX.setMaxQueuedQueries(10);
        rootBX.setHardConcurrencyLimit(10);

        InternalResourceGroup rootBY = rootB.getOrCreateSubGroup("y");
        rootBY.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootBY.setMaxQueuedQueries(10);
        rootBY.setHardConcurrencyLimit(10);

        // Queue 40 queries (= maxQueuedQueries (40) + maxRunningQueries (0))
        Set<MockManagedQueryExecution> queries = fillGroupTo(rootAX, ImmutableSet.of(), 10, false);
        queries.addAll(fillGroupTo(rootAY, ImmutableSet.of(), 10, false));
        queries.addAll(fillGroupTo(rootBX, ImmutableSet.of(), 10, true));
        queries.addAll(fillGroupTo(rootBY, ImmutableSet.of(), 10, true));

        ResourceGroupInfo info = root.getInfo();
        assertThat(info.numRunningQueries()).isEqualTo(0);
        assertThat(info.numQueuedQueries()).isEqualTo(40);

        // root.maxRunningQueries = 4, root.a.maxRunningQueries = 2, root.b.maxRunningQueries = 2. Will have 4 queries running and 36 left queued.
        root.setHardConcurrencyLimit(4);
        root.updateGroupsAndProcessQueuedQueries();
        info = root.getInfo();
        assertThat(info.numRunningQueries()).isEqualTo(4);
        assertThat(info.numQueuedQueries()).isEqualTo(36);

        // Complete running queries
        Iterator<MockManagedQueryExecution> iterator = queries.iterator();
        while (iterator.hasNext()) {
            MockManagedQueryExecution query = iterator.next();
            if (query.getState() == RUNNING) {
                query.complete();
                iterator.remove();
            }
        }

        // 4 more queries start running, 32 left queued.
        root.updateGroupsAndProcessQueuedQueries();
        info = root.getInfo();
        assertThat(info.numRunningQueries()).isEqualTo(4);
        assertThat(info.numQueuedQueries()).isEqualTo(32);

        // root.maxRunningQueries = 10, root.a.maxRunningQueries = 2, root.b.maxRunningQueries = 2. Still only have 4 running queries and 32 left queued.
        root.setHardConcurrencyLimit(10);
        root.updateGroupsAndProcessQueuedQueries();
        info = root.getInfo();
        assertThat(info.numRunningQueries()).isEqualTo(4);
        assertThat(info.numQueuedQueries()).isEqualTo(32);

        // root.maxRunningQueries = 10, root.a.maxRunningQueries = 2, root.b.maxRunningQueries = 10. Will have 10 running queries and 26 left queued.
        rootB.setHardConcurrencyLimit(10);
        root.updateGroupsAndProcessQueuedQueries();
        info = root.getInfo();
        assertThat(info.numRunningQueries()).isEqualTo(10);
        assertThat(info.numQueuedQueries()).isEqualTo(26);
    }

    @Test
    public void testGetResourceGroupStateInfo()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimitBytes(DataSize.of(1, GIGABYTE).toBytes());
        root.setMaxQueuedQueries(40);
        root.setHardConcurrencyLimit(10);
        root.setSchedulingPolicy(WEIGHTED);

        InternalResourceGroup rootA = root.getOrCreateSubGroup("a");
        rootA.setSoftMemoryLimitBytes(DataSize.of(10, MEGABYTE).toBytes());
        rootA.setMaxQueuedQueries(20);
        rootA.setHardConcurrencyLimit(0);

        InternalResourceGroup rootB = root.getOrCreateSubGroup("b");
        rootB.setSoftMemoryLimitBytes(DataSize.of(5, MEGABYTE).toBytes());
        rootB.setMaxQueuedQueries(20);
        rootB.setHardConcurrencyLimit(1);
        rootB.setSchedulingWeight(2);
        rootB.setSchedulingPolicy(QUERY_PRIORITY);

        InternalResourceGroup rootAX = rootA.getOrCreateSubGroup("x");
        rootAX.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootAX.setMaxQueuedQueries(10);
        rootAX.setHardConcurrencyLimit(10);

        InternalResourceGroup rootAY = rootA.getOrCreateSubGroup("y");
        rootAY.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootAY.setMaxQueuedQueries(10);
        rootAY.setHardConcurrencyLimit(10);

        Set<MockManagedQueryExecution> queries = fillGroupTo(rootAX, ImmutableSet.of(), 5, false);
        queries.addAll(fillGroupTo(rootAY, ImmutableSet.of(), 5, false));
        queries.addAll(fillGroupTo(rootB, ImmutableSet.of(), 10, true));

        ResourceGroupInfo rootInfo = root.getFullInfo();
        assertThat(rootInfo.id()).isEqualTo(root.getId());
        assertThat(rootInfo.state()).isEqualTo(CAN_RUN);
        assertThat(rootInfo.softMemoryLimit().toBytes()).isEqualTo(root.getSoftMemoryLimitBytes());
        assertThat(rootInfo.memoryUsage()).isEqualTo(DataSize.ofBytes(0));
        assertThat(rootInfo.cpuUsage().toMillis()).isEqualTo(0);
        List<ResourceGroupInfo> subGroups = rootInfo.subGroups().get();
        assertThat(subGroups).hasSize(2);
        assertGroupInfoEquals(subGroups.get(0), rootA.getInfo());
        assertThat(subGroups.get(0).id()).isEqualTo(rootA.getId());
        assertThat(subGroups.get(0).state()).isEqualTo(CAN_QUEUE);
        assertThat(subGroups.get(0).softMemoryLimit().toBytes()).isEqualTo(rootA.getSoftMemoryLimitBytes());
        assertThat(subGroups.get(0).hardConcurrencyLimit()).isEqualTo(rootA.getHardConcurrencyLimit());
        assertThat(subGroups.get(0).maxQueuedQueries()).isEqualTo(rootA.getMaxQueuedQueries());
        assertThat(subGroups.get(0).numEligibleSubGroups()).isEqualTo(2);
        assertThat(subGroups.get(0).numRunningQueries()).isEqualTo(0);
        assertThat(subGroups.get(0).numQueuedQueries()).isEqualTo(10);
        assertGroupInfoEquals(subGroups.get(1), rootB.getInfo());
        assertThat(subGroups.get(1).id()).isEqualTo(rootB.getId());
        assertThat(subGroups.get(1).state()).isEqualTo(CAN_QUEUE);
        assertThat(subGroups.get(1).softMemoryLimit().toBytes()).isEqualTo(rootB.getSoftMemoryLimitBytes());
        assertThat(subGroups.get(1).hardConcurrencyLimit()).isEqualTo(rootB.getHardConcurrencyLimit());
        assertThat(subGroups.get(1).maxQueuedQueries()).isEqualTo(rootB.getMaxQueuedQueries());
        assertThat(subGroups.get(1).numEligibleSubGroups()).isEqualTo(0);
        assertThat(subGroups.get(1).numRunningQueries()).isEqualTo(1);
        assertThat(subGroups.get(1).numQueuedQueries()).isEqualTo(9);
        assertThat(rootInfo.softConcurrencyLimit()).isEqualTo(root.getSoftConcurrencyLimit());
        assertThat(rootInfo.hardConcurrencyLimit()).isEqualTo(root.getHardConcurrencyLimit());
        assertThat(rootInfo.maxQueuedQueries()).isEqualTo(root.getMaxQueuedQueries());
        assertThat(rootInfo.numQueuedQueries()).isEqualTo(19);
        List<QueryStateInfo> runningQueries = rootInfo.runningQueries().get();
        assertThat(runningQueries).hasSize(1);
        QueryStateInfo queryInfo = runningQueries.get(0);
        assertThat(queryInfo.getResourceGroupId()).isEqualTo(Optional.of(rootB.getId()));
    }

    @Test
    public void testStartedQueries()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (_, _) -> {}, directExecutor())
        {
            @Override
            public void triggerProcessQueuedQueries()
            {
                // No op to allow the test fine-grained control about when to trigger the next query.
            }
        };
        InternalResourceGroup rootA = root.getOrCreateSubGroup("a");
        InternalResourceGroup rootA1 = rootA.getOrCreateSubGroup("1");
        InternalResourceGroup rootB = root.getOrCreateSubGroup("b");

        List<InternalResourceGroup> allGroups = List.of(root, rootB, rootA, rootA1);
        allGroups.forEach(group -> {
            group.setHardConcurrencyLimit(2);
            group.setMaxQueuedQueries(100);
        });

        MockManagedQueryExecution[] queries = Stream
                .generate(() -> new MockManagedQueryExecutionBuilder().build())
                .limit(4)
                .toArray(MockManagedQueryExecution[]::new);

        rootB.run(queries[0]);
        // no values yet since there is no previous start time to compare against
        assertThat(allGroups).extracting(group -> group.getStartedQueries().getTotalCount()).containsExactly(1L, 1L, 0L, 0L);

        rootA1.run(queries[1]);
        assertThat(allGroups).extracting(group -> group.getStartedQueries().getTotalCount()).containsExactly(2L, 1L, 1L, 1L);

        // these should queue
        rootA1.run(queries[2]);
        rootA1.run(queries[3]);
        assertThat(allGroups).extracting(group -> group.getStartedQueries().getTotalCount()).containsExactly(2L, 1L, 1L, 1L);

        // let q3/q4 run by draining q1/q2
        queries[0].complete();
        queries[1].complete();
        root.updateGroupsAndProcessQueuedQueries();
        assertThat(allGroups).extracting(group -> group.getStartedQueries().getTotalCount()).containsExactly(4L, 1L, 3L, 3L);
    }

    @Test
    public void testGetWaitingQueuedQueries()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        root.setMaxQueuedQueries(40);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);

        InternalResourceGroup rootA = root.getOrCreateSubGroup("a");
        rootA.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootA.setMaxQueuedQueries(20);
        rootA.setHardConcurrencyLimit(8);

        InternalResourceGroup rootAX = rootA.getOrCreateSubGroup("x");
        rootAX.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootAX.setMaxQueuedQueries(10);
        rootAX.setHardConcurrencyLimit(8);

        InternalResourceGroup rootAY = rootA.getOrCreateSubGroup("y");
        rootAY.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootAY.setMaxQueuedQueries(10);
        rootAY.setHardConcurrencyLimit(5);

        InternalResourceGroup rootB = root.getOrCreateSubGroup("b");
        rootB.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootB.setMaxQueuedQueries(20);
        rootB.setHardConcurrencyLimit(8);

        InternalResourceGroup rootBX = rootB.getOrCreateSubGroup("x");
        rootBX.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootBX.setMaxQueuedQueries(10);
        rootBX.setHardConcurrencyLimit(8);

        InternalResourceGroup rootBY = rootB.getOrCreateSubGroup("y");
        rootBY.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootBY.setMaxQueuedQueries(10);
        rootBY.setHardConcurrencyLimit(5);

        // Queue 40 queries (= maxQueuedQueries (40) + maxRunningQueries (0))
        Set<MockManagedQueryExecution> queries = fillGroupTo(rootAX, ImmutableSet.of(), 10, false);
        queries.addAll(fillGroupTo(rootAY, ImmutableSet.of(), 10, false));
        queries.addAll(fillGroupTo(rootBX, ImmutableSet.of(), 10, true));
        queries.addAll(fillGroupTo(rootBY, ImmutableSet.of(), 10, true));

        assertThat(root.getWaitingQueuedQueries()).isEqualTo(16);
        assertThat(rootA.getWaitingQueuedQueries()).isEqualTo(13);
        assertThat(rootAX.getWaitingQueuedQueries()).isEqualTo(10);
        assertThat(rootAY.getWaitingQueuedQueries()).isEqualTo(10);
        assertThat(rootB.getWaitingQueuedQueries()).isEqualTo(13);
        assertThat(rootBX.getWaitingQueuedQueries()).isEqualTo(10);
        assertThat(rootBY.getWaitingQueuedQueries()).isEqualTo(10);

        root.setHardConcurrencyLimit(20);
        root.updateGroupsAndProcessQueuedQueries();
        assertThat(root.getWaitingQueuedQueries()).isEqualTo(0);
        assertThat(rootA.getWaitingQueuedQueries()).isEqualTo(5);
        assertThat(rootAX.getWaitingQueuedQueries()).isEqualTo(6);
        assertThat(rootAY.getWaitingQueuedQueries()).isEqualTo(6);
        assertThat(rootB.getWaitingQueuedQueries()).isEqualTo(5);
        assertThat(rootBX.getWaitingQueuedQueries()).isEqualTo(6);
        assertThat(rootBY.getWaitingQueuedQueries()).isEqualTo(6);
    }

    @Test
    public void testGetQueriesQueuedOnInternal()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (_, _) -> {}, directExecutor());
        root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        root.setMaxQueuedQueries(40);
        // Start with zero capacity, so that nothing starts running until we've added all the queries
        root.setHardConcurrencyLimit(0);
        root.setSoftConcurrencyLimit(0);

        InternalResourceGroup rootA = root.getOrCreateSubGroup("a");
        rootA.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootA.setMaxQueuedQueries(20);
        rootA.setHardConcurrencyLimit(8);
        rootA.setSoftConcurrencyLimit(8);

        InternalResourceGroup rootAX = rootA.getOrCreateSubGroup("x");
        rootAX.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootAX.setMaxQueuedQueries(10);
        rootAX.setHardConcurrencyLimit(8);
        rootAX.setSoftConcurrencyLimit(8);

        InternalResourceGroup rootAY = rootA.getOrCreateSubGroup("y");
        rootAY.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootAY.setMaxQueuedQueries(10);
        rootAY.setHardConcurrencyLimit(5);
        rootAY.setSoftConcurrencyLimit(5);

        InternalResourceGroup rootB = root.getOrCreateSubGroup("b");
        rootB.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootB.setMaxQueuedQueries(20);
        rootB.setHardConcurrencyLimit(8);
        rootB.setSoftConcurrencyLimit(8);

        InternalResourceGroup rootBX = rootB.getOrCreateSubGroup("x");
        rootBX.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootBX.setMaxQueuedQueries(10);
        rootBX.setHardConcurrencyLimit(8);
        rootBX.setSoftConcurrencyLimit(8);

        InternalResourceGroup rootBY = rootB.getOrCreateSubGroup("y");
        rootBY.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootBY.setMaxQueuedQueries(10);
        rootBY.setHardConcurrencyLimit(5);
        rootBY.setSoftConcurrencyLimit(5);

        fillGroupTo(rootAX, ImmutableSet.of(), 10, false);
        fillGroupTo(rootAY, ImmutableSet.of(), 10, false);
        fillGroupTo(rootBX, ImmutableSet.of(), 10, true);
        fillGroupTo(rootBY, ImmutableSet.of(), 10, true);

        assertThat(root.getQueriesQueuedOnInternal()).isEqualTo(26);
        assertThat(rootA.getQueriesQueuedOnInternal()).isEqualTo(13);
        assertThat(rootAX.getQueriesQueuedOnInternal()).isEqualTo(8);
        assertThat(rootAY.getQueriesQueuedOnInternal()).isEqualTo(5);
        assertThat(rootB.getQueriesQueuedOnInternal()).isEqualTo(13);
        assertThat(rootBX.getQueriesQueuedOnInternal()).isEqualTo(8);
        assertThat(rootBY.getQueriesQueuedOnInternal()).isEqualTo(5);

        root.setHardConcurrencyLimit(20);
        root.updateGroupsAndProcessQueuedQueries();

        assertThat(root.getQueriesQueuedOnInternal()).isEqualTo(10);
        assertThat(rootA.getQueriesQueuedOnInternal()).isEqualTo(5);
        assertThat(rootAX.getQueriesQueuedOnInternal()).isEqualTo(4);
        assertThat(rootAY.getQueriesQueuedOnInternal()).isEqualTo(1);
        assertThat(rootB.getQueriesQueuedOnInternal()).isEqualTo(5);
        assertThat(rootBX.getQueriesQueuedOnInternal()).isEqualTo(4);
        assertThat(rootBY.getQueriesQueuedOnInternal()).isEqualTo(1);
    }

    @Test
    public void testGetWaitingQueuedQueriesWithDisabledGroup()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (_, _) -> {}, directExecutor());
        root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        root.setMaxQueuedQueries(40);
        root.setHardConcurrencyLimit(20);

        InternalResourceGroup rootA = root.getOrCreateSubGroup("a");
        rootA.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootA.setMaxQueuedQueries(20);
        rootA.setHardConcurrencyLimit(15);

        InternalResourceGroup rootAX = rootA.getOrCreateSubGroup("x");
        rootAX.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootAX.setMaxQueuedQueries(10);
        rootAX.setHardConcurrencyLimit(10);

        InternalResourceGroup rootB = root.getOrCreateSubGroup("b");
        rootB.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootB.setMaxQueuedQueries(20);
        rootB.setHardConcurrencyLimit(15);

        fillGroupTo(rootB, ImmutableSet.of(), 8, false);
        fillGroupTo(rootAX, ImmutableSet.of(), 6, false);
        rootAX.setDisabled(true);
        fillGroupTo(rootA, ImmutableSet.of(), 20, false);

        // Since there are 6 running queries in the group 'root.a.x', the group 'root.a',
        // which is now a leaf, can only run 9 queries even though its concurrency limit
        // is set to 15. However, it is currently running only 6 queries with 14 queued,
        // because its parent group has reached its concurrency limit (20), preventing
        // 'root.a' from running the additional 3 queries.
        assertThat(root.getWaitingQueuedQueries()).isEqualTo(3);
        assertThat(root.getQueuedQueries()).isEqualTo(14);
        assertThat(rootA.getWaitingQueuedQueries()).isEqualTo(14);
        assertThat(rootA.getQueuedQueries()).isEqualTo(14);
        assertThat(rootAX.getWaitingQueuedQueries()).isEqualTo(0);
        assertThat(rootAX.getQueuedQueries()).isEqualTo(0);
        assertThat(rootB.getWaitingQueuedQueries()).isEqualTo(0);
        assertThat(rootB.getQueuedQueries()).isEqualTo(0);
    }

    @Test
    public void testGetQueriesQueuedOnInternalWithDisabledGroup()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (_, _) -> {}, directExecutor());
        root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        root.setMaxQueuedQueries(40);
        root.setHardConcurrencyLimit(20);
        root.setSoftConcurrencyLimit(20);

        InternalResourceGroup rootA = root.getOrCreateSubGroup("a");
        rootA.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootA.setMaxQueuedQueries(20);
        rootA.setHardConcurrencyLimit(15);
        rootA.setSoftConcurrencyLimit(15);

        InternalResourceGroup rootAX = rootA.getOrCreateSubGroup("x");
        rootAX.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootAX.setMaxQueuedQueries(10);
        rootAX.setHardConcurrencyLimit(10);
        rootAX.setSoftConcurrencyLimit(10);

        InternalResourceGroup rootB = root.getOrCreateSubGroup("b");
        rootB.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootB.setMaxQueuedQueries(20);
        rootB.setHardConcurrencyLimit(15);
        rootB.setSoftConcurrencyLimit(15);

        fillGroupTo(rootB, ImmutableSet.of(), 8, false);
        fillGroupTo(rootAX, ImmutableSet.of(), 6, false);
        rootAX.setDisabled(true);
        fillGroupTo(rootA, ImmutableSet.of(), 20, false);

        // Since there are 6 running queries in the group 'root.a.x', the group 'root.a',
        // which is now a leaf, can only run 9 queries even though its concurrency limit
        // is set to 15. However, it is currently running only 6 queries with 14 queued,
        // because its parent group has reached its concurrency limit (20), preventing
        // 'root.a' from running the additional 3 queries.
        assertThat(root.getQueriesQueuedOnInternal()).isEqualTo(3);
        assertThat(root.getQueuedQueries()).isEqualTo(14);
        assertThat(rootA.getQueriesQueuedOnInternal()).isEqualTo(3);
        assertThat(rootA.getQueuedQueries()).isEqualTo(14);
        assertThat(rootAX.getQueriesQueuedOnInternal()).isEqualTo(0);
        assertThat(rootAX.getQueuedQueries()).isEqualTo(0);
        assertThat(rootB.getQueriesQueuedOnInternal()).isEqualTo(0);
        assertThat(rootB.getQueuedQueries()).isEqualTo(0);
    }

    private static int completeGroupQueries(Set<MockManagedQueryExecution> groupQueries)
    {
        int groupRan = 0;
        for (Iterator<MockManagedQueryExecution> iterator = groupQueries.iterator(); iterator.hasNext(); ) {
            MockManagedQueryExecution query = iterator.next();
            if (query.getState() == RUNNING) {
                query.complete();
                iterator.remove();
                groupRan++;
            }
        }
        return groupRan;
    }

    private static Set<MockManagedQueryExecution> fillGroupTo(InternalResourceGroup group, Set<MockManagedQueryExecution> existingQueries, int count)
    {
        return fillGroupTo(group, existingQueries, count, false);
    }

    private static Set<MockManagedQueryExecution> fillGroupTo(InternalResourceGroup group, Set<MockManagedQueryExecution> existingQueries, int count, boolean queryPriority)
    {
        int existingCount = existingQueries.size();
        Set<MockManagedQueryExecution> queries = new HashSet<>(existingQueries);
        for (int i = 0; i < count - existingCount; i++) {
            MockManagedQueryExecution query = new MockManagedQueryExecutionBuilder()
                    .withQueryId(group.getId().toString().replace(".", "") + Integer.toString(i))
                    .withPriority(queryPriority ? i + 1 : 1)
                    .build();

            queries.add(query);
            group.run(query);
        }
        return queries;
    }

    private static void assertGroupInfoEquals(ResourceGroupInfo actual, ResourceGroupInfo expected)
    {
        assertThat(actual.schedulingWeight() == expected.schedulingWeight() &&
                actual.softConcurrencyLimit() == expected.softConcurrencyLimit() &&
                actual.hardConcurrencyLimit() == expected.hardConcurrencyLimit() &&
                actual.maxQueuedQueries() == expected.maxQueuedQueries() &&
                actual.numQueuedQueries() == expected.numQueuedQueries() &&
                actual.numRunningQueries() == expected.numRunningQueries() &&
                actual.numEligibleSubGroups() == expected.numEligibleSubGroups() &&
                Objects.equals(actual.id(), expected.id()) &&
                actual.state() == expected.state() &&
                actual.schedulingPolicy() == expected.schedulingPolicy() &&
                Objects.equals(actual.softMemoryLimit(), expected.softMemoryLimit()) &&
                Objects.equals(actual.memoryUsage(), expected.memoryUsage()) &&
                Objects.equals(actual.cpuUsage(), expected.cpuUsage())).isTrue();
    }

    private static void assertExceedsCpuLimit(InternalResourceGroup group, long expectedMillis)
    {
        long actualMillis = group.getResourceUsageSnapshot().getCpuUsageMillis();
        assertThat(actualMillis).isEqualTo(expectedMillis);
        assertThat(actualMillis >= group.getHardCpuLimit().toMillis()).isTrue();
        assertThat(group.getCpuUsageMillis()).isEqualTo(expectedMillis);
    }

    private static void assertWithinCpuLimit(InternalResourceGroup group, long expectedMillis)
    {
        long actualMillis = group.getResourceUsageSnapshot().getCpuUsageMillis();
        assertThat(actualMillis).isEqualTo(expectedMillis);
        assertThat(actualMillis < group.getHardCpuLimit().toMillis()).isTrue();
        assertThat(group.getCpuUsageMillis()).isEqualTo(expectedMillis);
    }

    private static void assertExceedsMemoryLimit(InternalResourceGroup group, long expectedBytes)
    {
        long actualBytes = group.getResourceUsageSnapshot().getMemoryUsageBytes();
        assertThat(actualBytes).isEqualTo(expectedBytes);
        assertThat(actualBytes).isGreaterThan(group.getSoftMemoryLimitBytes());
        assertThat(group.getMemoryUsageBytes()).isEqualTo(expectedBytes);
    }

    private static void assertWithinMemoryLimit(InternalResourceGroup group, long expectedBytes)
    {
        long actualBytes = group.getResourceUsageSnapshot().getMemoryUsageBytes();
        assertThat(actualBytes).isEqualTo(expectedBytes);
        assertThat(actualBytes).isLessThanOrEqualTo(group.getSoftMemoryLimitBytes());
        assertThat(group.getMemoryUsageBytes()).isEqualTo(expectedBytes);
    }
}
