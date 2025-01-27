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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.client.NodeVersion;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStats;
import io.trino.execution.resourcegroups.InternalResourceGroup;
import io.trino.operator.RetryPolicy;
import io.trino.spi.QueryId;
import io.trino.spi.resourcegroups.QueryType;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.operator.BlockedReason.WAITING_FOR_MEMORY;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static io.trino.server.QueryStateInfo.createQueuedQueryStateInfo;
import static io.trino.spi.resourcegroups.SchedulingPolicy.WEIGHTED;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryStateInfo
{
    @Test
    public void testQueryStateInfo()
    {
        InternalResourceGroup root = new InternalResourceGroup("root", (group, export) -> {}, directExecutor());
        root.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        root.setMaxQueuedQueries(40);
        root.setHardConcurrencyLimit(0);
        root.setSchedulingPolicy(WEIGHTED);

        InternalResourceGroup rootA = root.getOrCreateSubGroup("a");
        rootA.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootA.setMaxQueuedQueries(20);
        rootA.setHardConcurrencyLimit(0);

        InternalResourceGroup rootAX = rootA.getOrCreateSubGroup("x");
        rootAX.setSoftMemoryLimitBytes(DataSize.of(1, MEGABYTE).toBytes());
        rootAX.setMaxQueuedQueries(10);
        rootAX.setHardConcurrencyLimit(0);

        // Verify QueryStateInfo for query queued on resource group root.a.y
        QueryStateInfo query = createQueuedQueryStateInfo(
                new BasicQueryInfo(createQueryInfo("query_root_a_x", QUEUED, "SELECT 1")),
                Optional.of(rootAX.getId()),
                Optional.of(ImmutableList.of(rootAX.getInfo(), rootA.getInfo(), root.getInfo())));

        assertThat(query.getQuery()).isEqualTo("SELECT 1");
        assertThat(query.getQueryId().toString()).isEqualTo("query_root_a_x");
        assertThat(query.getQueryState()).isEqualTo(QUEUED);
        assertThat(query.getProgress()).isEqualTo(Optional.empty());

        List<ResourceGroupInfo> chainInfo = query.getPathToRoot().get();

        assertThat(chainInfo).hasSize(3);

        ResourceGroupInfo rootAInfo = chainInfo.get(1);
        ResourceGroupInfo expectedRootAInfo = rootA.getInfo();
        assertThat(rootAInfo.id()).isEqualTo(expectedRootAInfo.id());
        assertThat(rootAInfo.state()).isEqualTo(expectedRootAInfo.state());
        assertThat(rootAInfo.numRunningQueries()).isEqualTo(expectedRootAInfo.numRunningQueries());
        assertThat(rootAInfo.numQueuedQueries()).isEqualTo(expectedRootAInfo.numQueuedQueries());

        ResourceGroupInfo actualRootInfo = chainInfo.get(2);
        ResourceGroupInfo expectedRootInfo = root.getInfo();
        assertThat(actualRootInfo.id()).isEqualTo(expectedRootInfo.id());
        assertThat(actualRootInfo.state()).isEqualTo(expectedRootInfo.state());
        assertThat(actualRootInfo.numRunningQueries()).isEqualTo(expectedRootInfo.numRunningQueries());
        assertThat(actualRootInfo.numQueuedQueries()).isEqualTo(expectedRootInfo.numQueuedQueries());
    }

    private QueryInfo createQueryInfo(String queryId, QueryState state, String query)
    {
        return new QueryInfo(
                new QueryId(queryId),
                TEST_SESSION.toSessionRepresentation(),
                state,
                URI.create("1"),
                ImmutableList.of("2", "3"),
                query,
                Optional.empty(),
                new QueryStats(
                        DateTime.parse("1991-09-06T05:00-05:30"),
                        DateTime.parse("1991-09-06T05:01-05:30"),
                        DateTime.parse("1991-09-06T05:02-05:30"),
                        DateTime.parse("1991-09-06T06:00-05:30"),
                        new Duration(10, SECONDS),
                        new Duration(8, MINUTES),
                        new Duration(7, MINUTES),
                        new Duration(34, MINUTES),
                        new Duration(9, MINUTES),
                        new Duration(10, MINUTES),
                        new Duration(11, MINUTES),
                        new Duration(1, SECONDS),
                        new Duration(2, SECONDS),
                        new Duration(12, MINUTES),
                        13,
                        14,
                        15,
                        16,
                        100,
                        17,
                        18,
                        34,
                        19,
                        20.0,
                        21.0,
                        DataSize.valueOf("21GB"),
                        DataSize.valueOf("22GB"),
                        DataSize.valueOf("23GB"),
                        DataSize.valueOf("24GB"),
                        DataSize.valueOf("25GB"),
                        DataSize.valueOf("26GB"),
                        DataSize.valueOf("27GB"),
                        DataSize.valueOf("28GB"),
                        DataSize.valueOf("29GB"),
                        true,
                        OptionalDouble.of(8.88),
                        OptionalDouble.of(0),
                        new Duration(23, MINUTES),
                        new Duration(24, MINUTES),
                        new Duration(25, MINUTES),
                        new Duration(26, MINUTES),
                        new Duration(27, MINUTES),
                        true,
                        ImmutableSet.of(WAITING_FOR_MEMORY),
                        DataSize.valueOf("271GB"),
                        DataSize.valueOf("272GB"),
                        281,
                        282,
                        new Duration(28, MINUTES),
                        new Duration(29, MINUTES),
                        DataSize.valueOf("273GB"),
                        DataSize.valueOf("274GB"),
                        283,
                        284,
                        DataSize.valueOf("28GB"),
                        DataSize.valueOf("29GB"),
                        30,
                        31,
                        DataSize.valueOf("32GB"),
                        DataSize.valueOf("33GB"),
                        34,
                        35,
                        new Duration(101, SECONDS),
                        new Duration(102, SECONDS),
                        DataSize.valueOf("36GB"),
                        DataSize.valueOf("37GB"),
                        38,
                        0,
                        39,
                        new Duration(103, SECONDS),
                        new Duration(104, SECONDS),
                        DataSize.valueOf("40GB"),
                        DataSize.valueOf("41GB"),
                        ImmutableList.of(),
                        DynamicFiltersStats.EMPTY,
                        ImmutableList.of(),
                        ImmutableList.of()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                "42",
                Optional.empty(),
                null,
                null,
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                false,
                Optional.empty(),
                Optional.of(QueryType.SELECT),
                RetryPolicy.NONE,
                false,
                new NodeVersion("version"));
    }
}
