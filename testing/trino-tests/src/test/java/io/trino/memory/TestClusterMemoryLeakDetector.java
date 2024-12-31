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
package io.trino.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.QueryState;
import io.trino.operator.RetryPolicy;
import io.trino.server.BasicQueryInfo;
import io.trino.server.BasicQueryStats;
import io.trino.spi.QueryId;
import io.trino.spi.resourcegroups.ResourceGroupId;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Optional;
import java.util.OptionalDouble;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.operator.BlockedReason.WAITING_FOR_MEMORY;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestClusterMemoryLeakDetector
{
    @Test
    public void testLeakDetector()
    {
        QueryId testQuery = new QueryId("test");
        ClusterMemoryLeakDetector leakDetector = new ClusterMemoryLeakDetector();

        leakDetector.checkForMemoryLeaks(ImmutableList::of, ImmutableMap.of());
        assertThat(leakDetector.getNumberOfLeakedQueries()).isEqualTo(0);

        // the leak detector should report no leaked queries as the query is still running
        leakDetector.checkForMemoryLeaks(() -> ImmutableList.of(createQueryInfo(testQuery.getId(), RUNNING)), ImmutableMap.of(testQuery, 1L));
        assertThat(leakDetector.getNumberOfLeakedQueries()).isEqualTo(0);

        // the leak detector should report exactly one leaked query since the query is finished, and its end time is way in the past
        leakDetector.checkForMemoryLeaks(() -> ImmutableList.of(createQueryInfo(testQuery.getId(), FINISHED)), ImmutableMap.of(testQuery, 1L));
        assertThat(leakDetector.getNumberOfLeakedQueries()).isEqualTo(1);

        // the leak detector should report no leaked queries as the query doesn't have any memory reservation
        leakDetector.checkForMemoryLeaks(() -> ImmutableList.of(createQueryInfo(testQuery.getId(), FINISHED)), ImmutableMap.of(testQuery, 0L));
        assertThat(leakDetector.getNumberOfLeakedQueries()).isEqualTo(0);

        // the leak detector should report exactly one leaked query since the coordinator doesn't know of any query
        leakDetector.checkForMemoryLeaks(ImmutableList::of, ImmutableMap.of(testQuery, 1L));
        assertThat(leakDetector.getNumberOfLeakedQueries()).isEqualTo(1);
    }

    private static BasicQueryInfo createQueryInfo(String queryId, QueryState state)
    {
        return new BasicQueryInfo(
                new QueryId(queryId),
                TEST_SESSION.toSessionRepresentation(),
                Optional.of(new ResourceGroupId("global")),
                state,
                true,
                URI.create("1"),
                "",
                Optional.empty(),
                Optional.empty(),
                new BasicQueryStats(
                        DateTime.parse("1991-09-06T05:00-05:30"),
                        DateTime.parse("1991-09-06T05:01-05:30"),
                        new Duration(8, MINUTES),
                        new Duration(7, MINUTES),
                        new Duration(34, MINUTES),
                        99,
                        13,
                        14,
                        15,
                        100,
                        0,
                        DataSize.valueOf("21GB"),
                        22,
                        DataSize.valueOf("23GB"),
                        DataSize.valueOf("23GB"),
                        DataSize.valueOf("23GB"),
                        DataSize.valueOf("23GB"),
                        24,
                        25,
                        DataSize.valueOf("26GB"),
                        DataSize.valueOf("27GB"),
                        DataSize.valueOf("28GB"),
                        DataSize.valueOf("29GB"),
                        new Duration(30, MINUTES),
                        new Duration(31, MINUTES),
                        new Duration(32, MINUTES),
                        new Duration(33, MINUTES),
                        new Duration(34, MINUTES),
                        new Duration(35, MINUTES),
                        new Duration(36, MINUTES),
                        new Duration(37, MINUTES),
                        true,
                        ImmutableSet.of(WAITING_FOR_MEMORY),
                        OptionalDouble.of(20),
                        OptionalDouble.of(0)),
                null,
                null,
                Optional.empty(),
                RetryPolicy.NONE);
    }
}
