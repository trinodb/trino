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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.QueryId;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.memory.LowMemoryKillerTestingUtils.toNodeMemoryInfoList;
import static io.trino.memory.LowMemoryKillerTestingUtils.toRunningQueryInfoList;
import static org.testng.Assert.assertEquals;

public class TestTotalReservationOnBlockedNodesQueryLowMemoryKiller
{
    private final LowMemoryKiller lowMemoryKiller = new TotalReservationOnBlockedNodesQueryLowMemoryKiller();

    @Test
    public void testMemoryPoolHasNoReservation()
    {
        int memoryPool = 12;
        Map<String, Map<String, Long>> queries = ImmutableMap.of(
                "q_1",
                ImmutableMap.of("n1", 0L, "n2", 0L, "n3", 0L, "n4", 0L, "n5", 0L));

        assertEquals(
                lowMemoryKiller.chooseTargetToKill(
                        toRunningQueryInfoList(queries),
                        toNodeMemoryInfoList(memoryPool, queries)),
                Optional.empty());
    }

    @Test
    public void testMemoryPoolNotBlocked()
    {
        int memoryPool = 12;
        Map<String, Map<String, Long>> queries = ImmutableMap.<String, Map<String, Long>>builder()
                .put("q_1", ImmutableMap.of("n1", 0L, "n2", 6L, "n3", 0L, "n4", 0L, "n5", 0L))
                .put("q_2", ImmutableMap.of("n1", 3L, "n2", 5L, "n3", 2L, "n4", 4L, "n5", 0L))
                .buildOrThrow();
        assertEquals(
                lowMemoryKiller.chooseTargetToKill(
                        toRunningQueryInfoList(queries),
                        toNodeMemoryInfoList(memoryPool, queries)),
                Optional.empty());
    }

    @Test
    public void testSkewedQuery()
    {
        int memoryPool = 12;
        // q1 is neither the query with the most total memory reservation, nor the query with the max memory reservation.
        // This also tests the corner case where a node has an empty memory pool
        Map<String, Map<String, Long>> queries = ImmutableMap.<String, Map<String, Long>>builder()
                .put("q_1", ImmutableMap.of("n1", 0L, "n2", 8L, "n3", 0L, "n4", 0L, "n5", 0L))
                .put("q_2", ImmutableMap.of("n1", 3L, "n2", 5L, "n3", 2L, "n4", 4L, "n5", 0L))
                .put("q_3", ImmutableMap.of("n1", 0L, "n2", 0L, "n3", 9L, "n4", 0L, "n5", 0L))
                .buildOrThrow();
        assertEquals(
                lowMemoryKiller.chooseTargetToKill(
                        toRunningQueryInfoList(queries),
                        toNodeMemoryInfoList(memoryPool, queries)),
                Optional.of(KillTarget.wholeQuery(new QueryId("q_1"))));
    }

    @Test
    public void testWillNotKillWholeQueryWithTaskRetries()
    {
        int memoryPool = 12;
        Map<String, Map<String, Long>> queries = ImmutableMap.<String, Map<String, Long>>builder()
                .put("q_1", ImmutableMap.of("n1", 0L, "n2", 5L, "n3", 0L, "n4", 0L, "n5", 0L))
                .put("q_2", ImmutableMap.of("n1", 3L, "n2", 8L, "n3", 2L, "n4", 4L, "n5", 0L))
                .put("q_3", ImmutableMap.of("n1", 0L, "n2", 0L, "n3", 3L, "n4", 0L, "n5", 0L))
                .buildOrThrow();

        Map<String, Map<String, Map<Integer, Long>>> tasks = ImmutableMap.of(
                "q_2",
                ImmutableMap.of(
                        "n1", ImmutableMap.of(1, 1L, 2, 3L),
                        "n2", ImmutableMap.of(), // no tasks reported for q_2 here even though based on per-query reservation it uses 8 here (and is biggest query)
                        "n3", ImmutableMap.of(6, 2L),
                        "n4", ImmutableMap.of(7, 2L, 8, 2L),
                        "n5", ImmutableMap.of()));

        // we expect "q_1" to be killed even though "q_2" is the biggest query here. We won't kill whole query if it has task retries enabled.

        assertEquals(
                lowMemoryKiller.chooseTargetToKill(
                        toRunningQueryInfoList(queries, ImmutableSet.of("q_2")),
                        toNodeMemoryInfoList(memoryPool, queries, tasks)),
                Optional.of(KillTarget.wholeQuery(new QueryId("q_1"))));
    }
}
