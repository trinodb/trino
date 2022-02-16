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
import io.trino.spi.QueryId;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.memory.LowMemoryKillerTestingUtils.toNodeMemoryInfoList;
import static io.trino.memory.LowMemoryKillerTestingUtils.toQueryMemoryInfoList;
import static io.trino.testing.assertions.Assert.assertEquals;

public class TestTotalReservationOnBlockedNodesLowMemoryKiller
{
    private final LowMemoryKiller lowMemoryKiller = new TotalReservationOnBlockedNodesLowMemoryKiller();

    @Test
    public void testGeneralPoolHasNoReservation()
    {
        int generalPool = 12;
        Map<String, Map<String, Long>> queries = ImmutableMap.<String, Map<String, Long>>builder()
                .put("q_1", ImmutableMap.of("n1", 0L, "n2", 0L, "n3", 0L, "n4", 0L, "n5", 0L))
                .buildOrThrow();
        assertEquals(
                lowMemoryKiller.chooseQueryToKill(
                        toQueryMemoryInfoList(queries),
                        toNodeMemoryInfoList(generalPool, queries)),
                Optional.empty());
    }

    @Test
    public void testGeneralPoolNotBlocked()
    {
        int generalPool = 12;
        Map<String, Map<String, Long>> queries = ImmutableMap.<String, Map<String, Long>>builder()
                .put("q_1", ImmutableMap.of("n1", 0L, "n2", 6L, "n3", 0L, "n4", 0L, "n5", 0L))
                .put("q_2", ImmutableMap.of("n1", 3L, "n2", 5L, "n3", 2L, "n4", 4L, "n5", 0L))
                .buildOrThrow();
        assertEquals(
                lowMemoryKiller.chooseQueryToKill(
                        toQueryMemoryInfoList(queries),
                        toNodeMemoryInfoList(generalPool, queries)),
                Optional.empty());
    }

    @Test
    public void testSkewedQuery()
    {
        int generalPool = 12;
        // q1 is neither the query with the most total memory reservation, nor the query with the max memory reservation.
        // This also tests the corner case where a node doesn't have a general pool.
        Map<String, Map<String, Long>> queries = ImmutableMap.<String, Map<String, Long>>builder()
                .put("q_1", ImmutableMap.of("n1", 0L, "n2", 8L, "n3", 0L, "n4", 0L, "n5", 0L))
                .put("q_2", ImmutableMap.of("n1", 3L, "n2", 5L, "n3", 2L, "n4", 4L, "n5", 0L))
                .put("q_3", ImmutableMap.of("n1", 0L, "n2", 0L, "n3", 9L, "n4", 0L, "n5", 0L))
                .buildOrThrow();
        assertEquals(
                lowMemoryKiller.chooseQueryToKill(
                        toQueryMemoryInfoList(queries),
                        toNodeMemoryInfoList(generalPool, queries)),
                Optional.of(new QueryId("q_1")));
    }
}
