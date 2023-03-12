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
import static io.trino.memory.LowMemoryKillerTestingUtils.toRunningQueryInfoList;
import static org.testng.Assert.assertEquals;

public class TestTotalReservationLowMemoryKiller
{
    private final LowMemoryKiller lowMemoryKiller = new TotalReservationLowMemoryKiller();

    @Test
    public void testMemoryPoolHasNoReservation()
    {
        int memoryPool = 12;
        Map<String, Map<String, Long>> queries = ImmutableMap.of("q_1", ImmutableMap.of("n1", 0L, "n2", 0L, "n3", 0L, "n4", 0L, "n5", 0L));
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
        // q2 is the query with the most total memory reservation, but not the query with the max memory reservation.
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
                Optional.of(KillTarget.wholeQuery(new QueryId("q_2"))));
    }
}
