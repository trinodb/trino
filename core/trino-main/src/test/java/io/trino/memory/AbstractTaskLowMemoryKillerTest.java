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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.memory.LowMemoryKillerTestingUtils.toNodeMemoryInfoList;
import static io.trino.memory.LowMemoryKillerTestingUtils.toRunningQueryInfoList;
import static junit.framework.TestCase.assertEquals;

abstract class AbstractTaskLowMemoryKillerTest
{
    private LowMemoryKiller lowMemoryKiller;

    @BeforeClass
    public void setup()
    {
        this.lowMemoryKiller = createLowMemoryKiller();
    }

    protected LowMemoryKiller getLowMemoryKiller()
    {
        return lowMemoryKiller;
    }

    protected abstract LowMemoryKiller createLowMemoryKiller();

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
    public void testWillNotKillTaskForQueryWithoutTaskRetriesEnabled()
    {
        int memoryPool = 5;
        Map<String, Map<String, Long>> queries = ImmutableMap.<String, Map<String, Long>>builder()
                .put("q_1", ImmutableMap.of("n1", 0L, "n2", 0L, "n3", 2L))
                .put("q_2", ImmutableMap.of("n1", 3L, "n2", 5L, "n3", 2L))
                .buildOrThrow();
        Map<String, Map<String, Map<Integer, Long>>> tasks = ImmutableMap.<String, Map<String, Map<Integer, Long>>>builder()
                .put("q_1", ImmutableMap.of(
                        "n3", ImmutableMap.of(1, 5L)))
                .put("q_2", ImmutableMap.of(
                        "n1", ImmutableMap.of(
                                1, 1L,
                                2, 2L),
                        "n2", ImmutableMap.of(
                                3, 3L,
                                4, 1L,
                                5, 1L),
                        "n3", ImmutableMap.of(6, 2L)))
                .buildOrThrow();

        assertEquals(
                lowMemoryKiller.chooseTargetToKill(
                        toRunningQueryInfoList(queries),
                        toNodeMemoryInfoList(memoryPool, queries, tasks)),
                Optional.empty());
    }
}
