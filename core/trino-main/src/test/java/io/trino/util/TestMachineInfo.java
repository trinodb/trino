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
package io.trino.util;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.util.MachineInfo.calculateAvailablePhysicalProcessorCount;
import static io.trino.util.MachineInfo.parseLinuxCpuFlags;
import static io.trino.util.MachineInfo.parseLinuxThreadsPerCore;
import static org.assertj.core.api.Assertions.assertThat;

final class TestMachineInfo
{
    @Test
    void testCalculateAvailablePhysicalProcessorCount()
    {
        assertThat(calculateAvailablePhysicalProcessorCount(16, false, () -> Optional.of(2))).isEqualTo(8);
        assertThat(calculateAvailablePhysicalProcessorCount(3, false, () -> Optional.of(2))).isEqualTo(2);
        assertThat(calculateAvailablePhysicalProcessorCount(16, false, () -> Optional.of(1))).isEqualTo(16);
        assertThat(calculateAvailablePhysicalProcessorCount(16, false, Optional::empty)).isEqualTo(16);
    }

    @Test
    void testCalculateAvailablePhysicalProcessorCountWithActiveProcessorCount()
    {
        AtomicBoolean threadsPerCoreRead = new AtomicBoolean();

        assertThat(calculateAvailablePhysicalProcessorCount(16, true, () -> {
            threadsPerCoreRead.set(true);
            return Optional.of(2);
        })).isEqualTo(16);
        assertThat(threadsPerCoreRead).isFalse();
    }

    @Test
    void testParseLinuxThreadsPerCore()
    {
        List<String> cpuInfoLines =
                """
                processor : 0
                siblings : 4
                cpu cores : 2

                processor : 1
                siblings : 4
                cpu cores : 2

                processor : 2
                siblings : 4
                cpu cores : 2

                processor : 3
                siblings : 4
                cpu cores : 2
                """.lines().toList();

        assertThat(parseLinuxThreadsPerCore(cpuInfoLines)).isEqualTo(Optional.of(2));
    }

    @Test
    void testParseLinuxThreadsPerCoreWithoutTopology()
    {
        List<String> cpuInfoLines =
                """
                processor : 0
                flags : avx512f avx512_vbmi2

                processor : 1
                flags : avx512f avx512_vbmi2
                """.lines().toList();

        assertThat(parseLinuxThreadsPerCore(cpuInfoLines)).isEqualTo(Optional.empty());
    }

    @Test
    void testParseLinuxThreadsPerCoreWithInvalidTopology()
    {
        assertThat(parseLinuxThreadsPerCore(
                """
                processor : 0
                siblings : 3
                cpu cores : 2
                """.lines().toList())).isEqualTo(Optional.empty());

        assertThat(parseLinuxThreadsPerCore(
                """
                processor : 0
                siblings : 4
                """.lines().toList())).isEqualTo(Optional.empty());
    }

    @Test
    void testParseLinuxCpuFlagsIntersection()
    {
        List<String> cpuInfoLines =
                """
                processor : 0
                flags : avx512f avx512_vbmi2 asimd

                processor : 1
                flags : avx512f asimd
                """.lines().toList();

        assertThat(parseLinuxCpuFlags(cpuInfoLines))
                .isEqualTo(Set.of("avx512f", "neon"));
    }

    @Test
    void testParseLinuxCpuFlagsSingleProcessor()
    {
        List<String> cpuInfoLines =
                """
                processor : 0
                flags : avx512f asimd
                """.lines().toList();

        assertThat(parseLinuxCpuFlags(cpuInfoLines))
                .isEqualTo(Set.of("avx512f", "neon"));
    }
}
