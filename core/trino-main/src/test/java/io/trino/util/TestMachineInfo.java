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

import static org.assertj.core.api.Assertions.assertThat;

final class TestMachineInfo
{
    @Test
    void testParseLinuxPhysicalProcessorCount()
    {
        List<String> cpuInfoLines =
                """
                processor : 0
                physical id : 0
                core id : 0

                processor : 1
                physical id : 0
                core id : 0

                processor : 2
                physical id : 0
                core id : 1

                processor : 3
                physical id : 0
                core id : 1
                """.lines().toList();

        assertThat(MachineInfo.parseLinuxPhysicalProcessorCount(cpuInfoLines))
                .isEqualTo(Optional.of(2));
    }

    @Test
    void testParseLinuxPhysicalProcessorCountWithoutCoreIds()
    {
        List<String> cpuInfoLines =
                """
                processor : 0
                flags : avx512f avx512_vbmi2

                processor : 1
                flags : avx512f avx512_vbmi2
                """.lines().toList();

        assertThat(MachineInfo.parseLinuxPhysicalProcessorCount(cpuInfoLines))
                .isEqualTo(Optional.empty());
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

        assertThat(MachineInfo.parseLinuxCpuFlags(cpuInfoLines))
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

        assertThat(MachineInfo.parseLinuxCpuFlags(cpuInfoLines))
                .isEqualTo(Set.of("avx512f", "neon"));
    }
}
