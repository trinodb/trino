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

import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableSet;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.CentralProcessor.ProcessorIdentifier;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Math.min;
import static java.util.Locale.ENGLISH;

public final class MachineInfo
{
    // cache physical processor count, so that it's not queried multiple times during tests
    private static volatile int physicalProcessorCount = -1;
    private static final SystemInfo SYSTEM_INFO = new SystemInfo();

    private MachineInfo() {}

    public static int getAvailablePhysicalProcessorCount()
    {
        if (physicalProcessorCount != -1) {
            return physicalProcessorCount;
        }

        String osArch = StandardSystemProperty.OS_ARCH.value();
        // logical core count (including container cpu quota if there is any)
        int availableProcessorCount = Runtime.getRuntime().availableProcessors();
        int totalPhysicalProcessorCount;
        if ("amd64".equals(osArch) || "x86_64".equals(osArch)) {
            // Oshi can recognize physical processor count (without hyper threading) for x86 platforms.
            // However, it doesn't correctly recognize physical processor count for ARM platforms.
            totalPhysicalProcessorCount = SYSTEM_INFO
                    .getHardware()
                    .getProcessor()
                    .getPhysicalProcessorCount();
        }
        else {
            // ARM platforms do not support hyper threading, therefore each logical processor is separate core
            totalPhysicalProcessorCount = availableProcessorCount;
        }

        // cap available processor count to container cpu quota (if there is any).
        physicalProcessorCount = min(totalPhysicalProcessorCount, availableProcessorCount);
        return physicalProcessorCount;
    }

    public static ProcessorIdentifier getProcessorInfo()
    {
        return SYSTEM_INFO.getHardware().getProcessor().getProcessorIdentifier();
    }

    public static Set<String> readCpuFlags()
    {
        CentralProcessor cpu = SYSTEM_INFO.getHardware().getProcessor();
        List<String> flags = cpu.getFeatureFlags();
        if (flags == null || flags.isEmpty()) {
            return ImmutableSet.of();
        }
        // Each element of flags represents the hardware support for an individual core, so we're want to calculate flags
        // advertised by all cores
        Set<String> intersection = null;

        for (String line : flags) {
            if (line == null || line.isBlank()) {
                continue;
            }

            // Strip the "flags:" / "Features:" prefix if present.
            String body = line;
            int colon = line.indexOf(':');
            if (colon >= 0) {
                body = line.substring(colon + 1);
            }

            // Tokenize + normalize.
            Set<String> tokens = Arrays.stream(body.trim().split("\\s+"))
                    .map(token -> normalizeFlag(token))
                    .filter(token -> !token.isEmpty())
                    .collect(toImmutableSet());

            if (tokens.isEmpty()) {
                continue;
            }

            if (intersection == null) {
                intersection = new HashSet<>(tokens);
            }
            else {
                intersection.retainAll(tokens);
                if (intersection.isEmpty()) {
                    break; // nothing in common
                }
            }
        }

        return intersection == null ? ImmutableSet.of() : intersection;
    }

    public static String normalizeFlag(String flag)
    {
        flag = flag.toLowerCase(ENGLISH).replace("_", "").trim();

        // Skip stray keys that may sneak in if the colon wasn’t found.
        if (flag.equals("flags") || flag.equals("features")) {
            return "";
        }

        return flag;
    }
}
