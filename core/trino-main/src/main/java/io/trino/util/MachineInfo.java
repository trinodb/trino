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

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Math.min;
import static java.util.Locale.ENGLISH;
import static java.util.function.Predicate.not;

public final class MachineInfo
{
    private static final Splitter FLAG_SPLITTER = Splitter.on(CharMatcher.whitespace())
            .omitEmptyStrings();
    private static final Splitter KEY_VALUE_SPLITTER = Splitter.on(':')
            .limit(2)
            .trimResults();

    // cache physical processor count, so that it's not queried multiple times during tests
    private static final Supplier<Integer> PHYSICAL_PROCESSOR_COUNT = memoize(MachineInfo::readAvailablePhysicalProcessorCount);
    private static final Supplier<Set<String>> CPU_FLAGS = memoize(MachineInfo::readCpuFlagsInternal);

    private MachineInfo() {}

    public static int getAvailablePhysicalProcessorCount()
    {
        return PHYSICAL_PROCESSOR_COUNT.get();
    }

    public static Set<String> readCpuFlags()
    {
        return CPU_FLAGS.get();
    }

    private static int readAvailablePhysicalProcessorCount()
    {
        String osArch = StandardSystemProperty.OS_ARCH.value();
        String osName = StandardSystemProperty.OS_NAME.value();
        // logical core count (including container cpu quota if there is any)
        int availableProcessorCount = Runtime.getRuntime().availableProcessors();
        int totalPhysicalProcessorCount;
        if ("Linux".equals(osName) && "amd64".equals(osArch)) {
            totalPhysicalProcessorCount = readLinuxPhysicalProcessorCount()
                    .orElse(availableProcessorCount);
        }
        else {
            // Fallback to logical processor count when physical core topology is not available.
            totalPhysicalProcessorCount = availableProcessorCount;
        }

        // cap available processor count to container cpu quota (if there is any).
        return min(totalPhysicalProcessorCount, availableProcessorCount);
    }

    private static Set<String> readCpuFlagsInternal()
    {
        return switch (StandardSystemProperty.OS_NAME.value()) {
            case "Linux" -> readLinuxCpuFlags();
            case "Mac OS X" -> readMacOsCpuFlags();
            case null, default -> ImmutableSet.of();
        };
    }

    private static Optional<Integer> readLinuxPhysicalProcessorCount()
    {
        return readLines(Path.of("/proc/cpuinfo"))
                .flatMap(MachineInfo::parseLinuxPhysicalProcessorCount);
    }

    static Optional<Integer> parseLinuxPhysicalProcessorCount(List<String> cpuInfoLines)
    {
        Set<String> physicalCores = new HashSet<>();
        String currentPhysicalId = null;
        String currentCoreId = null;

        // add a synthetic section separator so the final section is flushed
        for (String line : withTrailingBlankLine(cpuInfoLines)) {
            if (line.isBlank()) {
                if (currentPhysicalId != null && currentCoreId != null) {
                    physicalCores.add(currentPhysicalId + ":" + currentCoreId);
                }
                currentPhysicalId = null;
                currentCoreId = null;
                continue;
            }

            List<String> keyAndValue = KEY_VALUE_SPLITTER.splitToList(line);
            if (keyAndValue.size() != 2) {
                continue;
            }

            String key = keyAndValue.getFirst().toLowerCase(ENGLISH);
            String value = keyAndValue.getLast();
            if (key.equals("physical id")) {
                currentPhysicalId = value;
            }
            else if (key.equals("core id")) {
                currentCoreId = value;
            }
        }

        if (physicalCores.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(physicalCores.size());
    }

    private static Set<String> readLinuxCpuFlags()
    {
        return readLines(Path.of("/proc/cpuinfo"))
                .map(MachineInfo::parseLinuxCpuFlags)
                .orElse(ImmutableSet.of());
    }

    static Set<String> parseLinuxCpuFlags(List<String> cpuInfoLines)
    {
        Multiset<String> flagCounts = HashMultiset.create();
        int sectionCount = 0;
        Set<String> sectionFlags = new HashSet<>();

        // add a synthetic section separator so the final section is flushed
        for (String line : withTrailingBlankLine(cpuInfoLines)) {
            if (line.isBlank()) {
                if (!sectionFlags.isEmpty()) {
                    sectionCount++;
                    flagCounts.addAll(sectionFlags);
                }
                sectionFlags.clear();
                continue;
            }

            List<String> pair = KEY_VALUE_SPLITTER.splitToList(line);
            if (pair.size() != 2) {
                continue;
            }

            String key = pair.getFirst().toLowerCase(ENGLISH).trim();
            if (!key.equals("flags") && !key.equals("features")) {
                continue;
            }
            sectionFlags.addAll(parseCpuFlags(pair.getLast()));
        }

        int requiredCount = sectionCount;
        return flagCounts.elementSet().stream()
                .filter(flag -> flagCounts.count(flag) == requiredCount)
                .collect(toImmutableSet());
    }

    private static Set<String> readMacOsCpuFlags()
    {
        return switch (StandardSystemProperty.OS_ARCH.value()) {
            case "aarch64" -> ImmutableSet.of("neon");
            case null, default -> ImmutableSet.of();
        };
    }

    private static Optional<List<String>> readLines(Path path)
    {
        try {
            return Optional.of(Files.readAllLines(path));
        }
        catch (IOException e) {
            return Optional.empty();
        }
    }

    private static Iterable<String> withTrailingBlankLine(Iterable<String> lines)
    {
        return Iterables.concat(lines, List.of(""));
    }

    private static Set<String> parseCpuFlags(String value)
    {
        if (value.isBlank()) {
            return ImmutableSet.of();
        }

        return FLAG_SPLITTER.splitToStream(value.trim())
                .map(MachineInfo::normalizeCpuFlag)
                .filter(not(String::isEmpty))
                .collect(toImmutableSet());
    }

    public static String normalizeCpuFlag(String flag)
    {
        flag = flag.toLowerCase(ENGLISH).replace("_", "").trim();

        return switch (flag) {
            case "asimd", "advsimd" -> "neon";
            default -> flag;
        };
    }
}
