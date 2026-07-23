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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.VMOption;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Math.ceilDiv;
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
        if ("Linux".equals(osName) && "amd64".equals(osArch)) {
            return calculateAvailablePhysicalProcessorCount(
                    availableProcessorCount,
                    isActiveProcessorCountExplicitlySet(),
                    MachineInfo::readLinuxThreadsPerCore);
        }

        return availableProcessorCount;
    }

    @VisibleForTesting
    static int calculateAvailablePhysicalProcessorCount(
            int availableProcessorCount,
            boolean activeProcessorCountExplicitlySet,
            Supplier<Optional<Integer>> linuxThreadsPerCore)
    {
        if (activeProcessorCountExplicitlySet) {
            return availableProcessorCount;
        }

        return linuxThreadsPerCore.get()
                .map(threadsPerCore -> ceilDiv(availableProcessorCount, threadsPerCore))
                .orElse(availableProcessorCount);
    }

    private static Set<String> readCpuFlagsInternal()
    {
        return switch (StandardSystemProperty.OS_NAME.value()) {
            case "Linux" -> readLinuxCpuFlags();
            case "Mac OS X" -> readMacOsCpuFlags();
            case null, default -> ImmutableSet.of();
        };
    }

    private static Optional<Integer> readLinuxThreadsPerCore()
    {
        return readLines(Path.of("/proc/cpuinfo"))
                .flatMap(MachineInfo::parseLinuxThreadsPerCore);
    }

    private static boolean isActiveProcessorCountExplicitlySet()
    {
        try {
            HotSpotDiagnosticMXBean bean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
            return bean != null && bean.getVMOption("ActiveProcessorCount").getOrigin() == VMOption.Origin.VM_CREATION;
        }
        catch (IllegalArgumentException | UnsupportedOperationException e) {
            return false;
        }
    }

    static Optional<Integer> parseLinuxThreadsPerCore(List<String> cpuInfoLines)
    {
        Integer threadsPerCore = null;
        Integer currentSiblings = null;
        Integer currentCpuCores = null;

        // add a synthetic section separator so the final section is flushed
        for (String line : withTrailingBlankLine(cpuInfoLines)) {
            if (line.isBlank()) {
                if ((currentSiblings == null) != (currentCpuCores == null)) {
                    return Optional.empty();
                }
                if (currentSiblings != null && currentCpuCores != null) {
                    if (currentSiblings < currentCpuCores || currentSiblings % currentCpuCores != 0) {
                        return Optional.empty();
                    }
                    int currentThreadsPerCore = currentSiblings / currentCpuCores;
                    if (threadsPerCore != null && !threadsPerCore.equals(currentThreadsPerCore)) {
                        return Optional.empty();
                    }
                    threadsPerCore = currentThreadsPerCore;
                }
                currentSiblings = null;
                currentCpuCores = null;
                continue;
            }

            List<String> keyAndValue = KEY_VALUE_SPLITTER.splitToList(line);
            if (keyAndValue.size() != 2) {
                continue;
            }

            String key = keyAndValue.getFirst().toLowerCase(ENGLISH);
            String value = keyAndValue.getLast();
            if (key.equals("siblings")) {
                Optional<Integer> parsedValue = parsePositiveInteger(value);
                if (parsedValue.isEmpty()) {
                    return Optional.empty();
                }
                currentSiblings = parsedValue.get();
            }
            else if (key.equals("cpu cores")) {
                Optional<Integer> parsedValue = parsePositiveInteger(value);
                if (parsedValue.isEmpty()) {
                    return Optional.empty();
                }
                currentCpuCores = parsedValue.get();
            }
        }

        return Optional.ofNullable(threadsPerCore);
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

    private static Optional<Integer> parsePositiveInteger(String value)
    {
        try {
            int parsedValue = Integer.parseInt(value);
            if (parsedValue <= 0) {
                return Optional.empty();
            }
            return Optional.of(parsedValue);
        }
        catch (NumberFormatException e) {
            return Optional.empty();
        }
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
