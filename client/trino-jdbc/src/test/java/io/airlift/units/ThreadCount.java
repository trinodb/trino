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
package io.airlift.units;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.OptionalInt;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.airlift.units.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.lines;

// This class is a copy from airlift's units due to an inability to use
// newer units version in JDBC due to different JDK target.
// It is temporary solution until client and JDBC are moved to JDK 11+.
// This class is added to test classes, so it won't be a part of the jdbc driver.
public class ThreadCount
        implements Comparable<ThreadCount>
{
    private static final String PER_CORE_SUFFIX = "C";
    private static final Supplier<Integer> AVAILABLE_PROCESSORS = MachineInfo::getAvailablePhysicalProcessorCount;
    private final int threadCount;

    ThreadCount(int threadCount)
    {
        checkArgument(threadCount >= 0, "Thread count cannot be negative");
        this.threadCount = threadCount;
    }

    public int getThreadCount()
    {
        return threadCount;
    }

    public static ThreadCount exactValueOf(int value)
    {
        return new ThreadCount(value);
    }

    public static ThreadCount valueOf(String value)
    {
        if (value.endsWith(PER_CORE_SUFFIX)) {
            float parsedMultiplier = parseFloat(value.substring(0, value.lastIndexOf(PER_CORE_SUFFIX)).trim());
            checkArgument(parsedMultiplier > 0, "Thread multiplier cannot be negative");
            float threadCount = parsedMultiplier * AVAILABLE_PROCESSORS.get();
            checkArgument(threadCount <= Integer.MAX_VALUE, "Thread count is greater than 2^32 - 1");
            return new ThreadCount(round(threadCount));
        }

        return new ThreadCount(parseInteger(value));
    }

    public static ThreadCount boundedValueOf(String value, String minValue, String maxValue)
    {
        ThreadCount parsed = ThreadCount.valueOf(value);
        ThreadCount min = ThreadCount.valueOf(minValue);
        ThreadCount max = ThreadCount.valueOf(maxValue);

        if (parsed.compareTo(min) < 0) {
            return min;
        }

        if (parsed.compareTo(max) > 0) {
            return max;
        }
        return parsed;
    }

    private static float parseFloat(String value)
    {
        try {
            return Float.parseFloat(value);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(format("Cannot parse value '%s' as float", value), e);
        }
    }

    private static int parseInteger(String value)
    {
        try {
            long parsed = Long.parseLong(value);
            checkArgument(parsed <= Integer.MAX_VALUE, "Thread count is greater than 2^32 - 1");
            return toIntExact(parsed);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(format("Cannot parse value '%s' as integer", value), e);
        }
    }

    @Override
    public int compareTo(ThreadCount o)
    {
        return Integer.compare(threadCount, o.threadCount);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ThreadCount that = (ThreadCount) o;
        return threadCount == that.threadCount;
    }

    @Override
    public int hashCode()
    {
        return threadCount;
    }

    @Override
    public String toString()
    {
        return (threadCount == 1) ? "1 thread" : (threadCount + " threads");
    }

    static final class MachineInfo
    {
        private static final Path CPU_INFO_PATH = Paths.get("/proc/cpuinfo");

        // cache physical processor count, so that it's not queried multiple times during tests
        private static volatile int physicalProcessorCount = -1;

        private MachineInfo() {}

        public static int getAvailablePhysicalProcessorCount()
        {
            if (physicalProcessorCount != -1) {
                return physicalProcessorCount;
            }

            String osArch = System.getProperty("os.arch");
            // logical core count (including container cpu quota if there is any)
            int availableProcessorCount = Runtime.getRuntime().availableProcessors();
            int totalPhysicalProcessorCount = availableProcessorCount;
            if ("amd64".equals(osArch) || "x86_64".equals(osArch)) {
                OptionalInt procInfo = tryReadFromProcCpuinfo();
                if (procInfo.isPresent()) {
                    totalPhysicalProcessorCount = procInfo.getAsInt();
                }
            }

            // cap available processor count to container cpu quota (if there is any).
            physicalProcessorCount = min(totalPhysicalProcessorCount, availableProcessorCount);
            return physicalProcessorCount;
        }

        private static OptionalInt tryReadFromProcCpuinfo()
        {
            if (!exists(CPU_INFO_PATH)) {
                return OptionalInt.empty();
            }

            try (Stream<String> lines = lines(CPU_INFO_PATH)) {
                return OptionalInt.of(toIntExact(lines.filter(line ->
                        line.matches("^processor\\s+: \\d")).count()));
            }
            catch (IOException e) {
                return OptionalInt.empty();
            }
        }
    }
}
