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
package io.trino.spi.simd;

import java.io.BufferedReader;
import java.io.File;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static java.util.Locale.ENGLISH;

public final class SimdUtils
{
    public static final int MINIMUM_SIMD_LENGTH = 256;

    private SimdUtils() {}

    public static boolean procCpuInfoContains(String needleLowercase)
    {
        File cpuInfo = new File("/proc/cpuinfo");
        if (!cpuInfo.exists()) {
            return false;
        }
        try (BufferedReader br = Files.newBufferedReader(cpuInfo.toPath())) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.toLowerCase(ENGLISH).contains(needleLowercase)) {
                    return true;
                }
            }
        }
        catch (Exception ignored) {
            // ignore
        }
        return false;
    }

    public static Optional<String> linuxCpuVendorId()
    {
        File cpuInfo = new File("/proc/cpuinfo");
        if (!cpuInfo.exists()) {
            return Optional.empty();
        }
        try (BufferedReader br = Files.newBufferedReader(cpuInfo.toPath())) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.toLowerCase(ENGLISH);
                if (line.startsWith("vendor_id")) {
                    int idx = line.indexOf(':');
                    if (idx >= 0 && idx + 1 < line.length()) {
                        return Optional.of(line.substring(idx + 1).trim());
                    }
                }
            }
        }
        catch (Exception ignored) {
            // ignore I/O or permission issues
        }
        return Optional.empty();
    }

    // Consolidated from SimdFlags: read CPU flags for the given OS
    public static Set<String> readCpuFlags(OSType osType)
    {
        if (osType.equals(OSType.LINUX)) {
            return readLinuxCpuFlags();
        }
        return Set.of();
    }

    private static Set<String> readLinuxCpuFlags()
    {
        File cpuInfo = new File("/proc/cpuinfo");
        if (!cpuInfo.exists()) {
            return Set.of();
        }
        Set<String> flags = new HashSet<>();
        try (BufferedReader br = Files.newBufferedReader(cpuInfo.toPath())) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.toLowerCase(ENGLISH);
                if (line.startsWith("flags") || line.startsWith("features")) {
                    int idx = line.indexOf(':');
                    if ((idx >= 0) && (idx + 1 < line.length())) {
                        String[] parts = line.substring(idx + 1).trim().split("\\s+");
                        for (String part : parts) {
                            if (!part.isEmpty()) {
                                flags.add(normalizeFlag(part));
                            }
                        }
                    }
                }
            }
        }
        catch (Exception ignored) {
            return Set.of();
        }
        return flags;
    }

    public static String normalizeFlag(String flag)
    {
        return flag.toLowerCase(ENGLISH).replace("_", "");
    }
}
