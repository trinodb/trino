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
package io.trino.tests.product.suite;

import com.google.common.base.Stopwatch;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Math.max;
import static java.lang.Math.round;

final class SystemMemoryMonitor
{
    private static final Logger log = Logger.get(SystemMemoryMonitor.class);

    private static final Path MEMINFO = Path.of("/proc/meminfo");
    private static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(10);
    private static final Duration SNAPSHOT_INTERVAL = Duration.ofSeconds(60);
    private static final double WARN_USED_THRESHOLD = 0.85;
    private static final String USAGE_FORMAT = "RAM %d%% used, %d MB available of %d MB";

    private static final AtomicBoolean STARTED = new AtomicBoolean();

    private SystemMemoryMonitor() {}

    // idempotent
    static void start()
    {
        if (!Files.isReadable(MEMINFO)) {
            return;
        }
        if (!STARTED.compareAndSet(false, true)) {
            return;
        }
        Thread.ofVirtual().name("system-memory-monitor").start(SystemMemoryMonitor::monitor);
    }

    private static void monitor()
    {
        Stopwatch sinceSnapshot = Stopwatch.createStarted();
        while (!Thread.currentThread().isInterrupted()) {
            Meminfo meminfo = readMeminfo();
            double usedFraction = (double) (meminfo.totalMb() - meminfo.availableMb()) / meminfo.totalMb();
            int usedPercent = max(0, (int) round(usedFraction * 100));
            if (usedFraction >= WARN_USED_THRESHOLD) {
                log.warn(USAGE_FORMAT, usedPercent, meminfo.availableMb(), meminfo.totalMb());
            }
            else if (sinceSnapshot.elapsed().compareTo(SNAPSHOT_INTERVAL) >= 0) {
                log.info(USAGE_FORMAT, usedPercent, meminfo.availableMb(), meminfo.totalMb());
                sinceSnapshot.reset().start();
            }
            try {
                Thread.sleep(SAMPLE_INTERVAL);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static Meminfo readMeminfo()
    {
        long totalKb = 0;
        long availableKb = 0;
        try {
            // MemAvailable, not com.sun.management.OperatingSystemMXBean#getFreeMemorySize(): the JDK
            // figure reports free memory only, which excludes reclaimable page cache and so understates
            // what is actually available.
            for (String line : Files.readAllLines(MEMINFO)) {
                if (line.startsWith("MemTotal:")) {
                    totalKb = valueKb(line);
                }
                else if (line.startsWith("MemAvailable:")) {
                    availableKb = valueKb(line);
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read " + MEMINFO, e);
        }
        if (totalKb <= 0 || availableKb <= 0) {
            throw new IllegalStateException("Did not find MemTotal and MemAvailable in " + MEMINFO);
        }
        return new Meminfo(totalKb / 1024, availableKb / 1024);
    }

    private static long valueKb(String line)
    {
        // e.g. "MemTotal:       16391688 kB"
        return Long.parseLong(line.split("\\s+")[1]);
    }

    private record Meminfo(long totalMb, long availableMb) {}
}
