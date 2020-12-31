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
package io.trino.tests.product.launcher.env;

import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.MemoryStatsConfig;
import com.github.dockerjava.api.model.StatisticNetworksConfig;
import com.github.dockerjava.api.model.Statistics;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import org.testcontainers.DockerClientFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.tests.product.launcher.env.StatisticsFetcher.Stats.statisticsAreEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StatisticsFetcher
        implements AutoCloseable
{
    private final String containerId;
    private static final Logger log = Logger.get(StatisticsFetcher.class);
    private final String containerLogicalName;
    private final AtomicReference<Stats> lastStats = new AtomicReference<>(new Stats());
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final StatisticsCallback callback;

    StatisticsFetcher(String containerId, String containerLogicalName)
    {
        this.containerId = requireNonNull(containerId, "containerId is null");
        this.containerLogicalName = requireNonNull(containerLogicalName, "containerLogicalName is null");
        this.callback = new StatisticsCallback();
    }

    public Stats get()
    {
        return lastStats.get();
    }

    public void start()
    {
        if (started.compareAndSet(false, true)) {
            DockerClientFactory.lazyClient()
                    .statsCmd(containerId)
                    .exec(callback);

            log.info("Started listening for container %s statistics stream...", containerLogicalName);
        }
    }

    @Override
    public void close()
    {
        try {
            callback.close();
        }
        catch (IOException e) {
            log.warn(e, "Caught exception while closing fetcher for container %s", containerLogicalName);
        }
    }

    public static StatisticsFetcher create(DockerContainer container)
    {
        return new StatisticsFetcher(container.getContainerId(), container.getLogicalName());
    }

    private Stats toStats(Statistics statistics, Stats previousStats)
    {
        if (statisticsAreEmpty(statistics)) {
            return previousStats;
        }

        Stats stats = new Stats();
        stats.systemCpuUsage = statistics.getCpuStats().getSystemCpuUsage();
        stats.totalCpuUsage = statistics.getCpuStats().getCpuUsage().getTotalUsage();
        stats.cpuUsagePerc = 0.0;

        if (previousStats.systemCpuUsage != -1 && previousStats.totalCpuUsage != -1) {
            double usageCpuDelta = stats.totalCpuUsage - previousStats.totalCpuUsage;
            double systemCpuDelta = stats.systemCpuUsage - previousStats.systemCpuUsage;

            if (usageCpuDelta > 0.0 && systemCpuDelta > 0.0) {
                stats.cpuUsagePerc = usageCpuDelta / systemCpuDelta * statistics.getCpuStats().getCpuUsage().getPercpuUsage().size() * 100;
            }
        }

        MemoryStatsConfig memoryStats = statistics.getMemoryStats();
        stats.memoryLimit = DataSize.ofBytes(memoryStats.getLimit()).to(GIGABYTE);
        stats.memoryUsage = DataSize.ofBytes(memoryStats.getUsage()).to(GIGABYTE);
        stats.memoryMaxUsage = DataSize.ofBytes(memoryStats.getMaxUsage()).to(GIGABYTE);
        stats.memoryUsagePerc = 100.0 * memoryStats.getUsage() / memoryStats.getLimit();

        stats.pids = statistics.getPidsStats().getCurrent();

        Supplier<Stream<StatisticNetworksConfig>> stream = () -> statistics.getNetworks().values().stream();
        stats.networkReceived = DataSize.ofBytes(stream.get().map(StatisticNetworksConfig::getRxBytes).reduce(0L, Long::sum)).succinct();
        stats.networkSent = DataSize.ofBytes(stream.get().map(StatisticNetworksConfig::getTxBytes).reduce(0L, Long::sum)).succinct();

        return stats;
    }

    private class StatisticsCallback
            implements ResultCallback<Statistics>
    {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private Closeable stream;

        @Override
        public void onNext(Statistics statistics)
        {
            lastStats.getAndUpdate(previousStats -> toStats(statistics, previousStats));
        }

        @Override
        public void onStart(Closeable stream)
        {
            this.stream = requireNonNull(stream, "stream is null");
        }

        @Override
        public void onError(Throwable throwable)
        {
            if (!closed.get()) {
                log.warn(throwable, "Caught exception while processing statistics");

                try {
                    close();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void onComplete()
        {
            try {
                close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close()
                throws IOException
        {
            if (closed.compareAndSet(false, true)) {
                if (stream != null) {
                    stream.close();
                }

                log.info("Stopped listening for container %s stats", containerLogicalName);
            }
        }
    }

    public static class Stats
    {
        public static final String[] HEADER = {
                "container",
                "cpu",
                "mem",
                "max mem",
                "mem %",
                "peak mem",
                "pids",
                "net in",
                "net out"
        };

        private long systemCpuUsage = -1;
        private long totalCpuUsage = -1;
        private double cpuUsagePerc;
        private double memoryUsagePerc;
        private DataSize memoryUsage;
        private DataSize memoryLimit;
        private DataSize memoryMaxUsage;
        private long pids;
        public DataSize networkReceived;
        public DataSize networkSent;

        public boolean areCalculated()
        {
            return cpuUsagePerc > 0.0;
        }

        public String[] toRow(String name)
        {
            if (!areCalculated()) {
                return new String[] {name, "n/a"};
            }

            return new String[] {
                    name,
                    format("%.2f%%", cpuUsagePerc),
                    memoryLimit.toString(),
                    memoryUsage.toString(),
                    format("%.2f%%", memoryUsagePerc),
                    memoryMaxUsage.toString(),
                    format("%d", pids),
                    format("%s", networkReceived),
                    format("%s", networkSent)
            };
        }

        public static boolean statisticsAreEmpty(Statistics statistics)
        {
            return statistics == null || statistics.getRead().equals("0001-01-01T00:00:00Z");
        }
    }
}
