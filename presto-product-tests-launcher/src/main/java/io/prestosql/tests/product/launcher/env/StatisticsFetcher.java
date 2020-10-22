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
package io.prestosql.tests.product.launcher.env;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.MemoryStatsConfig;
import com.github.dockerjava.api.model.StatisticNetworksConfig;
import com.github.dockerjava.api.model.Statistics;
import com.github.dockerjava.core.InvocationBuilder;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import net.jodah.failsafe.FailsafeExecutor;
import org.testcontainers.DockerClientFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StatisticsFetcher
{
    private final DockerContainer container;
    private final FailsafeExecutor executor;
    private static final Logger log = Logger.get(StatisticsFetcher.class);
    private AtomicReference<Stats> lastStats = new AtomicReference<>(new Stats());

    public StatisticsFetcher(DockerContainer container, FailsafeExecutor executor)
    {
        this.container = requireNonNull(container, "container is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    public Stats get()
    {
        if (!container.isRunning()) {
            log.warn("Could not get statistics for stopped container %s", container.getLogicalName());
            return lastStats.get();
        }

        try (DockerClient client = DockerClientFactory.lazyClient(); InvocationBuilder.AsyncResultCallback<Statistics> callback = new InvocationBuilder.AsyncResultCallback<>()) {
            client.statsCmd(container.getContainerId()).exec(callback);

            return lastStats.getAndUpdate(previousStats -> toStats((Statistics) executor.get(callback::awaitResult), previousStats));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (RuntimeException e) {
            log.error("Could not fetch container %s statistics: %s", container.getLogicalName(), getStackTraceAsString(e));
            return lastStats.get();
        }
    }

    private Stats toStats(Statistics statistics, Stats previousStats)
    {
        Stats stats = new Stats();

        if (statistics == null || statistics.getCpuStats() == null) {
            return previousStats;
        }

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
        stats.memoryLimit = DataSize.ofBytes(memoryStats.getLimit()).succinct();
        stats.memoryUsage = DataSize.ofBytes(memoryStats.getUsage()).succinct();
        stats.memoryMaxUsage = DataSize.ofBytes(memoryStats.getMaxUsage()).succinct();
        stats.memoryUsagePerc = 100.0 * memoryStats.getUsage() / memoryStats.getLimit();

        stats.pids = statistics.getPidsStats().getCurrent();

        Supplier<Stream<StatisticNetworksConfig>> stream = () -> statistics.getNetworks().values().stream();
        stats.networkReceived = DataSize.ofBytes(stream.get().map(StatisticNetworksConfig::getRxBytes).reduce(0L, Long::sum)).succinct();
        stats.networkSent = DataSize.ofBytes(stream.get().map(StatisticNetworksConfig::getRxBytes).reduce(0L, Long::sum)).succinct();

        return stats;
    }

    public static class Stats
    {
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

        @Override
        public String toString()
        {
            return format("cpu: %s, memory: %s, pids: %d, network i/o: %s",
                    format("%.2f%%", cpuUsagePerc),
                    format("%s / %s (%.2f%%, max %s)", memoryUsage, memoryLimit, memoryUsagePerc, memoryMaxUsage),
                    pids,
                    format("%s / %s", networkReceived, networkSent));
        }
    }
}
