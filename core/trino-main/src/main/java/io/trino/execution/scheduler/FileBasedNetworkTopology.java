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
package io.trino.execution.scheduler;

import com.google.common.base.Splitter;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.HostAddress;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.CharMatcher.whitespace;
import static io.trino.execution.scheduler.NetworkLocation.ROOT_LOCATION;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class FileBasedNetworkTopology
        implements NetworkTopology
{
    private static final Logger log = Logger.get(FileBasedNetworkTopology.class);
    private static final Splitter SPLIT_HOST_AND_LOCATION = Splitter.on(whitespace()).omitEmptyStrings();
    private static final Splitter SPLIT_SEGMENTS = Splitter.on('/');

    private final File networkTopologyFile;
    private final long refreshPeriodNanos;
    private final Ticker ticker;
    private long lastUpdate;
    @GuardedBy("this")
    private Map<String, NetworkLocation> topology;

    @Inject
    public FileBasedNetworkTopology(TopologyFileConfig topologyConfig)
    {
        this(
                requireNonNull(topologyConfig, "topologyConfig is null").getNetworkTopologyFile(),
                topologyConfig.getRefreshPeriod(),
                Ticker.systemTicker());
    }

    FileBasedNetworkTopology(File networkTopologyFile, Duration refreshPeriod, Ticker ticker)
    {
        this.networkTopologyFile = requireNonNull(networkTopologyFile, "networkTopologyFile is null");
        this.refreshPeriodNanos = requireNonNull(refreshPeriod, "refreshPeriodNanos is null").roundTo(NANOSECONDS);
        this.ticker = requireNonNull(ticker, "ticker is null");
        refreshTopology();
    }

    @Managed
    public synchronized void refreshTopology()
    {
        lastUpdate = ticker.read();
        topology = loadTopologyFile(networkTopologyFile);
    }

    private synchronized Map<String, NetworkLocation> getTopology()
    {
        if (ticker.read() - lastUpdate >= refreshPeriodNanos) {
            try {
                refreshTopology();
            }
            catch (RuntimeException e) {
                log.error(e);
            }
        }
        return topology;
    }

    @Override
    public NetworkLocation locate(HostAddress address)
    {
        return getTopology().getOrDefault(address.getHostText(), ROOT_LOCATION);
    }

    private static Map<String, NetworkLocation> loadTopologyFile(File topologyFile)
    {
        ImmutableMap.Builder<String, NetworkLocation> topology = ImmutableMap.builder();
        List<String> lines;
        try {
            lines = Files.asCharSource(topologyFile, UTF_8).readLines();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Could not read topology file", e);
        }
        for (int lineNumber = 1; lineNumber <= lines.size(); lineNumber++) {
            String line = lines.get(lineNumber - 1).trim();
            if (line.isEmpty()) {
                continue;
            }
            List<String> parts = SPLIT_HOST_AND_LOCATION.splitToList(line);
            if (parts.size() != 2) {
                throw invalidFile(lineNumber, "expected two parts for host and location");
            }

            String location = parts.get(1);
            if (!location.startsWith("/")) {
                throw invalidFile(lineNumber, "location must start with a leading slash");
            }
            List<String> segments = SPLIT_SEGMENTS.splitToList(location.substring(1));
            if (segments.isEmpty()) {
                throw invalidFile(lineNumber, "location must contain at least one segment");
            }
            if (segments.stream().anyMatch(String::isEmpty)) {
                throw invalidFile(lineNumber, "location must not contain an empty segment");
            }
            // NOTE: segment cannot contain whitespace because the host and location splitter would
            // have created multiple parts in that case

            topology.put(parts.get(0), new NetworkLocation(segments));
        }
        return topology.buildOrThrow();
    }

    private static RuntimeException invalidFile(int lineNumber, String message)
    {
        return new RuntimeException(format("Error in network topology file line %s: %s", lineNumber, message));
    }
}
