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
package io.prestosql.execution.scheduler;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.util.Objects.requireNonNull;

public class FileBasedNetworkTopology
        implements NetworkTopology
{
    private static final Logger log = Logger.get(FileBasedNetworkTopology.class);

    private final Path networkTopologyFile;
    private final Supplier<Map<String, NetworkLocation>> topologySupplier;
    private Map<String, NetworkLocation> validTopology;
    private boolean lastReloadSuccessed;

    @Inject
    public FileBasedNetworkTopology(TopologyAwareNodeSelectorConfig topologyConfig)
    {
        networkTopologyFile = Paths.get(requireNonNull(topologyConfig.getNetworkTopologyFile()));
        Duration refreshPeriod = topologyConfig.getRefreshPeriod();
        try {
            validTopology = loadTopologyFile();
        }
        catch (Exception e) {
            throw new PrestoException(CONFIGURATION_INVALID, e);
        }

        if (refreshPeriod == null) {
            topologySupplier = memoize(() -> validTopology);
        }
        else {
            topologySupplier = memoizeWithExpiration(
                    this::reloadTopology, refreshPeriod.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private Map<String, NetworkLocation> loadTopologyFile()
            throws IOException
    {
        Splitter splitHostAndLocation = Splitter.on(' ').trimResults().omitEmptyStrings();
        Splitter splitSegments = Splitter.on('/').trimResults().omitEmptyStrings();
        try (Stream<String> stream = Files.lines(networkTopologyFile)) {
            ImmutableMap.Builder<String, NetworkLocation> builder = ImmutableMap.builder();
            stream.map(splitHostAndLocation::splitToList)
                    .filter(line -> !line.isEmpty())
                    .forEach(pair ->
                        builder.put(pair.get(0),
                                new NetworkLocation(splitSegments.splitToList(pair.get(1)))));
            return builder.build();
        }
    }

    private Map<String, NetworkLocation> reloadTopology()
    {
        try {
            validTopology = loadTopologyFile();
            lastReloadSuccessed = true;
        }
        catch (Exception e) {
            if (lastReloadSuccessed) {
                log.error(e, "Failed to reload topology file. Possibly the file is mal-formatted.");
            }
            lastReloadSuccessed = false;
        }
        return validTopology;
    }

    @Override
    public NetworkLocation locate(HostAddress address)
    {
        return topologySupplier.get().getOrDefault(address.getHostText(), NetworkLocation.ROOT_LOCATION);
    }
}
