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
package io.trino.plugin.varada.configuration;

import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.LOCAL_DATA_STORAGE_PREFIX;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX;
import static java.lang.String.format;

public class ProxiedConnectorConfiguration
{
    public static final String PROXIED_CONNECTOR = "proxied-connector";
    public static final String PASS_THROUGH_DISPATCHER = "enable.passthrough";

    public static final String DELTA_LAKE_CONNECTOR_NAME = "delta-lake";
    public static final String HIVE_CONNECTOR_NAME = "hive";
    public static final String HUDI_CONNECTOR_NAME = "hudi";
    public static final String ICEBERG_CONNECTOR_NAME = "iceberg";

    private final Set<String> supportedConnectors = Set.of(DELTA_LAKE_CONNECTOR_NAME, HIVE_CONNECTOR_NAME, HUDI_CONNECTOR_NAME, ICEBERG_CONNECTOR_NAME);

    private String proxiedConnector;
    private Set<String> passThroughDispatcherSet;

    public String getProxiedConnector()
    {
        return proxiedConnector;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + PROXIED_CONNECTOR)
    @Config(WARP_SPEED_PREFIX + PROXIED_CONNECTOR)
    public void setProxiedConnector(String proxiedConnector)
    {
        this.proxiedConnector = proxiedConnector;
    }

    public Set<String> getPassThroughDispatcherSet()
    {
        return (passThroughDispatcherSet != null) ? passThroughDispatcherSet : Set.of();
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + PASS_THROUGH_DISPATCHER)
    @Config(WARP_SPEED_PREFIX + PASS_THROUGH_DISPATCHER)
    public void setPassThroughDispatcherSet(String passThroughDispatcherListStr)
    {
        this.passThroughDispatcherSet = Arrays.stream(passThroughDispatcherListStr.split(",", -1))
                .map(String::trim)
                .collect(Collectors.toSet());
        checkArgument(supportedConnectors.containsAll(passThroughDispatcherSet),
                format("%s configuration only supports %s", PASS_THROUGH_DISPATCHER, supportedConnectors));
    }

    @Override
    public String toString()
    {
        return "ProxiedConnectorConfiguration{" +
                "proxiedConnector='" + proxiedConnector + '\'' +
                ", passThroughDispatcherSet=" + passThroughDispatcherSet +
                '}';
    }
}
