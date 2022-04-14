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
package io.trino.plugin.base;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Versions
{
    private Versions() {}

    /**
     * Check if the SPI version of the Trino server matches exactly the SPI version the connector plugin was built for.
     * Using plugins built for a different version of Trino may fail at runtime, especially if plugin author
     * chooses not to maintain compatibility with older SPI versions, as happens for plugins maintained together with
     * the Trino project.
     */
    public static void checkSpiVersion(ConnectorContext context, String connectorName, Class<? extends Connector> connectorClass)
    {
        String spiVersion = context.getSpiVersion();
        Optional<String> pluginVersion = getPluginMavenVersion(connectorClass);

        if (pluginVersion.isEmpty()) {
            // Assume we're in tests. In tests, plugin version is unknown and SPI version may be known, e.g. when running single module's tests from maven.
            return;
        }

        checkState(
                spiVersion.equals(pluginVersion.get()),
                format(
                        "Trino SPI version %s does not match %s connector version %s. The connector cannot be used with SPI version other than it was compiled for.",
                        spiVersion,
                        connectorName,
                        pluginVersion.get()));
    }

    private static Optional<String> getPluginMavenVersion(Class<? extends Connector> connectorClass)
    {
        String specificationVersion = connectorClass.getPackage().getSpecificationVersion();
        String implementationVersion = connectorClass.getPackage().getImplementationVersion();
        if (specificationVersion == null && implementationVersion == null) {
            // The information comes from jar's Manifest and is not present e.g. when running tests, or from an IDE.
            return Optional.empty();
        }

        requireNonNull(specificationVersion, "specificationVersion not present when implementationVersion is present");
        requireNonNull(implementationVersion, "implementationVersion not present when specificationVersion is present");

        // Implementation version comes from Manifest and is defined in Airbase as equivalent to `git describe` output.
        if (implementationVersion.matches(".*-(\\d+)-g([0-9a-f]+)(-dirty)?$")) {
            // Specification version comes from Manifest and is the project version with `-SNAPSHOT` stripped, and .0 added (if not present).
            return Optional.of(specificationVersion.replaceAll("\\.0$", "") + "-SNAPSHOT");
        }
        return Optional.of(implementationVersion);
    }
}
