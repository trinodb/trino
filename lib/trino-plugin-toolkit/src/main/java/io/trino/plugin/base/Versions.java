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

import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public final class Versions
{
    private Versions() {}

    /**
     * Check if the SPI version of the Trino server matches exactly the SPI version the connector plugin was built for.
     * Using plugins built for a different version of Trino may fail at runtime, especially if plugin author
     * chooses not to maintain compatibility with older SPI versions, as happens for plugins maintained together with
     * the Trino project.
     */
    public static void checkSpiVersion(ConnectorContext context, ConnectorFactory connectorFactory)
    {
        String spiVersion = context.getSpiVersion();
        String compileTimeSpiVersion = SpiVersionHolder.SPI_COMPILE_TIME_VERSION;

        checkState(
                spiVersion.equals(compileTimeSpiVersion),
                format("Trino SPI version %s does not match the version %s connector %s was compiled for", spiVersion, compileTimeSpiVersion, connectorFactory.getName()));
    }
}
