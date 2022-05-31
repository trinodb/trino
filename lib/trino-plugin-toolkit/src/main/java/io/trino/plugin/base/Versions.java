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

import static java.lang.String.format;

public final class Versions
{
    private Versions() {}

    /**
     * Ensure that the SPI version of the Trino server exactly matches the SPI
     * version the connector plugin was built for.
     *
     * <p>Using plugins built for different Trino versions may fail at runtime
     * unless care is taken to only use the compatible parts of the SPI.
     *
     * <p>To simplify development with third-party plugins, this check allows
     * snapshots to be treated as both the preceding and following versions (as
     * long as the version is formatted as X-SNAPSHOT, where X is a number).
     */
    public static void checkSpiVersion(ConnectorContext context, ConnectorFactory connectorFactory)
    {
        String runtimeVersion = context.getSpiVersion();
        String compiledVersion = SpiVersionHolder.SPI_COMPILE_TIME_VERSION;

        // Short path for the common case
        if (runtimeVersion.equals(compiledVersion)) {
            return;
        }

        int runtimeNumber;
        int compiledNumber;
        try {
            runtimeNumber = Integer.parseInt(runtimeVersion.replaceFirst("-SNAPSHOT$", ""));
            compiledNumber = Integer.parseInt(compiledVersion.replaceFirst("-SNAPSHOT$", ""));
        }
        catch (NumberFormatException e) {
            // Fail compatibility if either version isn't formatted as XXX or XXX-SNAPSHOT
            throw failure(runtimeVersion, compiledVersion, connectorFactory);
        }

        if (runtimeVersion.endsWith("-SNAPSHOT") == compiledVersion.endsWith("-SNAPSHOT")) {
            // Both or neither are snapshots: versions must be identical, which we already checked.
            throw failure(runtimeVersion, compiledVersion, connectorFactory);
        }
        if (runtimeNumber == compiledNumber) {
            // Versions are XXX and XXX-SNAPSHOT
            return;
        }
        if (runtimeVersion.endsWith("-SNAPSHOT") && runtimeNumber == compiledNumber + 1) {
            // Versions are XXX and (XXX+1)-SNAPSHOT
            return;
        }
        if (compiledVersion.endsWith("-SNAPSHOT") && compiledNumber == runtimeNumber + 1) {
            // Versions are XXX and (XXX+1)-SNAPSHOT
            return;
        }

        throw failure(runtimeVersion, compiledVersion, connectorFactory);
    }

    private static IllegalStateException failure(String runtimeVersion, String compiledVersion, ConnectorFactory connectorFactory)
    {
        return new IllegalStateException(format(
                  "Trino SPI version %s is not compatible with the version %s connector %s was compiled for",
                  runtimeVersion,
                  compiledVersion,
                  connectorFactory.getName()));
    }
}
