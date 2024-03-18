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

package io.trino.plugin.warp;

import com.starburstdata.trino.plugin.license.LicenseManager;
import io.trino.plugin.varada.dispatcher.CachingPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class WarpPlugin
        extends CachingPlugin
        implements Plugin
{
    private final LicenseManager licenseManager;

    public WarpPlugin()
    {
        this(() -> true);
    }

    public WarpPlugin(LicenseManager licenseManager)
    {
        this.licenseManager = requireNonNull(licenseManager);
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return List.of(new StarburstWarpConnectorFactory(super.getConnectorFactory(), licenseManager, Collections.emptyList()));
    }
}
