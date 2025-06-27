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
package io.trino.plugin.lakehouse;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorContext;

import static java.util.Objects.requireNonNull;

class LakehouseFileSystemModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;
    private final NodeManager nodeManager;
    private final OpenTelemetry openTelemetry;

    public LakehouseFileSystemModule(String catalogName, ConnectorContext context)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.nodeManager = context.getNodeManager();
        this.openTelemetry = context.getOpenTelemetry();
    }

    @Override
    protected void setup(Binder binder)
    {
        boolean metadataCacheEnabled = buildConfigObject(IcebergConfig.class).isMetadataCacheEnabled();
        install(new FileSystemModule(catalogName, nodeManager, openTelemetry, metadataCacheEnabled));
    }
}
