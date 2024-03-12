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
package io.trino.plugin.iceberg.catalog.snowflake;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import org.apache.iceberg.snowflake.TrinoIcebergSnowflakeCatalogFactory;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class IcebergSnowflakeCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(IcebergSnowflakeCatalogConfig.class);
        binder.bind(IcebergTableOperationsProvider.class).to(SnowflakeIcebergTableOperationsProvider.class).in(Scopes.SINGLETON);
        binder.bind(TrinoCatalogFactory.class).to(TrinoIcebergSnowflakeCatalogFactory.class).in(Scopes.SINGLETON);

        IcebergConfig icebergConfig = buildConfigObject(IcebergConfig.class);
        if (icebergConfig.getFileFormat() != IcebergFileFormat.PARQUET) {
            throw new RuntimeException("Snowflake only supports Iceberg tables that use the Parquet file format");
        }
    }
}
