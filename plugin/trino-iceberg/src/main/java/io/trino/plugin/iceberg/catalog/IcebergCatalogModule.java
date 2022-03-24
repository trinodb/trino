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
package io.trino.plugin.iceberg.catalog;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.iceberg.CatalogType;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.file.IcebergFileMetastoreCatalogModule;
import io.trino.plugin.iceberg.catalog.glue.IcebergGlueCatalogModule;
import io.trino.plugin.iceberg.catalog.hms.IcebergHiveMetastoreCatalogModule;
import io.trino.plugin.iceberg.catalog.jdbc.IcebergJdbcCatalogModule;
import io.trino.plugin.iceberg.catalog.nessie.IcebergNessieCatalogModule;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogModule;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.trino.plugin.iceberg.CatalogType.GLUE;
import static io.trino.plugin.iceberg.CatalogType.HIVE_METASTORE;
import static io.trino.plugin.iceberg.CatalogType.JDBC;
import static io.trino.plugin.iceberg.CatalogType.NESSIE;
import static io.trino.plugin.iceberg.CatalogType.REST;
import static io.trino.plugin.iceberg.CatalogType.TESTING_FILE_METASTORE;

public class IcebergCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindCatalogModule(HIVE_METASTORE, new IcebergHiveMetastoreCatalogModule());
        bindCatalogModule(TESTING_FILE_METASTORE, new IcebergFileMetastoreCatalogModule());
        bindCatalogModule(GLUE, new IcebergGlueCatalogModule());
        bindCatalogModule(REST, new IcebergRestCatalogModule());
        bindCatalogModule(JDBC, new IcebergJdbcCatalogModule());
        bindCatalogModule(NESSIE, new IcebergNessieCatalogModule());
    }

    private void bindCatalogModule(CatalogType catalogType, Module module)
    {
        install(conditionalModule(
                IcebergConfig.class,
                config -> config.getCatalogType() == catalogType,
                module));
    }
}
