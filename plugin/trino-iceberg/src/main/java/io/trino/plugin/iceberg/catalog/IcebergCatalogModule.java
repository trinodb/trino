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
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.file.IcebergFileMetastoreCatalogModule;
import io.trino.plugin.iceberg.catalog.glue.IcebergGlueCatalogModule;
import io.trino.plugin.iceberg.catalog.hms.IcebergHiveMetastoreCatalogModule;
import io.trino.plugin.iceberg.catalog.jdbc.IcebergJdbcCatalogModule;
import io.trino.plugin.iceberg.catalog.nessie.IcebergNessieCatalogModule;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogModule;
import io.trino.plugin.iceberg.catalog.snowflake.IcebergSnowflakeCatalogModule;

public class IcebergCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        switch (buildConfigObject(IcebergConfig.class).getCatalogType()) {
            case HIVE_METASTORE -> install(new IcebergHiveMetastoreCatalogModule());
            case TESTING_FILE_METASTORE -> install(new IcebergFileMetastoreCatalogModule());
            case GLUE -> install(new IcebergGlueCatalogModule());
            case REST -> install(new IcebergRestCatalogModule());
            case JDBC -> install(new IcebergJdbcCatalogModule());
            case NESSIE -> install(new IcebergNessieCatalogModule());
            case SNOWFLAKE -> install(new IcebergSnowflakeCatalogModule());
        }
    }
}
