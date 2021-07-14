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
package io.trino.plugin.iceberg;

import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.hadoop.HadoopCatalog;

import javax.inject.Inject;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

class TrinoCatalogFactory
{
    private final IcebergConfig config;
    private final CatalogName catalogName;
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final HiveTableOperationsProvider tableOperationsProvider;

    @Inject
    public TrinoCatalogFactory(
            CatalogName catalogName,
            IcebergConfig config,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            HiveTableOperationsProvider tableOperationsProvider)
    {
        this.config = requireNonNull(config, "config is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationProvider is null");
    }

    public TrinoCatalog create()
    {
        switch (config.getCatalogType()) {
            case HIVE:
                return new TrinoHiveCatalog(catalogName, metastore, hdfsEnvironment, typeManager, tableOperationsProvider);
            case HADOOP:
                return new TrinoIcebergCatalog(HadoopCatalog.class.getName(), catalogName, config, hdfsEnvironment);
            default:
                throw new TrinoException(NOT_SUPPORTED, "Unsupported Trino Iceberg catalog type " + config.getCatalogType());
        }
    }
}
