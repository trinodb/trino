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
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class TrinoCatalogFactory
{
    private final CatalogName catalogName;
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final HiveTableOperationsProvider tableOperationsProvider;
    private final String trinoVersion;
    private final CatalogType catalogType;
    private final boolean isUniqueTableLocation;

    @Inject
    public TrinoCatalogFactory(
            IcebergConfig config,
            CatalogName catalogName,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            HiveTableOperationsProvider tableOperationsProvider,
            NodeVersion nodeVersion)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationProvider is null");
        this.trinoVersion = requireNonNull(nodeVersion, "trinoVersion is null").toString();
        requireNonNull(config, "config is null");
        this.catalogType = config.getCatalogType();
        this.isUniqueTableLocation = config.isUniqueTableLocation();
    }

    public TrinoCatalog create()
    {
        switch (catalogType) {
            case HIVE:
                return new TrinoHiveCatalog(catalogName, memoizeMetastore(metastore, 1000), hdfsEnvironment, typeManager, tableOperationsProvider, trinoVersion, isUniqueTableLocation);
            case UNKNOWN:
                throw new TrinoException(NOT_SUPPORTED, "Unknown Trino Iceberg catalog type");
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported Trino Iceberg catalog type " + catalogType);
    }
}
