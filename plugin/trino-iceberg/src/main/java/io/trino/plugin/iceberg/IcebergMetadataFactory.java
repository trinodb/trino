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

import io.airlift.json.JsonCodec;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class IcebergMetadataFactory
{
    private final TypeManager typeManager;
    private final TypeOperators typeOperators;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final TrinoCatalogFactory catalogFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final long globalMetadataCacheTtl;
    private final int maxCacheSize;
    private final long globalMetadataCacheTtlForListing;
    private final CatalogType catalogType;

    @Inject
    public IcebergMetadataFactory(
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec,
            TrinoCatalogFactory catalogFactory,
            HdfsEnvironment hdfsEnvironment, IcebergConfig icebergConfig)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        // TODO consider providing TypeOperators in ConnectorContext to increase cache reuse
        this.typeOperators = new TypeOperators();
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.globalMetadataCacheTtl = icebergConfig.getGlobalMetadataCacheTtl();
        this.maxCacheSize = icebergConfig.getMaxCacheSize();
        this.globalMetadataCacheTtlForListing = icebergConfig.getMetadataCacheSchemaTableListingTtl();
        this.catalogType = icebergConfig.getCatalogType();
    }

    public TrinoCatalogFactory getCatalogFactory()
    {
        return catalogFactory;
    }

    public IcebergMetadata create(ConnectorIdentity identity)
    {
        return new IcebergMetadata(typeManager, typeOperators, commitTaskCodec, catalogFactory.create(identity), hdfsEnvironment);
    }

    public CachedIcebergMetadata createCachedMetadata(ConnectorIdentity identity)
    {
        return new CachedIcebergMetadata(this, catalogType, typeManager, typeOperators, commitTaskCodec, catalogFactory.create(identity), hdfsEnvironment, globalMetadataCacheTtl, maxCacheSize, globalMetadataCacheTtlForListing);
    }
}
