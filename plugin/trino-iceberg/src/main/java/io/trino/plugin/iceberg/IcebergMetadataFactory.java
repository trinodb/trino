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

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

public class IcebergMetadataFactory
{
    private final TypeManager typeManager;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final TrinoCatalogFactory catalogFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TableStatisticsWriter tableStatisticsWriter;

    @Inject
    public IcebergMetadataFactory(
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec,
            TrinoCatalogFactory catalogFactory,
            TrinoFileSystemFactory fileSystemFactory,
            TableStatisticsWriter tableStatisticsWriter)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.tableStatisticsWriter = requireNonNull(tableStatisticsWriter, "tableStatisticsWriter is null");
    }

    public IcebergMetadata create(ConnectorIdentity identity)
    {
        return new IcebergMetadata(
                typeManager,
                commitTaskCodec,
                catalogFactory.create(identity),
                fileSystemFactory,
                tableStatisticsWriter);
    }
}
