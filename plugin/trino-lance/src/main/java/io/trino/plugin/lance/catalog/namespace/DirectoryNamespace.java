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
package io.trino.plugin.lance.catalog.namespace;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.lance.catalog.BaseTable;
import io.trino.plugin.lance.catalog.TrinoCatalog;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DirectoryNamespace
        implements TrinoCatalog
{
    public static final String DEFAULT_NAMESPACE = "default";
    private final TrinoFileSystemFactory fileSystemFactory;
    private final Location warehouseLocation;

    @Inject
    public DirectoryNamespace(TrinoFileSystemFactory fileSystemFactory, DirectoryNamespaceConfig config)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.warehouseLocation = Location.of(config.getWarehouseLocation());
    }

    private static String directoryName(Location directory, Location location)
    {
        String prefix = directory.path();
        if (!prefix.isEmpty() && !prefix.endsWith("/")) {
            prefix += "/";
        }
        String path = location.path();
        verify(path.endsWith("/"), "path does not endRowPosition with slash: %s", location);
        verify(path.startsWith(prefix), "path [%s] is not a child of directory [%s]", location, directory);
        return path.substring(prefix.length(), path.length() - 1);
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        return List.of(DEFAULT_NAMESPACE);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        checkArgument(namespace.isEmpty() || namespace.get().equals(DEFAULT_NAMESPACE));
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        try {
            for (Location location : fileSystem.listDirectories(warehouseLocation)) {
                String directoryName = directoryName(warehouseLocation, location);
                if (directoryName.endsWith(BaseTable.LANCE_SUFFIX)) {
                    String tableName = directoryName.substring(0, directoryName.length() - BaseTable.LANCE_SUFFIX.length());
                    builder.add(new SchemaTableName(DEFAULT_NAMESPACE, tableName));
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(format("Failed to list tables under %s:", warehouseLocation), e);
        }

        return builder.build();
    }

    @Override
    public Optional<BaseTable> loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        checkArgument(schemaTableName.getSchemaName().equals(DEFAULT_NAMESPACE));
        Location tableLocation = warehouseLocation.appendPath(schemaTableName.getTableName() + BaseTable.LANCE_SUFFIX);
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        try {
            Optional<Boolean> tableExists = fileSystem.directoryExists(tableLocation);
            if (tableExists.isPresent() && tableExists.get()) {
                return Optional.of(new BaseTable(fileSystem, tableLocation));
            }
            return Optional.empty();
        }
        catch (IOException e) {
            throw new RuntimeException(format("Failed to check if table exists under %s:", warehouseLocation), e);
        }
    }
}
