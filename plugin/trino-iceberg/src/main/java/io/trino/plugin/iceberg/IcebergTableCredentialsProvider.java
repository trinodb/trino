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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ReferencingView;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class IcebergTableCredentialsProvider
{
    private final TrinoCatalog catalog;
    private final Map<TableCredentialsKey, IcebergTableCredentials> tableCredentials = new ConcurrentHashMap<>();

    public IcebergTableCredentialsProvider(TrinoCatalog catalog)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession session, SchemaTableName schemaTableName, List<ReferencingView> referencingViews)
    {
        return Optional.of(tableCredentials.computeIfAbsent(new TableCredentialsKey(schemaTableName, referencingViews), key ->
                IcebergTableCredentials.forFileIO(catalog.loadTable(session, key.schemaTableName(), key.referencingViews()).io())));
    }

    public void putTableCredentials(SchemaTableName schemaTableName, IcebergTableCredentials credentials)
    {
        tableCredentials.put(new TableCredentialsKey(schemaTableName, ImmutableList.of()), credentials);
    }

    // Credentials obtained through a view must not be served to reads of the table that do not go through that view
    private record TableCredentialsKey(SchemaTableName schemaTableName, List<ReferencingView> referencingViews)
    {
        private TableCredentialsKey
        {
            requireNonNull(schemaTableName, "schemaTableName is null");
            referencingViews = ImmutableList.copyOf(requireNonNull(referencingViews, "referencingViews is null"));
        }
    }
}
