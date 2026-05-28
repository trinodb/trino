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

import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.SchemaTableName;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class IcebergTableCredentialsProvider
{
    private final TrinoCatalog catalog;
    private final Map<SchemaTableName, IcebergTableCredentials> tableCredentials = new ConcurrentHashMap<>();

    public IcebergTableCredentialsProvider(TrinoCatalog catalog)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return Optional.of(tableCredentials.computeIfAbsent(schemaTableName, key ->
                new IcebergTableCredentials(catalog.loadTable(session, key).io().properties())));
    }

    public void putTableCredentials(SchemaTableName schemaTableName, IcebergTableCredentials credentials)
    {
        tableCredentials.put(schemaTableName, credentials);
    }
}
