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
package io.trino.plugin.iceberg.catalog.jdbc;

import org.apache.iceberg.exceptions.CommitFailedException;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IcebergJdbcClient
{
    private final IcebergJdbcConnectionFactory connectionFactory;
    private final String catalogName;

    public IcebergJdbcClient(IcebergJdbcConnectionFactory connectionFactory, String catalogName)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    public void createTable(String schemaName, String tableName, String metadataLocation)
    {
        try (Handle handle = Jdbi.open(connectionFactory)) {
            handle.createUpdate("" +
                            "INSERT INTO iceberg_tables " +
                            "(catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location) " +
                            "VALUES (:catalog, :schema, :table, :metadata_location, null)")
                    .bind("catalog", catalogName)
                    .bind("schema", schemaName)
                    .bind("table", tableName)
                    .bind("metadata_location", metadataLocation)
                    .execute();
        }
    }

    public void alterTable(String schemaName, String tableName, String newMetadataLocation, String previousMetadataLocation)
    {
        try (Handle handle = Jdbi.open(connectionFactory)) {
            int updatedRecords = handle.createUpdate("" +
                            "UPDATE iceberg_tables " +
                            "SET metadata_location = :metadata_location, previous_metadata_location = :previous_metadata_location " +
                            "WHERE catalog_name = :catalog AND table_namespace = :schema AND table_name = :table AND metadata_location = :previous_metadata_location")
                    .bind("metadata_location", newMetadataLocation)
                    .bind("previous_metadata_location", previousMetadataLocation)
                    .bind("catalog", catalogName)
                    .bind("schema", schemaName)
                    .bind("table", tableName)
                    .execute();
            if (updatedRecords != 1) {
                throw new CommitFailedException("Failed to update table due to concurrent updates");
            }
        }
    }

    public Optional<String> getMetadataLocation(String schemaName, String tableName)
    {
        try (Handle handle = Jdbi.open(connectionFactory)) {
            return handle.createQuery("" +
                            "SELECT metadata_location " +
                            "FROM iceberg_tables " +
                            "WHERE catalog_name = :catalog AND table_namespace = :schema AND table_name = :table")
                    .bind("catalog", catalogName)
                    .bind("schema", schemaName)
                    .bind("table", tableName)
                    .mapTo(String.class)
                    .findOne();
        }
    }
}
