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
package io.trino.plugin.iceberg.catalog.dynamodb;

import io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class DynamoDbIcebergTableOperations
        extends AbstractIcebergTableOperations
{
    private final DynamoDbIcebergClient dynamoClient;

    public DynamoDbIcebergTableOperations(
            FileIO fileIo,
            DynamoDbIcebergClient dynamoClient,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, session, database, table, owner, location);
        this.dynamoClient = requireNonNull(dynamoClient, "dynamoClient is null");
    }

    @Override
    protected String getRefreshedLocation(boolean invalidateCaches)
    {
        return dynamoClient.getMetadataLocation(database, tableName)
                .orElseThrow(() -> new TableNotFoundException(getSchemaTableName()));
    }

    @Override
    protected void commitNewTable(TableMetadata metadata)
    {
        verify(version == -1, "commitNewTable called on a table which already exists");
        dynamoClient.createTable(database, tableName, metadata.schema(), writeNewMetadata(metadata, 0));
        shouldRefresh = true;
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        checkState(currentMetadataLocation != null, "No current metadata location for existing table");
        dynamoClient.alterTable(database, tableName, metadata.schema(), metadata.properties(), writeNewMetadata(metadata, version + 1), currentMetadataLocation);
        shouldRefresh = true;
    }
}
