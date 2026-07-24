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
package io.trino.plugin.doris;

import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

public record DorisTableHandle(
        String schemaName,
        String tableName,
        String remoteSchemaName,
        String remoteTableName,
        DorisRelationType relationType)
        implements ConnectorTableHandle
{
    public DorisTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(remoteSchemaName, "remoteSchemaName is null");
        requireNonNull(remoteTableName, "remoteTableName is null");
        requireNonNull(relationType, "relationType is null");
    }

    public DorisTableHandle(String schemaName, String tableName)
    {
        this(schemaName, tableName, schemaName, tableName, DorisRelationType.TABLE);
    }

    public DorisTableHandle(String schemaName, String tableName, String remoteSchemaName, String remoteTableName)
    {
        this(schemaName, tableName, remoteSchemaName, remoteTableName, DorisRelationType.TABLE);
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }
}
