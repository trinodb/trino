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
package io.trino.plugin.base.mapping;

import io.trino.spi.security.ConnectorIdentity;

public interface IdentifierMapping
{
    String fromRemoteSchemaName(String remoteSchemaName);

    String fromRemoteTableName(String remoteSchemaName, String remoteTableName);

    String fromRemoteColumnName(String remoteColumnName);

    String toRemoteSchemaName(RemoteIdentifiers remoteIdentifiers, ConnectorIdentity identity, String schemaName);

    String toRemoteTableName(RemoteIdentifiers remoteIdentifiers, ConnectorIdentity identity, String remoteSchema, String tableName);

    String toRemoteColumnName(RemoteIdentifiers remoteIdentifiers, String columnName);
}
