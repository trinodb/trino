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

package io.trino.plugin.jdbc.mapping;

import io.trino.spi.security.ConnectorIdentity;

import java.sql.Connection;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public abstract class ForwardingIdentifierMapping
        implements IdentifierMapping
{
    public static IdentifierMapping of(Supplier<IdentifierMapping> delegateSupplier)
    {
        requireNonNull(delegateSupplier, "delegateSupplier is null");
        return new ForwardingIdentifierMapping()
        {
            @Override
            protected IdentifierMapping delegate()
            {
                return delegateSupplier.get();
            }
        };
    }

    protected abstract IdentifierMapping delegate();

    @Override
    public String fromRemoteSchemaName(String remoteSchemaName)
    {
        return delegate().fromRemoteSchemaName(remoteSchemaName);
    }

    @Override
    public String fromRemoteTableName(String remoteSchemaName, String remoteTableName)
    {
        return delegate().fromRemoteTableName(remoteSchemaName, remoteTableName);
    }

    @Override
    public String fromRemoteColumnName(String remoteColumnName)
    {
        return delegate().fromRemoteColumnName(remoteColumnName);
    }

    @Override
    public String toRemoteSchemaName(ConnectorIdentity identity, Connection connection, String schemaName)
    {
        return delegate().toRemoteSchemaName(identity, connection, schemaName);
    }

    @Override
    public String toRemoteTableName(ConnectorIdentity identity, Connection connection, String remoteSchema, String tableName)
    {
        return delegate().toRemoteTableName(identity, connection, remoteSchema, tableName);
    }

    @Override
    public String toRemoteColumnName(Connection connection, String columnName)
    {
        return delegate().toRemoteColumnName(connection, columnName);
    }
}
