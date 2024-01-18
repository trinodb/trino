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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RemoteColumnNameCacheKey
{
    private final ConnectorIdentity identity;
    private final String schema;
    private final String table;

    RemoteColumnNameCacheKey(ConnectorIdentity identity, String schema, String table)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteColumnNameCacheKey that = (RemoteColumnNameCacheKey) o;
        return Objects.equals(identity, that.identity) &&
                Objects.equals(schema, that.schema) &&
                Objects.equals(table, that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(identity, schema, table);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("identity", identity)
                .add("schema", schema)
                .add("table", table)
                .toString();
    }
}
