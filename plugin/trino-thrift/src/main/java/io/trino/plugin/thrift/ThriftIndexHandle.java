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
package io.trino.plugin.thrift;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorIndexHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public record ThriftIndexHandle(SchemaTableName schemaTableName, TupleDomain<ColumnHandle> tupleDomain)
        implements ConnectorIndexHandle
{
    public ThriftIndexHandle
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(tupleDomain, "tupleDomain is null");
    }

    @Override
    public String toString()
    {
        return format("%s, constraint = %s", schemaTableName, tupleDomain.toString());
    }
}
