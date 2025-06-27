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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorIndexHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ThriftIndexHandle
        implements ConnectorIndexHandle
{
    private final SchemaTableName schemaTableName;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public ThriftIndexHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ThriftIndexHandle other = (ThriftIndexHandle) obj;
        return Objects.equals(this.schemaTableName, other.schemaTableName) &&
                Objects.equals(this.tupleDomain, other.tupleDomain);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, tupleDomain);
    }

    @Override
    public String toString()
    {
        return format("%s, constraint = %s", schemaTableName, tupleDomain.toString());
    }
}
