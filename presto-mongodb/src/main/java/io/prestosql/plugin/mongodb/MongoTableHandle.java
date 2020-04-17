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
package io.prestosql.plugin.mongodb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class MongoTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private final TupleDomain<ColumnHandle> constraint;

    public MongoTableHandle(SchemaTableName schemaTableName)
    {
        this(schemaTableName, TupleDomain.all());
    }

    @JsonCreator
    public MongoTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, constraint);
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
        MongoTableHandle other = (MongoTableHandle) obj;
        return Objects.equals(this.schemaTableName, other.schemaTableName) &&
                Objects.equals(this.constraint, other.constraint);
    }

    @Override
    public String toString()
    {
        return schemaTableName.toString();
    }
}
