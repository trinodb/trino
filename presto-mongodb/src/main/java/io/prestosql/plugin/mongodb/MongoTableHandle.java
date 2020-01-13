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
    private final String databaseName; // case sensitive name
    private final String collectionName; // case sensitive name
    private final TupleDomain<ColumnHandle> constraint;

    public MongoTableHandle(SchemaTableName table)
    {
        this(table.getSchemaName(), table.getTableName(), TupleDomain.all());
    }

    @JsonCreator
    public MongoTableHandle(
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("collectionName") String collectionName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.collectionName = requireNonNull(collectionName, "collectionName is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(databaseName, collectionName);
    }

    @JsonProperty
    public String getDatabaseName()
    {
        return databaseName;
    }

    @JsonProperty
    public String getCollectionName()
    {
        return collectionName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(databaseName, collectionName);
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
        return Objects.equals(this.databaseName, other.databaseName) &&
                Objects.equals(this.collectionName, other.collectionName);
    }

    @Override
    public String toString()
    {
        return String.format("%s.%s", databaseName, collectionName);
    }
}
