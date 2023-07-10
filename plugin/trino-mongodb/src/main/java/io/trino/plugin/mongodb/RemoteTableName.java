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
package io.trino.plugin.mongodb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class RemoteTableName
{
    private final String databaseName;
    private final String collectionName;

    @JsonCreator
    public RemoteTableName(@JsonProperty String databaseName, @JsonProperty String collectionName)
    {
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.collectionName = requireNonNull(collectionName, "collectionName is null");
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteTableName that = (RemoteTableName) o;
        return databaseName.equals(that.databaseName) &&
                collectionName.equals(that.collectionName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(databaseName, collectionName);
    }

    @Override
    public String toString()
    {
        return databaseName + "." + collectionName;
    }
}
