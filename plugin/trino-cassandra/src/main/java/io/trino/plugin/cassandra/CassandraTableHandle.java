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
package io.trino.plugin.cassandra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CassandraTableHandle
        implements ConnectorTableHandle
{
    private final CassandraRelationHandle relationHandle;

    @JsonCreator
    public CassandraTableHandle(@JsonProperty("relationHandle") CassandraRelationHandle relationHandle)
    {
        this.relationHandle = requireNonNull(relationHandle, "relationHandle is null");
    }

    @JsonProperty
    public CassandraRelationHandle getRelationHandle()
    {
        return relationHandle;
    }

    @JsonIgnore
    public CassandraNamedRelationHandle getRequiredNamedRelation()
    {
        checkState(isNamedRelation(), "The table handle does not represent a named relation: %s", this);
        return (CassandraNamedRelationHandle) relationHandle;
    }

    @JsonIgnore
    public boolean isSynthetic()
    {
        return !isNamedRelation();
    }

    @JsonIgnore
    public boolean isNamedRelation()
    {
        return relationHandle instanceof CassandraNamedRelationHandle;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relationHandle);
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
        CassandraTableHandle other = (CassandraTableHandle) obj;
        return Objects.equals(this.relationHandle, other.relationHandle);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("relationHandle", relationHandle)
                .toString();
    }
}
