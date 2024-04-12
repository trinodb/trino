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

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.trino.spi.connector.ConnectorTableHandle;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public record CassandraTableHandle(CassandraRelationHandle relationHandle)
        implements ConnectorTableHandle
{
    public CassandraTableHandle
    {
        requireNonNull(relationHandle, "relationHandle is null");
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
}
