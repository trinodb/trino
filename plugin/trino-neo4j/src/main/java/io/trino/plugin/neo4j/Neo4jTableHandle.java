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
package io.trino.plugin.neo4j;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class Neo4jTableHandle
        implements ConnectorTableHandle
{
    private final Neo4jRelationHandle relationHandle;

    @JsonCreator
    public Neo4jTableHandle(
            @JsonProperty("relationHandle") Neo4jRelationHandle relationHandle)
    {
        this.relationHandle = requireNonNull(relationHandle, "relationHandle is null");
    }

    @JsonProperty
    public Neo4jRelationHandle getRelationHandle()
    {
        return relationHandle;
    }

    @JsonIgnore
    public Neo4jNamedRelationHandle getRequiredNamedRelation()
    {
        checkState(isNamedRelation(), "The table handle does not represent a named relation: %s", this);
        return (Neo4jNamedRelationHandle) relationHandle;
    }

    @JsonIgnore
    public Optional<Neo4jNamedRelationHandle> getNamedRelation()
    {
        if (relationHandle instanceof Neo4jNamedRelationHandle namedRelationHandle) {
            return Optional.of(namedRelationHandle);
        }
        else {
            return Optional.empty();
        }
    }

    @JsonIgnore
    public boolean isSynthetic()
    {
        return !isNamedRelation();
    }

    @JsonIgnore
    public boolean isNamedRelation()
    {
        return relationHandle instanceof Neo4jNamedRelationHandle;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("relationHandle", relationHandle)
                .toString();
    }
}
