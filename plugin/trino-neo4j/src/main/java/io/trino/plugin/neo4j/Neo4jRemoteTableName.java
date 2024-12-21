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
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class Neo4jRemoteTableName
{
    public enum Type
    {
        NODE, RELATIONSHIP
    }

    private final String databaseName;
    private final String name;
    private final Type type;

    @JsonCreator
    public Neo4jRemoteTableName(
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type)
    {
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public String getDatabaseName()
    {
        return this.databaseName;
    }

    @JsonProperty
    public String getName()
    {
        return this.name;
    }

    @JsonProperty
    public Type getType()
    {
        return this.type;
    }

    @Override
    public String toString()
    {
        return this.databaseName + '.' + this.name;
    }
}
