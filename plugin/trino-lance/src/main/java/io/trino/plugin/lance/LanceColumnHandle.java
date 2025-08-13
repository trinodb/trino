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
package io.trino.plugin.lance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public class LanceColumnHandle
        implements ColumnHandle
{
    private final int id;
    private final String name;
    private final Type type;

    @JsonCreator
    public LanceColumnHandle(
            @JsonProperty("id") int id,
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type)
    {
        this.id = id;
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public int getId()
    {
        return id;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }
}
