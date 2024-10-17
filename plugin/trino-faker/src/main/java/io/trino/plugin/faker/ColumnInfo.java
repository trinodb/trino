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
package io.trino.plugin.faker;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ColumnInfo
{
    public static final String NULL_PROBABILITY_PROPERTY = "null_probability";
    public static final String GENERATOR_PROPERTY = "generator";

    private final ColumnHandle handle;
    private final String name;
    private final Type type;
    private ColumnMetadata metadata;

    public ColumnInfo(ColumnHandle handle, String name, Type type, Map<String, Object> properties, Optional<String> comment)
    {
        this(handle, ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setProperties(properties)
                .setComment(comment)
                .build());
    }

    public ColumnInfo(ColumnHandle handle, ColumnMetadata metadata)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.name = metadata.getName();
        this.type = metadata.getType();
    }

    public ColumnHandle getHandle()
    {
        return handle;
    }

    public String getName()
    {
        return name;
    }

    public ColumnMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public String toString()
    {
        return name + "::" + type;
    }

    public ColumnInfo withComment(Optional<String> comment)
    {
        return new ColumnInfo(handle, ColumnMetadata.builderFrom(metadata)
                .setComment(comment)
                .build());
    }
}
