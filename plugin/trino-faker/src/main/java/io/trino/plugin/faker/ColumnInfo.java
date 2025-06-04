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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record ColumnInfo(FakerColumnHandle handle, ColumnMetadata metadata)
{
    public static final String NULL_PROBABILITY_PROPERTY = "null_probability";
    public static final String GENERATOR_PROPERTY = "generator";
    public static final String MIN_PROPERTY = "min";
    public static final String MAX_PROPERTY = "max";
    public static final String ALLOWED_VALUES_PROPERTY = "allowed_values";
    public static final String STEP_PROPERTY = "step";

    public ColumnInfo
    {
        requireNonNull(handle, "handle is null");
        requireNonNull(metadata, "metadata is null");
    }

    public String name()
    {
        return metadata.getName();
    }

    public Type type()
    {
        return metadata.getType();
    }

    @Override
    public String toString()
    {
        return metadata.getName() + "::" + metadata.getType();
    }

    public ColumnInfo withComment(Optional<String> comment)
    {
        return new ColumnInfo(handle, ColumnMetadata.builderFrom(metadata)
                .setComment(comment)
                .build());
    }

    public ColumnInfo withHandle(FakerColumnHandle handle)
    {
        return new ColumnInfo(handle, metadata);
    }

    public ColumnInfo withProperties(Map<String, Object> properties)
    {
        return new ColumnInfo(handle, ColumnMetadata.builderFrom(metadata)
                .setProperties(ImmutableMap.copyOf(properties))
                .build());
    }
}
