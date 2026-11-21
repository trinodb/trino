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
package io.trino.plugin.memory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.trino.spi.connector.ColumnMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public record ColumnInfo(MemoryColumnHandle handle, Optional<String> defaultValue, boolean nullable, Optional<String> comment, List<FieldDefault> fieldDefaults)
{
    public ColumnInfo
    {
        requireNonNull(handle, "handle is null");
        requireNonNull(defaultValue, "defaultValue is null");
        requireNonNull(comment, "comment is null");
        fieldDefaults = List.copyOf(fieldDefaults);
    }

    public ColumnInfo(MemoryColumnHandle handle, Optional<String> defaultValue, boolean nullable, Optional<String> comment, Map<List<String>, String> fieldDefaults)
    {
        this(handle, defaultValue, nullable, comment, fieldDefaults.entrySet().stream()
                .map(entry -> new FieldDefault(entry.getKey(), entry.getValue()))
                .collect(toImmutableList()));
    }

    @JsonIgnore
    public ColumnMetadata getMetadata()
    {
        return ColumnMetadata.builder()
                .setName(handle.name())
                .setType(handle.type())
                .setDefaultValue(defaultValue)
                .setNullable(nullable)
                .setComment(comment)
                .setFieldDefaults(fieldDefaults.stream().collect(toImmutableMap(FieldDefault::path, FieldDefault::value)))
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", handle.name())
                .add("type", handle.type())
                .add("defaultValue", defaultValue)
                .add("nullable", nullable)
                .add("comment", comment)
                .add("fieldDefaults", fieldDefaults)
                .toString();
    }

    public record FieldDefault(List<String> path, String value)
    {
        public FieldDefault
        {
            path = List.copyOf(path);
            requireNonNull(value, "value is null");
        }
    }
}
