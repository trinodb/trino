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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record TableInfo(List<ColumnInfo> columns, Map<String, Object> properties, Optional<String> comment)
{
    public static final String NULL_PROBABILITY_PROPERTY = "null_probability";
    public static final String DEFAULT_LIMIT_PROPERTY = "default_limit";
    public static final String SEQUENCE_DETECTION_ENABLED = "sequence_detection_enabled";
    public static final String DICTIONARY_DETECTION_ENABLED = "dictionary_detection_enabled";

    public TableInfo
    {
        columns = ImmutableList.copyOf(columns);
        properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        requireNonNull(comment, "comment is null");
    }

    public ColumnInfo column(ColumnHandle handle)
    {
        return columns.stream()
                .filter(column -> column.handle().equals(handle))
                .findFirst()
                .orElseThrow();
    }

    public TableInfo withColumns(List<ColumnInfo> columns)
    {
        return new TableInfo(columns, properties, comment);
    }

    public TableInfo withProperties(Map<String, Object> properties)
    {
        return new TableInfo(columns, properties, comment);
    }

    public TableInfo withComment(Optional<String> comment)
    {
        return new TableInfo(columns, properties, comment);
    }
}
