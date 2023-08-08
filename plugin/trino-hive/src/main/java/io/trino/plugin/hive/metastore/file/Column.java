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
package io.trino.plugin.hive.metastore.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.trino.plugin.hive.HiveType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@Immutable
public class Column
{
    private final String name;
    private final HiveType type;
    private final Optional<String> comment;
    private final Map<String, String> properties;

    @JsonCreator
    public Column(
            @JsonProperty("name") String name,
            @JsonProperty("type") HiveType type,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("properties") Optional<Map<String, String>> properties)
    {
        this(
                name,
                type,
                comment,
                properties.orElse(ImmutableMap.of()));
    }

    public Column(
            String name,
            HiveType type,
            Optional<String> comment,
            Map<String, String> properties)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public HiveType getType()
    {
        return type;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .toString();
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

        Column column = (Column) o;
        return Objects.equals(name, column.name) &&
                Objects.equals(type, column.type) &&
                Objects.equals(comment, column.comment) &&
                Objects.equals(properties, column.properties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, comment, properties);
    }

    public static List<Column> fromMetastoreModel(List<io.trino.plugin.hive.metastore.Column> metastoreColumns)
    {
        return metastoreColumns.stream()
                .map(Column::fromMetastoreModel)
                .collect(toImmutableList());
    }

    public static Column fromMetastoreModel(io.trino.plugin.hive.metastore.Column metastoreColumn)
    {
        return new Column(
                metastoreColumn.getName(),
                metastoreColumn.getType(),
                metastoreColumn.getComment(),
                metastoreColumn.getProperties());
    }

    public static List<io.trino.plugin.hive.metastore.Column> toMetastoreModel(List<Column> fileMetastoreColumns)
    {
        return fileMetastoreColumns.stream()
                .map(Column::toMetastoreModel)
                .collect(toImmutableList());
    }

    public static io.trino.plugin.hive.metastore.Column toMetastoreModel(Column fileMetastoreColumn)
    {
        return new io.trino.plugin.hive.metastore.Column(
                fileMetastoreColumn.getName(),
                fileMetastoreColumn.getType(),
                fileMetastoreColumn.getComment(),
                fileMetastoreColumn.getProperties());
    }
}
