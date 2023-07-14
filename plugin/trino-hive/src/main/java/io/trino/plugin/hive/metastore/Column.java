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
package io.trino.plugin.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.trino.plugin.hive.HiveType;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class Column
{
    private final String name;
    private final String typeName;
    private final Optional<String> comment;

    @Deprecated
    public Column(
            String name,
            HiveType hiveType,
            Optional<String> comment)
    {
        this(name, hiveType.toString(), comment);
    }

    @JsonCreator
    public Column(
            @JsonProperty("name") String name,
            @JsonProperty("typeName") String typeName,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.name = requireNonNull(name, "name is null");
        this.typeName = requireNonNull(typeName, "typeName is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getTypeName()
    {
        return typeName;
    }

    @Deprecated
    @JsonIgnore
    public HiveType getType()
    {
        return getHiveType();
    }

    @JsonIgnore
    public HiveType getHiveType()
    {
        return HiveType.valueOf(typeName);
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("typeName", typeName)
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
                Objects.equals(typeName, column.typeName) &&
                Objects.equals(comment, column.comment);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, typeName, comment);
    }
}
