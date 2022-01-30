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
package io.trino.metadata;

import io.trino.spi.type.TypeId;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ViewColumn
{
    private final String name;
    private final TypeId type;
    private final Optional<String> comment;

    public ViewColumn(String name, TypeId type)
    {
        this(name, type, Optional.empty());
    }

    public ViewColumn(String name, TypeId type, Optional<String> comment)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.comment = comment;
    }

    public String getName()
    {
        return name;
    }

    public TypeId getType()
    {
        return type;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).omitNullValues()
                .add("name", name)
                .add("type", type)
                .add("comment", comment.orElse(null))
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
        ViewColumn that = (ViewColumn) o;
        return Objects.equals(name, that.name) && Objects.equals(type, that.type) && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, comment);
    }
}
