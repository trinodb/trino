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
package io.trino.sql.analyzer;

import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.type.Type;
import io.trino.sql.tree.QualifiedName;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Field
{
    private final Optional<QualifiedObjectName> originTable;
    private final Optional<String> originBranch;
    private final Optional<String> originColumnName;
    private final Optional<QualifiedName> relationAlias;
    private final Optional<String> name;
    private final Type type;
    private final boolean hidden;
    private final boolean aliased;

    public static Builder builder()
    {
        return new Builder();
    }

    public static Field newUnqualified(String name, Type type)
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");

        return builder()
                .name(Optional.of(name))
                .type(type)
                .build();
    }

    public static Field newUnqualified(Optional<String> name, Type type)
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");

        return builder()
                .name(name)
                .type(type)
                .build();
    }

    public static Field newUnqualified(Optional<String> name, Type type, Optional<QualifiedObjectName> originTable, Optional<String> originBranch, Optional<String> originColumn, boolean aliased)
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        requireNonNull(originTable, "originTable is null");
        requireNonNull(originBranch, "originBranch is null");

        return builder()
                .name(name)
                .type(type)
                .originTable(originTable)
                .originBranch(originBranch)
                .originColumnName(originColumn)
                .aliased(aliased)
                .build();
    }

    public static Field newQualified(QualifiedName relationAlias, Optional<String> name, Type type, boolean hidden, Optional<QualifiedObjectName> originTable, Optional<String> originBranch, Optional<String> originColumn, boolean aliased)
    {
        requireNonNull(relationAlias, "relationAlias is null");
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        requireNonNull(originTable, "originTable is null");
        requireNonNull(originBranch, "originBranch is null");

        return builder()
                .relationAlias(Optional.of(relationAlias))
                .name(name)
                .type(type)
                .hidden(hidden)
                .originTable(originTable)
                .originBranch(originBranch)
                .originColumnName(originColumn)
                .aliased(aliased)
                .build();
    }

    private Field(Optional<QualifiedName> relationAlias, Optional<String> name, Type type, boolean hidden, Optional<QualifiedObjectName> originTable, Optional<String> originBranch, Optional<String> originColumnName, boolean aliased)
    {
        requireNonNull(relationAlias, "relationAlias is null");
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        requireNonNull(originTable, "originTable is null");
        requireNonNull(originBranch, "originBranch is null");
        requireNonNull(originColumnName, "originColumnName is null");

        this.relationAlias = relationAlias;
        this.name = name;
        this.type = type;
        this.hidden = hidden;
        this.originTable = originTable;
        this.originBranch = originBranch;
        this.originColumnName = originColumnName;
        this.aliased = aliased;
    }

    public Builder rebuild()
    {
        return new Builder(this);
    }

    public Optional<QualifiedObjectName> getOriginTable()
    {
        return originTable;
    }

    public Optional<String> getOriginBranch()
    {
        return originBranch;
    }

    public Optional<String> getOriginColumnName()
    {
        return originColumnName;
    }

    public Optional<QualifiedName> getRelationAlias()
    {
        return relationAlias;
    }

    public Optional<String> getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public boolean isHidden()
    {
        return hidden;
    }

    public boolean isAliased()
    {
        return aliased;
    }

    public boolean matchesPrefix(Optional<QualifiedName> prefix)
    {
        return prefix.isEmpty() || relationAlias.isPresent() && relationAlias.get().hasSuffix(prefix.get());
    }

    /*
      Namespaces can have names such as "x", "x.y" or "" if there's no name
      Name to resolve can have names like "a", "x.a", "x.y.a"

      namespace  name     possible match
       ""         "a"           y
       "x"        "a"           y
       "x.y"      "a"           y

       ""         "x.a"         n
       "x"        "x.a"         y
       "x.y"      "x.a"         n

       ""         "x.y.a"       n
       "x"        "x.y.a"       n
       "x.y"      "x.y.a"       n

       ""         "y.a"         n
       "x"        "y.a"         n
       "x.y"      "y.a"         y
     */
    public boolean canResolve(QualifiedName name)
    {
        if (this.name.isEmpty()) {
            return false;
        }

        // TODO: need to know whether the qualified name and the name of this field were quoted
        return matchesPrefix(name.getPrefix()) && this.name.get().equalsIgnoreCase(name.getSuffix());
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();
        if (relationAlias.isPresent()) {
            result.append(relationAlias.get())
                    .append(".");
        }

        result.append(name.orElse("<anonymous>"))
                .append(":")
                .append(type);

        return result.toString();
    }

    public static class Builder
    {
        private Optional<QualifiedName> relationAlias = Optional.empty();
        private Optional<String> name = Optional.empty();
        private Type type;
        private boolean hidden;
        private Optional<QualifiedObjectName> originTable = Optional.empty();
        private Optional<String> originBranch = Optional.empty();
        private Optional<String> originColumnName = Optional.empty();
        private boolean aliased;

        private Builder() {}

        private Builder(Field field)
        {
            this.relationAlias = field.relationAlias;
            this.name = field.name;
            this.type = field.type;
            this.hidden = field.hidden;
            this.originTable = field.originTable;
            this.originBranch = field.originBranch;
            this.originColumnName = field.originColumnName;
            this.aliased = field.aliased;
        }

        public Builder relationAlias(Optional<QualifiedName> relationAlias)
        {
            this.relationAlias = requireNonNull(relationAlias, "relationAlias is null");
            return this;
        }

        public Builder name(Optional<String> name)
        {
            this.name = requireNonNull(name, "name is null");
            return this;
        }

        public Builder type(Type type)
        {
            this.type = requireNonNull(type, "type is null");
            return this;
        }

        public Builder hidden(boolean hidden)
        {
            this.hidden = hidden;
            return this;
        }

        public Builder originTable(Optional<QualifiedObjectName> originTable)
        {
            this.originTable = requireNonNull(originTable, "originTable is null");
            return this;
        }

        public Builder originBranch(Optional<String> originBranch)
        {
            this.originBranch = requireNonNull(originBranch, "originBranch is null");
            return this;
        }

        public Builder originColumnName(Optional<String> originColumnName)
        {
            this.originColumnName = requireNonNull(originColumnName, "originColumnName is null");
            return this;
        }

        public Builder aliased(boolean aliased)
        {
            this.aliased = aliased;
            return this;
        }

        public Field build()
        {
            requireNonNull(type, "type is null");
            return new Field(relationAlias, name, type, hidden, originTable, originBranch, originColumnName, aliased);
        }
    }
}
