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

import com.google.common.collect.ImmutableList;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.type.Type;
import io.trino.sql.tree.QualifiedName;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Field
{
    private final List<OriginColumnDetail> originColumnDetails;
    private final Optional<QualifiedName> relationAlias;
    private final Optional<String> name;
    private final Type type;
    private final boolean hidden;
    private final boolean aliased;

    public static Field newUnqualified(String name, Type type)
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");

        return new Field(Optional.empty(), Optional.of(name), type, false, ImmutableList.of(), false);
    }

    public static Field newUnqualified(Optional<String> name, Type type)
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");

        return new Field(Optional.empty(), name, type, false, ImmutableList.of(), false);
    }

    public static Field newUnqualified(Optional<String> name, Type type, List<OriginColumnDetail> originColumnDetails, boolean aliased)
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        requireNonNull(originColumnDetails, "originTable is null");

        return new Field(Optional.empty(), name, type, false, originColumnDetails, aliased);
    }

    public static Field newQualified(QualifiedName relationAlias, Optional<String> name, Type type, boolean hidden, List<OriginColumnDetail> originColumnDetails, boolean aliased)
    {
        requireNonNull(relationAlias, "relationAlias is null");
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        requireNonNull(originColumnDetails, "originTable is null");

        return new Field(Optional.of(relationAlias), name, type, hidden, originColumnDetails, aliased);
    }

    public Field(Optional<QualifiedName> relationAlias, Optional<String> name, Type type, boolean hidden, List<OriginColumnDetail> originColumnDetails, boolean aliased)
    {
        requireNonNull(relationAlias, "relationAlias is null");
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        requireNonNull(originColumnDetails, "originColumnDetails is null");

        this.relationAlias = relationAlias;
        this.name = name;
        this.type = type;
        this.hidden = hidden;
        this.originColumnDetails = ImmutableList.copyOf(originColumnDetails);
        this.aliased = aliased;
    }

    public List<OriginColumnDetail> getOriginColumnDetails()
    {
        return originColumnDetails;
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

    public static class OriginColumnDetail
    {
        private final QualifiedObjectName tableName;
        private final String columnName;

        public OriginColumnDetail(QualifiedObjectName tableName, String columnName)
        {
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.columnName = requireNonNull(columnName, "columnName is null");
        }

        public QualifiedObjectName getTableName()
        {
            return tableName;
        }

        public String getColumnName()
        {
            return columnName;
        }
    }
}
