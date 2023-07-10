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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class NestedColumns
        extends JsonTableColumnDefinition
{
    private final StringLiteral jsonPath;
    private final Optional<Identifier> pathName;
    private final List<JsonTableColumnDefinition> columns;

    public NestedColumns(NodeLocation location, StringLiteral jsonPath, Optional<Identifier> pathName, List<JsonTableColumnDefinition> columns)
    {
        super(location);
        this.jsonPath = requireNonNull(jsonPath, "jsonPath is null");
        this.pathName = requireNonNull(pathName, "pathName is null");
        this.columns = ImmutableList.copyOf(columns);
        checkArgument(!columns.isEmpty(), "columns is empty");
    }

    public StringLiteral getJsonPath()
    {
        return jsonPath;
    }

    public Optional<Identifier> getPathName()
    {
        return pathName;
    }

    public List<JsonTableColumnDefinition> getColumns()
    {
        return columns;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitNestedColumns(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(jsonPath)
                .addAll(columns)
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("jsonPath", jsonPath)
                .add("pathName", pathName.orElse(null))
                .add("columns", columns)
                .omitNullValues()
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

        NestedColumns that = (NestedColumns) o;
        return Objects.equals(jsonPath, that.jsonPath) &&
                Objects.equals(pathName, that.pathName) &&
                Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(jsonPath, pathName, columns);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return pathName.equals(((NestedColumns) other).pathName);
    }
}
