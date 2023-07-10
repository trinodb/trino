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
import io.trino.sql.tree.JsonPathParameter.JsonFormat;
import io.trino.sql.tree.JsonQuery.ArrayWrapperBehavior;
import io.trino.sql.tree.JsonQuery.EmptyOrErrorBehavior;
import io.trino.sql.tree.JsonQuery.QuotesBehavior;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class QueryColumn
        extends JsonTableColumnDefinition
{
    private final Identifier name;
    private final DataType type;
    private final JsonFormat format;
    private final Optional<StringLiteral> jsonPath;
    private final ArrayWrapperBehavior wrapperBehavior;
    private final Optional<QuotesBehavior> quotesBehavior;
    private final EmptyOrErrorBehavior emptyBehavior;
    private final Optional<EmptyOrErrorBehavior> errorBehavior;

    public QueryColumn(
            NodeLocation location,
            Identifier name,
            DataType type,
            JsonFormat format,
            Optional<StringLiteral> jsonPath,
            ArrayWrapperBehavior wrapperBehavior,
            Optional<QuotesBehavior> quotesBehavior,
            EmptyOrErrorBehavior emptyBehavior,
            Optional<EmptyOrErrorBehavior> errorBehavior)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.format = requireNonNull(format, "format is null");
        this.jsonPath = requireNonNull(jsonPath, "jsonPath is null");
        this.wrapperBehavior = requireNonNull(wrapperBehavior, "wrapperBehavior is null");
        this.quotesBehavior = requireNonNull(quotesBehavior, "quotesBehavior is null");
        this.emptyBehavior = requireNonNull(emptyBehavior, "emptyBehavior is null");
        this.errorBehavior = requireNonNull(errorBehavior, "errorBehavior is null");
    }

    public Identifier getName()
    {
        return name;
    }

    public DataType getType()
    {
        return type;
    }

    public JsonFormat getFormat()
    {
        return format;
    }

    public Optional<StringLiteral> getJsonPath()
    {
        return jsonPath;
    }

    public ArrayWrapperBehavior getWrapperBehavior()
    {
        return wrapperBehavior;
    }

    public Optional<QuotesBehavior> getQuotesBehavior()
    {
        return quotesBehavior;
    }

    public EmptyOrErrorBehavior getEmptyBehavior()
    {
        return emptyBehavior;
    }

    public Optional<EmptyOrErrorBehavior> getErrorBehavior()
    {
        return errorBehavior;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitQueryColumn(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return jsonPath.map(ImmutableList::of).orElse(ImmutableList.of());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("format", format)
                .add("jsonPath", jsonPath.orElse(null))
                .add("wrapperBehavior", wrapperBehavior)
                .add("quotesBehavior", quotesBehavior.orElse(null))
                .add("emptyBehavior", emptyBehavior)
                .add("errorBehavior", errorBehavior.orElse(null))
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

        QueryColumn that = (QueryColumn) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(type, that.type) &&
                Objects.equals(format, that.format) &&
                Objects.equals(jsonPath, that.jsonPath) &&
                wrapperBehavior == that.wrapperBehavior &&
                Objects.equals(quotesBehavior, that.quotesBehavior) &&
                emptyBehavior == that.emptyBehavior &&
                Objects.equals(errorBehavior, that.errorBehavior);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, format, jsonPath, wrapperBehavior, quotesBehavior, emptyBehavior, errorBehavior);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        QueryColumn otherQueryColumn = (QueryColumn) other;

        return name.equals(otherQueryColumn.name) &&
                type.equals(otherQueryColumn.type) &&
                format.equals(otherQueryColumn.format) &&
                wrapperBehavior == otherQueryColumn.wrapperBehavior &&
                Objects.equals(quotesBehavior, otherQueryColumn.quotesBehavior) &&
                emptyBehavior == otherQueryColumn.emptyBehavior &&
                Objects.equals(errorBehavior, otherQueryColumn.errorBehavior);
    }
}
