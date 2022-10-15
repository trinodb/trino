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
package io.trino.plugin.pinot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PinotColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type dataType;
    private final String expression;
    private final boolean aliased;
    private final boolean aggregate;
    private final boolean returnNullOnEmptyGroup;
    private final Optional<String> pushedDownAggregateFunctionName;
    private final Optional<String> pushedDownAggregateFunctionArgument;

    public PinotColumnHandle(String columnName, Type dataType)
    {
        this(columnName, dataType, columnName, false, false, true, Optional.empty(), Optional.empty());
    }

    @JsonCreator
    public PinotColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("dataType") Type dataType,
            @JsonProperty("expression") String expression,
            @JsonProperty("aliased") boolean aliased,
            @JsonProperty("aggregate") boolean aggregate,
            @JsonProperty("returnNullOnEmptyGroup") boolean returnNullOnEmptyGroup,
            @JsonProperty("pushedDownAggregateFunctionName") Optional<String> pushedDownAggregateFunctionName,
            @JsonProperty("pushedDownAggregateFunctionArgument") Optional<String> pushedDownAggregateFunctionArgument)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.dataType = requireNonNull(dataType, "dataType is null");
        this.expression = requireNonNull(expression, "expression is null");
        this.aliased = aliased;
        this.aggregate = aggregate;
        this.returnNullOnEmptyGroup = returnNullOnEmptyGroup;
        requireNonNull(pushedDownAggregateFunctionName, "pushedDownAggregateFunctionName is null");
        requireNonNull(pushedDownAggregateFunctionArgument, "pushedDownAggregateFunctionArgument is null");
        checkState(pushedDownAggregateFunctionName.isPresent() == pushedDownAggregateFunctionArgument.isPresent(), "Unexpected arguments: Either pushedDownAggregateFunctionName and pushedDownAggregateFunctionArgument must both be present or both be empty.");
        checkState((pushedDownAggregateFunctionName.isPresent() && aggregate) || pushedDownAggregateFunctionName.isEmpty(), "Unexpected arguments: aggregate is false but pushed down aggregation is present");
        this.pushedDownAggregateFunctionName = pushedDownAggregateFunctionName;
        this.pushedDownAggregateFunctionArgument = pushedDownAggregateFunctionArgument;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getDataType()
    {
        return dataType;
    }

    @JsonProperty
    public String getExpression()
    {
        return expression;
    }

    // Keep track of whether this column is aliased, it will determine how the pinot sql query is built
    // The reason is that pinot parses the broker request into pinot pql but expects pinot sql.
    // In some cases the parsed pql expression is an invalid sql expression.
    @JsonProperty
    public boolean isAliased()
    {
        return aliased;
    }

    // True if this is an aggregate column for both passthrough query and pushed down aggregate expressions.
    @JsonProperty
    public boolean isAggregate()
    {
        return aggregate;
    }

    // Some aggregations should return null on empty group, ex. min/max
    // If false then return the value from Pinot, ex. count(*)
    @JsonProperty
    public boolean isReturnNullOnEmptyGroup()
    {
        return returnNullOnEmptyGroup;
    }

    // If the aggregate expression is pushed down store the function name
    // If the argument is an alias the pinot expression will use the original
    // column name in the expression and alias it.
    //
    // Example: SELECT MAX(bar) FROM "SELECT foo AS bar FROM table"
    // Will translate to the pinot query "SELECT MAX(foo) AS \"max(bar)\""
    //
    // Note: Pinot omits quotes on the autogenerated column name "max(bar)"
    @JsonProperty
    public Optional<String> getPushedDownAggregateFunctionName()
    {
        return pushedDownAggregateFunctionName;
    }

    // See comment for getPushedDownAggregateFunctionName()
    @JsonProperty
    public Optional<String> getPushedDownAggregateFunctionArgument()
    {
        return pushedDownAggregateFunctionArgument;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(dataType)
                .build();
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

        PinotColumnHandle that = (PinotColumnHandle) o;
        return Objects.equals(getColumnName(), that.getColumnName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("dataType", dataType)
                .add("expression", expression)
                .add("aliased", aliased)
                .add("aggregate", aggregate)
                .add("returnNullOnEmptyGroup", returnNullOnEmptyGroup)
                .add("pushedDownAggregateFunctionName", pushedDownAggregateFunctionName)
                .add("pushedDownAggregateFunctionArgument", pushedDownAggregateFunctionArgument)
                .toString();
    }
}
