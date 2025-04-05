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
package io.trino.sql.dialect.trino.operation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.type.RowType;
import io.trino.sql.dialect.trino.Attributes.SortOrderList;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Attributes.LIMIT;
import static io.trino.sql.dialect.trino.Attributes.PARTIAL;
import static io.trino.sql.dialect.trino.Attributes.PRE_SORTED_INDEXES;
import static io.trino.sql.dialect.trino.Attributes.SORT_ORDERS;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public class Limit
        extends TrinoOperation
{
    private static final String NAME = "limit";

    private final Result result;
    private final Value input;
    private final Region orderingSelector;
    private final Map<Operation.AttributeKey, Object> attributes;

    public Limit(
            String resultName,
            Value input,
            Block orderingSelector,
            Optional<SortOrderList> sortOrders,
            long count,
            boolean partial,
            List<Integer> preSortedIndexes, // indexes in orderingSelector
            Map<Operation.AttributeKey, Object> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(orderingSelector, "orderingSelector is null");
        requireNonNull(sortOrders, "sortOrders is null");
        requireNonNull(preSortedIndexes, "preSortedIndexes is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        if (!IS_RELATION.test(trinoType(input.type()))) {
            throw new TrinoException(IR_ERROR, "input to the limit operation must be of relation type");
        }
        this.input = input;

        this.result = new Result(resultName, input.type());

        if (orderingSelector.parameters().size() != 1 ||
                !trinoType(orderingSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(input.type()))) ||
                !(trinoType(orderingSelector.getReturnedType()) instanceof RowType || trinoType(orderingSelector.getReturnedType()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, "invalid ordering selector for limit operation");
        }
        this.orderingSelector = singleBlockRegion(orderingSelector);

        if (trinoType(orderingSelector.getReturnedType()).getTypeParameters().size() != sortOrders.map(orders -> orders.sortOrders().size()).orElse(0)) {
            throw new TrinoException(IR_ERROR, "ordering fields and sort orders for limit do not match in size");
        }

        int orderingSize = trinoType(orderingSelector.getReturnedType()).getTypeParameters().size();
        preSortedIndexes.stream()
                .forEach(index -> {
                    if (index < 0 || index >= orderingSize) {
                        throw new TrinoException(IR_ERROR, "invalid pre-sorted field for limit operation");
                    }
                });

        if (count < 0) {
            throw new TrinoException(IR_ERROR, "invalid count for limit operation");
        }

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        sortOrders.ifPresent(orders -> SORT_ORDERS.putAttribute(attributes, orders));
        LIMIT.putAttribute(attributes, count);
        PARTIAL.putAttribute(attributes, partial);
        PRE_SORTED_INDEXES.putAttribute(attributes, preSortedIndexes);

        // TODO derive attributes from source attributes
        this.attributes = attributes.buildOrThrow();
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return ImmutableList.of(input);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(orderingSelector);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return "pretty limit";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new Limit(
                result.name(),
                newArgument,
                orderingSelector.getOnlyBlock(),
                Optional.ofNullable(SORT_ORDERS.getAttribute(attributes)),
                LIMIT.getAttribute(attributes),
                PARTIAL.getAttribute(attributes),
                PRE_SORTED_INDEXES.getAttribute(attributes),
                ImmutableMap.of());
    }

    public boolean isWithTies()
    {
        return SORT_ORDERS.getAttribute(attributes()) != null;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (Limit) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.input, that.input) &&
                Objects.equals(this.orderingSelector, that.orderingSelector) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, input, orderingSelector, attributes);
    }
}
