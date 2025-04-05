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
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.Attributes.AggregationStep;
import io.trino.sql.dialect.trino.Attributes.SortOrderList;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.type.FunctionType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Attributes.AGGREGATION_STEP;
import static io.trino.sql.dialect.trino.Attributes.AggregationStep.FINAL;
import static io.trino.sql.dialect.trino.Attributes.AggregationStep.PARTIAL;
import static io.trino.sql.dialect.trino.Attributes.AggregationStep.SINGLE;
import static io.trino.sql.dialect.trino.Attributes.DISTINCT;
import static io.trino.sql.dialect.trino.Attributes.RESOLVED_FUNCTION;
import static io.trino.sql.dialect.trino.Attributes.SORT_ORDERS;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AggregateCall
        extends TrinoOperation
{
    private static final String NAME = "aggregate_call";

    private final Result result;
    private final Value group;
    private final Region arguments;
    private final Region filterSelector;
    private final Region maskSelector;
    private final Region orderingSelector;
    private final Map<AttributeKey, Object> attributes;

    public AggregateCall(
            String resultName,
            Value group,
            // AggregateCall should derive its own output type based on the function and step.
            // For SINGLE and FINAL step, the type can be derived from the ResolvedFunction.
            // For PARTIAL and INTERMEDIATE step, the type can be obtained through a metadata call: AggregationFunctionMetadata.getIntermediateTypes().
            // For now, we are passing the type at construction.
            // TODO derive output type instead of passing
            Type outputType,
            Block arguments,
            Block filterSelector,
            Block maskSelector,
            Block orderingSelector,
            Optional<SortOrderList> sortOrders,
            ResolvedFunction function,
            boolean distinct,
            AggregationStep step) // needed to verify argument count and validate output type
    // TODO pass input attributes
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(group, "group is null");
        requireNonNull(outputType, "outputType is null");
        requireNonNull(arguments, "arguments is null");
        requireNonNull(filterSelector, "filterSelector is null");
        requireNonNull(maskSelector, "maskSelector is null");
        requireNonNull(orderingSelector, "orderingSelector is null");
        requireNonNull(sortOrders, "sortOrders is null");
        requireNonNull(function, "function is null");
        requireNonNull(step, "step is null");

        if (!IS_RELATION.test(trinoType(group.type()))) {
            throw new TrinoException(IR_ERROR, "group input of AggregateCall operation must be of relation type");
        }
        this.group = group;

        if (arguments.parameters().size() != 1 ||
                !trinoType(arguments.parameters().getFirst().type()).equals(relationRowType(trinoType(group.type()))) ||
                !(trinoType(arguments.getReturnedType()) instanceof RowType || trinoType(arguments.getReturnedType()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, "invalid arguments for AggregateCall operation");
        }

        // verify argument count considering step
        List<Type> argumentTypes = trinoType(arguments.getReturnedType()).getTypeParameters();
        if (step == SINGLE || step == PARTIAL) {
            if (function.signature().getArgumentTypes().size() != argumentTypes.size()) {
                throw new TrinoException(IR_ERROR, format("expected %s arguments for aggregation step %s, found: %s", function.signature().getArgumentTypes().size(), step, argumentTypes.size()));
            }
            for (int i = 0; i < argumentTypes.size(); i++) {
                Type expectedType = function.signature().getArgumentType(i);
                Type actualType = argumentTypes.get(i);
                if (!expectedType.equals(actualType)) {
                    throw new TrinoException(IR_ERROR, format("invalid argument type. expected: %s, actual: %s", expectedType, actualType));
                }
            }
        }
        else {
            // intermediate and final steps get the intermediate value and the lambda functions
            int expectedArgumentCount = 1 + (int) function.signature().getArgumentTypes().stream()
                    .filter(FunctionType.class::isInstance)
                    .count();
            if (expectedArgumentCount != argumentTypes.size()) {
                throw new TrinoException(IR_ERROR, format("expected %s arguments for aggregation step %s, found: %s", expectedArgumentCount, step, argumentTypes.size()));
            }
        }

        // TODO verify that arguments are all FieldSelection or Lambda operations -- we must pass the current Program (or its value map) to get values sources
        /*if (trinoType(arguments.getReturnedType()) instanceof RowType rowType) {
            Row argumentsRow = (Row) getOnlyElement(arguments.getTerminalOperation().arguments()).source(program);
            // recreating the check from AggregationNode.Aggregation
            // each argument must be either
            //  - reference to input row field
            //  - Lambda operation
            for (Value argument : argumentsRow.arguments()) {
                SourceNode sourceNode = argument.source(program);
                boolean validArgument = sourceNode instanceof Lambda ||
                        sourceNode instanceof FieldSelection fieldSelection && getOnlyElement(fieldSelection.arguments()).source(program).equals(arguments);
                if (!validArgument) {
                    throw new TrinoException(IR_ERROR, "invalid argument to AggregateCall operation. Expected lambda or input field reference");
                }
            }
        }*/
        this.arguments = singleBlockRegion(arguments);

        if ((step == SINGLE || step == FINAL) && !outputType.equals(function.signature().getReturnType())) {
            throw new TrinoException(IR_ERROR, "invalid output type for AggregateCall operation");
        }
        this.result = new Result(resultName, irType(outputType));

        if (filterSelector.parameters().size() != 1 ||
                !trinoType(filterSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(group.type()))) ||
                !(trinoType(filterSelector.getReturnedType()) instanceof RowType || trinoType(filterSelector.getReturnedType()).equals(EMPTY_ROW)) ||
                trinoType(filterSelector.getReturnedType()).getTypeParameters().size() > 1) {
            throw new TrinoException(IR_ERROR, "invalid filter selector for AggregateCall operation");
        }
        this.filterSelector = singleBlockRegion(filterSelector);

        if (maskSelector.parameters().size() != 1 ||
                !trinoType(maskSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(group.type()))) ||
                !(trinoType(maskSelector.getReturnedType()) instanceof RowType || trinoType(maskSelector.getReturnedType()).equals(EMPTY_ROW)) ||
                trinoType(maskSelector.getReturnedType()).getTypeParameters().size() > 1) {
            throw new TrinoException(IR_ERROR, "invalid mask selector for AggregateCall operation");
        }
        this.maskSelector = singleBlockRegion(maskSelector);

        if (orderingSelector.parameters().size() != 1 ||
                !trinoType(orderingSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(group.type()))) ||
                !(trinoType(orderingSelector.getReturnedType()) instanceof RowType || trinoType(orderingSelector.getReturnedType()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, "invalid ordering selector for AggregateCall operation");
        }
        if (!(trinoType(orderingSelector.getReturnedType()).getTypeParameters().isEmpty() || step == SINGLE)) {
            throw new TrinoException(IR_ERROR, "ORDER BY is not supported for distributed aggregation");
        }
        this.orderingSelector = singleBlockRegion(orderingSelector);

        if (trinoType(orderingSelector.getReturnedType()).getTypeParameters().size() != sortOrders.map(orders -> orders.sortOrders().size()).orElse(0)) {
            throw new TrinoException(IR_ERROR, "ordering fields and sort orders for AggregateCall do not match in size");
        }

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        sortOrders.ifPresent(orders -> SORT_ORDERS.putAttribute(attributes, orders));
        RESOLVED_FUNCTION.putAttribute(attributes, function);
        DISTINCT.putAttribute(attributes, distinct);

        // TODO: derive attributes from input attributes; derive attributes from ResolvedFunction
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
        return ImmutableList.of(group);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(arguments, filterSelector, maskSelector, orderingSelector);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return "pretty aggregate call";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new AggregateCall(
                result.name(),
                newArgument,
                trinoType(result.type()),
                arguments.getOnlyBlock(),
                filterSelector.getOnlyBlock(),
                maskSelector.getOnlyBlock(),
                orderingSelector.getOnlyBlock(),
                Optional.ofNullable(SORT_ORDERS.getAttribute(attributes)),
                RESOLVED_FUNCTION.getAttribute(attributes),
                DISTINCT.getAttribute(attributes),
                AGGREGATION_STEP.getAttribute(attributes));
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
        var that = (AggregateCall) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.group, that.group) &&
                Objects.equals(this.arguments, that.arguments) &&
                Objects.equals(this.filterSelector, that.filterSelector) &&
                Objects.equals(this.maskSelector, that.maskSelector) &&
                Objects.equals(this.orderingSelector, that.orderingSelector) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, group, arguments, filterSelector, maskSelector, orderingSelector, attributes);
    }
}
