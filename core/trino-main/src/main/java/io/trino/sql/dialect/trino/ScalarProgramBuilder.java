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
package io.trino.sql.dialect.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.ProgramBuilder.ValueNameAllocator;
import io.trino.sql.dialect.trino.operation.Array;
import io.trino.sql.dialect.trino.operation.Between;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldSelection;
import io.trino.sql.dialect.trino.operation.Lambda;
import io.trino.sql.dialect.trino.operation.Logical;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.Reference;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.SourceNode;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Context.argumentMapping;
import static io.trino.sql.dialect.trino.Context.composedMapping;
import static java.lang.String.format;
import static java.util.HashMap.newHashMap;
import static java.util.Objects.requireNonNull;

/**
 * A rewriter transforming a tree of scalar Expressions into a MLIR program based on `IrVisitor`.
 * It is called from `RelationalProgramBuilder` to transform predicates etc.
 * Note: we don't need to pass a `RelationalProgramBuilder` for recursive calls because the
 * IR Expressions don't have nested relations.
 */
public class ScalarProgramBuilder
        extends IrVisitor<Operation, Context>
{
    private final ValueNameAllocator nameAllocator;
    private final ImmutableMap.Builder<Value, SourceNode> valueMap;

    public ScalarProgramBuilder(ValueNameAllocator nameAllocator, ImmutableMap.Builder<Value, SourceNode> valueMap)
    {
        this.nameAllocator = requireNonNull(nameAllocator, "nameAllocator is null");
        this.valueMap = requireNonNull(valueMap, "valueMap is null");
    }

    @Override
    protected Operation visitExpression(Expression node, Context context)
    {
        throw new UnsupportedOperationException("The new IR does not support " + node.getClass().getSimpleName() + " yet");
    }

    @Override
    protected Operation visitArray(io.trino.sql.ir.Array node, Context context)
    {
        // lowering alert! Unrolls the array constructor into elements and a final Array operation.
        ImmutableList.Builder<Operation> elementsBuilder = ImmutableList.builder();
        for (Expression element : node.elements()) {
            elementsBuilder.add(element.accept(this, context));
        }
        List<Operation> elements = elementsBuilder.build();

        String resultName = nameAllocator.newName();
        Array array = new Array(
                resultName,
                node.elementType(),
                elements.stream().map(Operation::result).collect(toImmutableList()),
                elements.stream().map(Operation::attributes).collect(toImmutableList()));
        valueMap.put(array.result(), array);
        context.block().addOperation(array);
        return array;
    }

    @Override
    protected Operation visitBetween(io.trino.sql.ir.Between node, Context context)
    {
        Operation input = node.value().accept(this, context);
        Operation min = node.min().accept(this, context);
        Operation max = node.max().accept(this, context);

        String resultName = nameAllocator.newName();
        Between between = new Between(
                resultName,
                input.result(),
                min.result(),
                max.result(),
                ImmutableList.of(input.attributes(), min.attributes(), max.attributes()));
        valueMap.put(between.result(), between);
        context.block().addOperation(between);
        return between;
    }

    @Override
    protected Operation visitConstant(io.trino.sql.ir.Constant node, Context context)
    {
        String resultName = nameAllocator.newName();
        Constant constant = new Constant(resultName, node.type(), node.value());
        valueMap.put(constant.result(), constant);
        context.block().addOperation(constant);
        return constant;
    }

    @Override
    protected Operation visitLambda(io.trino.sql.ir.Lambda node, Context context)
    {
        String resultName = nameAllocator.newName();

        // model lambda logic as a Block. collect all lambda arguments in a row
        // and pass them as a single parameter to the Block.
        Type lambdaParameterType;
        Map<Symbol, String> lambdaMapping;

        if (node.arguments().isEmpty()) {
            lambdaParameterType = EMPTY_ROW;
            lambdaMapping = ImmutableMap.of();
        }
        else {
            ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
            // using a HashMap to handle duplicates. one symbol can be passed multiple times to a lambda.
            lambdaMapping = newHashMap(node.arguments().size());
            int fieldIndex = 1;
            for (Symbol argument : node.arguments()) {
                if (!lambdaMapping.containsKey(argument)) {
                    String fieldName = format("a_%s", fieldIndex++);
                    fields.add(new RowType.Field(Optional.of(fieldName), argument.type()));
                    lambdaMapping.put(argument, fieldName);
                }
            }
            lambdaParameterType = RowType.from(fields.build());
        }

        Block.Parameter lambdaParameter = new Block.Parameter(
                nameAllocator.newName(),
                lambdaParameterType);

        Block.Builder lambdaBuilder = new Block.Builder(Optional.of("^lambda"), ImmutableList.of(lambdaParameter));
        node.body().accept(
                this,
                new Context(lambdaBuilder, composedMapping(context, argumentMapping(lambdaParameter, lambdaMapping))));

        addReturnOperation(lambdaBuilder);

        Block lambdaBody = lambdaBuilder.build();
        valueMap.put(lambdaParameter, lambdaBody);

        Lambda lambda = new Lambda(resultName, lambdaBody);
        valueMap.put(lambda.result(), lambda);
        context.block().addOperation(lambda);
        return lambda;
    }

    @Override
    protected Operation visitLogical(io.trino.sql.ir.Logical node, Context context)
    {
        // lowering alert! Unrolls the logical expression into terms and a final Logical operation constructor.
        ImmutableList.Builder<Operation> termsBuilder = ImmutableList.builder();
        for (Expression term : node.terms()) {
            termsBuilder.add(term.accept(this, context));
        }
        List<Operation> terms = termsBuilder.build();

        String resultName = nameAllocator.newName();
        Logical logical = new Logical(
                resultName,
                terms.stream().map(Operation::result).collect(toImmutableList()),
                Attributes.LogicalOperator.of(node.operator()),
                terms.stream().map(Operation::attributes).collect(toImmutableList()));
        valueMap.put(logical.result(), logical);
        context.block().addOperation(logical);
        return logical;
    }

    @Override
    protected Operation visitReference(Reference node, Context context)
    {
        Context.RowField rowField = context.symbolMapping().get(new Symbol(node.type(), node.name()));
        if (rowField == null) {
            throw new TrinoException(IR_ERROR, "no mapping for symbol " + node.name());
        }
        String resultName = nameAllocator.newName();
        FieldSelection fieldSelection = new FieldSelection(resultName, rowField.row(), rowField.field(), ImmutableMap.of()); // TODO pass attributes through correlation / block argument
        valueMap.put(fieldSelection.result(), fieldSelection);
        context.block().addOperation(fieldSelection);
        return fieldSelection;
    }

    @Override
    protected Operation visitRow(io.trino.sql.ir.Row node, Context context)
    {
        // lowering alert! Unrolls the row into field assignments and a final row constructor.
        ImmutableList.Builder<Operation> itemsBuilder = ImmutableList.builder();
        for (Expression item : node.items()) {
            itemsBuilder.add(item.accept(this, context));
        }
        List<Operation> items = itemsBuilder.build();

        String resultName = nameAllocator.newName();
        Row row = new Row(
                resultName,
                items.stream().map(Operation::result).collect(toImmutableList()),
                items.stream().map(Operation::attributes).collect(toImmutableList()));
        valueMap.put(row.result(), row);
        context.block().addOperation(row);
        return row;
    }

    /**
     * Return the value of the recent operation in the builder
     */
    private void addReturnOperation(Block.Builder builder)
    {
        String returnValue = nameAllocator.newName();
        Operation recentOperation = builder.recentOperation();
        Return returnOperation = new Return(returnValue, recentOperation.result(), recentOperation.attributes());
        valueMap.put(returnOperation.result(), returnOperation);
        builder.addOperation(returnOperation);
    }
}
