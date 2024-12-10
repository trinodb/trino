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
import io.trino.sql.dialect.trino.operation.Bind;
import io.trino.sql.dialect.trino.operation.Call;
import io.trino.sql.dialect.trino.operation.Case;
import io.trino.sql.dialect.trino.operation.Cast;
import io.trino.sql.dialect.trino.operation.Coalesce;
import io.trino.sql.dialect.trino.operation.Comparison;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.FieldSelection;
import io.trino.sql.dialect.trino.operation.In;
import io.trino.sql.dialect.trino.operation.IsNull;
import io.trino.sql.dialect.trino.operation.Lambda;
import io.trino.sql.dialect.trino.operation.Logical;
import io.trino.sql.dialect.trino.operation.NullIf;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.Switch;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.SourceNode;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.Symbol;
import org.assertj.core.util.VisibleForTesting;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Context.argumentMapping;
import static io.trino.sql.dialect.trino.Context.composedMapping;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
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
                elements.stream()
                        .map(Operation::result)
                        .collect(toImmutableList()),
                elements.stream()
                        .map(Operation::attributes)
                        .collect(toImmutableList()));
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
    protected Operation visitBind(io.trino.sql.ir.Bind node, Context context)
    {
        ImmutableList.Builder<Operation> argumentsBuilder = ImmutableList.builder();
        for (Expression expression : node.values()) {
            argumentsBuilder.add(expression.accept(this, context));
        }
        List<Operation> arguments = argumentsBuilder.build();
        Operation lambda = node.function().accept(this, context);

        ImmutableList.Builder<Map<AttributeKey, Object>> sourceAttributes = ImmutableList.builder();
        arguments.stream()
                .map(Operation::attributes)
                .forEach(sourceAttributes::add);
        sourceAttributes.add(lambda.attributes());

        String resultName = nameAllocator.newName();
        Bind bind = new Bind(
                resultName,
                arguments.stream()
                        .map(Operation::result)
                        .collect(toImmutableList()),
                lambda.result(),
                sourceAttributes.build());
        valueMap.put(bind.result(), bind);
        context.block().addOperation(bind);
        return bind;
    }

    @Override
    protected Operation visitCall(io.trino.sql.ir.Call node, Context context)
    {
        ImmutableList.Builder<Operation> argumentsBuilder = ImmutableList.builder();
        for (Expression argument : node.arguments()) {
            argumentsBuilder.add(argument.accept(this, context));
        }
        List<Operation> arguments = argumentsBuilder.build();

        String resultName = nameAllocator.newName();
        Call call = new Call(
                resultName,
                arguments.stream()
                        .map(Operation::result)
                        .collect(toImmutableList()),
                node.function(),
                arguments.stream()
                        .map(Operation::attributes)
                        .collect(toImmutableList()));
        valueMap.put(call.result(), call);
        context.block().addOperation(call);
        return call;
    }

    @Override
    protected Operation visitCase(io.trino.sql.ir.Case node, Context context)
    {
        List<Operation> operands = node.whenClauses().stream()
                .map(WhenClause::getOperand)
                .map(operand -> operand.accept(this, context))
                .collect(toImmutableList());
        List<Operation> results = node.whenClauses().stream()
                .map(WhenClause::getResult)
                .map(result -> result.accept(this, context))
                .collect(toImmutableList());
        Operation defaultValue = node.defaultValue().accept(this, context);

        ImmutableList.Builder<Map<AttributeKey, Object>> sourceAttributes = ImmutableList.builder();
        operands.stream()
                .map(Operation::attributes)
                .forEach(sourceAttributes::add);
        results.stream()
                .map(Operation::attributes)
                .forEach(sourceAttributes::add);
        sourceAttributes.add(defaultValue.attributes());

        String resultName = nameAllocator.newName();
        Case caseOperation = new Case(
                resultName,
                operands.stream()
                        .map(Operation::result)
                        .collect(toImmutableList()),
                results.stream()
                        .map(Operation::result)
                        .collect(toImmutableList()),
                defaultValue.result(),
                sourceAttributes.build());
        valueMap.put(caseOperation.result(), caseOperation);
        context.block().addOperation(caseOperation);
        return caseOperation;
    }

    @Override
    protected Operation visitCast(io.trino.sql.ir.Cast node, Context context)
    {
        Operation argument = node.expression().accept(this, context);
        String resultName = nameAllocator.newName();
        Cast cast = new Cast(resultName, argument.result(), node.type(), argument.attributes());
        valueMap.put(cast.result(), cast);
        context.block().addOperation(cast);
        return cast;
    }

    @Override
    protected Operation visitCoalesce(io.trino.sql.ir.Coalesce node, Context context)
    {
        ImmutableList.Builder<Operation> operandsBuilder = ImmutableList.builder();
        for (Expression element : node.operands()) {
            operandsBuilder.add(element.accept(this, context));
        }
        List<Operation> operands = operandsBuilder.build();

        String resultName = nameAllocator.newName();
        Coalesce coalesce = new Coalesce(
                resultName,
                operands.stream()
                        .map(Operation::result)
                        .collect(toImmutableList()),
                operands.stream()
                        .map(Operation::attributes)
                        .collect(toImmutableList()));
        valueMap.put(coalesce.result(), coalesce);
        context.block().addOperation(coalesce);
        return coalesce;
    }

    @Override
    protected Operation visitComparison(io.trino.sql.ir.Comparison node, Context context)
    {
        Operation left = node.left().accept(this, context);
        Operation right = node.right().accept(this, context);

        String resultName = nameAllocator.newName();
        Comparison comparison = new Comparison(
                resultName,
                left.result(),
                right.result(),
                Attributes.ComparisonOperator.of(node.operator()),
                ImmutableList.of(left.attributes(), right.attributes()));
        valueMap.put(comparison.result(), comparison);
        context.block().addOperation(comparison);
        return comparison;
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
    protected Operation visitFieldReference(io.trino.sql.ir.FieldReference node, Context context)
    {
        Operation base = node.base().accept(this, context);

        String resultName = nameAllocator.newName();
        FieldReference fieldReference = new FieldReference(
                resultName,
                base.result(),
                node.field(),
                base.attributes());
        valueMap.put(fieldReference.result(), fieldReference);
        context.block().addOperation(fieldReference);
        return fieldReference;
    }

    @Override
    protected Operation visitIn(io.trino.sql.ir.In node, Context context)
    {
        Operation value = node.value().accept(this, context);
        List<Operation> valueList = node.valueList().stream()
                .map(element -> element.accept(this, context))
                .collect(toImmutableList());

        ImmutableList.Builder<Map<AttributeKey, Object>> sourceAttributes = ImmutableList.builder();
        sourceAttributes.add(value.attributes());
        valueList.stream()
                .map(Operation::attributes)
                .forEach(sourceAttributes::add);

        String resultName = nameAllocator.newName();
        In in = new In(
                resultName,
                value.result(),
                valueList.stream()
                        .map(Operation::result)
                        .collect(toImmutableList()),
                sourceAttributes.build());
        valueMap.put(in.result(), in);
        context.block().addOperation(in);
        return in;
    }

    @Override
    protected Operation visitIsNull(io.trino.sql.ir.IsNull node, Context context)
    {
        Operation input = node.value().accept(this, context);

        String resultName = nameAllocator.newName();
        IsNull isNull = new IsNull(
                resultName,
                input.result(),
                input.attributes());
        valueMap.put(isNull.result(), isNull);
        context.block().addOperation(isNull);
        return isNull;
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
            // using a HashMap to handle duplicates.
            // it should be illegal to have duplicate lambda arguments but seems allowed.
            lambdaMapping = newHashMap(node.arguments().size());
            for (int i = 0; i < node.arguments().size(); i++) {
                Symbol argument = node.arguments().get(i);
                String fieldName = format("a_%s", i + 1);
                fields.add(new RowType.Field(Optional.of(fieldName), argument.type()));
                lambdaMapping.putIfAbsent(argument, fieldName);
            }
            lambdaParameterType = RowType.from(fields.build());
        }

        Block.Parameter lambdaParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(lambdaParameterType));

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
        ImmutableList.Builder<Operation> termsBuilder = ImmutableList.builder();
        for (Expression term : node.terms()) {
            termsBuilder.add(term.accept(this, context));
        }
        List<Operation> terms = termsBuilder.build();

        String resultName = nameAllocator.newName();
        Logical logical = new Logical(
                resultName,
                terms.stream()
                        .map(Operation::result)
                        .collect(toImmutableList()),
                Attributes.LogicalOperator.of(node.operator()),
                terms.stream()
                        .map(Operation::attributes)
                        .collect(toImmutableList()));
        valueMap.put(logical.result(), logical);
        context.block().addOperation(logical);
        return logical;
    }

    @Override
    protected Operation visitNullIf(io.trino.sql.ir.NullIf node, Context context)
    {
        Operation first = node.first().accept(this, context);
        Operation second = node.second().accept(this, context);

        String resultName = nameAllocator.newName();
        NullIf nullIf = new NullIf(
                resultName,
                first.result(),
                second.result(),
                ImmutableList.of(first.attributes(), second.attributes()));
        valueMap.put(nullIf.result(), nullIf);
        context.block().addOperation(nullIf);
        return nullIf;
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
        ImmutableList.Builder<Operation> itemsBuilder = ImmutableList.builder();
        for (Expression item : node.items()) {
            itemsBuilder.add(item.accept(this, context));
        }
        List<Operation> items = itemsBuilder.build();

        String resultName = nameAllocator.newName();
        Row row = new Row(
                resultName,
                items.stream()
                        .map(Operation::result)
                        .collect(toImmutableList()),
                items.stream()
                        .map(Operation::attributes)
                        .collect(toImmutableList()));
        valueMap.put(row.result(), row);
        context.block().addOperation(row);
        return row;
    }

    @Override
    protected Operation visitSwitch(io.trino.sql.ir.Switch node, Context context)
    {
        Operation operand = node.operand().accept(this, context);
        List<Operation> when = node.whenClauses().stream()
                .map(WhenClause::getOperand)
                .map(expression -> expression.accept(this, context))
                .collect(toImmutableList());
        List<Operation> then = node.whenClauses().stream()
                .map(WhenClause::getResult)
                .map(expression -> expression.accept(this, context))
                .collect(toImmutableList());
        Operation defaultValue = node.defaultValue().accept(this, context);

        ImmutableList.Builder<Map<AttributeKey, Object>> sourceAttributes = ImmutableList.builder();
        sourceAttributes.add(operand.attributes());
        when.stream()
                .map(Operation::attributes)
                .forEach(sourceAttributes::add);
        then.stream()
                .map(Operation::attributes)
                .forEach(sourceAttributes::add);
        sourceAttributes.add(defaultValue.attributes());

        String resultName = nameAllocator.newName();
        Switch switchOperation = new Switch(
                resultName,
                operand.result(),
                when.stream()
                        .map(Operation::result)
                        .collect(toImmutableList()),
                then.stream()
                        .map(Operation::result)
                        .collect(toImmutableList()),
                defaultValue.result(),
                sourceAttributes.build());
        valueMap.put(switchOperation.result(), switchOperation);
        context.block().addOperation(switchOperation);
        return switchOperation;
    }

    /**
     * Return the value of the recent operation in the builder
     */
    @VisibleForTesting
    public void addReturnOperation(Block.Builder builder)
    {
        String returnValue = nameAllocator.newName();
        Operation recentOperation = builder.recentOperation();
        Return returnOperation = new Return(returnValue, recentOperation.result(), recentOperation.attributes());
        valueMap.put(returnOperation.result(), returnOperation);
        builder.addOperation(returnOperation);
    }
}
