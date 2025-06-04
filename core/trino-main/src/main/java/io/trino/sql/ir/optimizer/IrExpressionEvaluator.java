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
package io.trino.sql.ir.optimizer;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Array;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.type.TypeCoercion;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class IrExpressionEvaluator
{
    private static final MethodHandle LAMBDA_EVALUATOR;

    static {
        try {
            LAMBDA_EVALUATOR = MethodHandles.lookup()
                    .findVirtual(IrExpressionEvaluator.class, "evaluate", methodType(Object.class, Session.class, Expression.class, Map.class, Object[].class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final InterpretedFunctionInvoker functionInvoker;
    private final TypeCoercion typeCoercion;
    private final Metadata metadata;

    public IrExpressionEvaluator(PlannerContext context)
    {
        metadata = context.getMetadata();
        functionInvoker = new InterpretedFunctionInvoker(context.getFunctionManager());
        typeCoercion = new TypeCoercion(context.getTypeManager()::getType);
    }

    public Object evaluate(Expression expression, Session session, Map<String, Object> bindings)
    {
        return switch (expression) {
            case Array e -> evaluateInternal(e, session, bindings);
            case Between e -> evaluateInternal(e, session, bindings);
            case Bind e -> evaluateInternal(e, session, bindings);
            case Call e -> evaluateInternal(e, session, bindings);
            case Case e -> evaluateInternal(e, session, bindings);
            case Cast e -> evaluateInternal(e, session, bindings);
            case Coalesce e -> evaluateInternal(e, session, bindings);
            case Comparison e -> evaluateInternal(e, session, bindings);
            case Constant e -> e.value();
            case FieldReference e -> evaluateInternal(e, session, bindings);
            case In e -> evaluateInternal(e, session, bindings);
            case IsNull e -> evaluateInternal(e, session, bindings);
            case Lambda e -> makeLambdaInvoker(session, e);
            case Logical e -> evaluateInternal(e, session, bindings);
            case NullIf e -> evaluateInternal(e, session, bindings);
            case Reference reference -> bindings.get(reference.name());
            case Row e -> evaluateInternal(e, session, bindings);
            case Switch e -> evaluateInternal(e, session, bindings);
        };
    }

    private Object evaluateInternal(Bind bind, Session session, Map<String, Object> assignments)
    {
        Map<String, Constant> bindings = new HashMap<>();

        for (int i = 0; i < bind.values().size(); i++) {
            Symbol argument = bind.function().arguments().get(i);
            Object value = evaluate(bind.values().get(i), session, assignments);
            bindings.put(argument.name(), new Constant(argument.type(), value));
        }

        return makeLambdaInvoker(
                session,
                new Lambda(
                        bind.function().arguments().subList(bind.values().size(), bind.function().arguments().size()),
                        substituteBindings(bind.function().body(), bindings)));
    }

    private Expression substituteBindings(Expression expression, Map<String, Constant> bindings)
    {
        ExpressionTreeRewriter<Void> rewriter = new ExpressionTreeRewriter<>(
                new ExpressionRewriter<>()
                {
                    @Override
                    public Expression rewriteReference(Reference reference, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                    {
                        Constant constant = bindings.get(reference.name());
                        return constant == null ? reference : constant;
                    }
                });

        return rewriter.rewrite(expression, null);
    }

    private Object evaluateInternal(Switch expression, Session session, Map<String, Object> bindings)
    {
        Expression operand = expression.operand();
        Object value = evaluate(operand, session, bindings);

        if (value == null) {
            return evaluate(expression.defaultValue(), session, bindings);
        }

        ConnectorSession connectorSession = session.toConnectorSession();
        ResolvedFunction equals = metadata.resolveOperator(EQUAL, ImmutableList.of(operand.type(), operand.type()));

        for (WhenClause clause : expression.whenClauses()) {
            Object candidate = evaluate(clause.getOperand(), session, bindings);

            if (Boolean.TRUE.equals(functionInvoker.invoke(equals, connectorSession, Arrays.asList(value, candidate)))) {
                return evaluate(clause.getResult(), session, bindings);
            }
        }

        return evaluate(expression.defaultValue(), session, bindings);
    }

    private Object evaluateInternal(Row expression, Session session, Map<String, Object> bindings)
    {
        return buildRowValue((RowType) expression.type(), builders -> {
            for (int i = 0; i < expression.items().size(); ++i) {
                writeNativeValue(
                        expression.items().get(i).type(), builders.get(i),
                        evaluate(expression.items().get(i), session, bindings));
            }
        });
    }

    private Object evaluateInternal(NullIf expression, Session session, Map<String, Object> bindings)
    {
        ConnectorSession connectorSession = session.toConnectorSession();

        Object first = evaluate(expression.first(), session, bindings);
        Object second = evaluate(expression.second(), session, bindings);

        Type commonType = typeCoercion.getCommonSuperType(expression.first().type(), expression.second().type()).orElseThrow();

        // cast(first as <common type>) == cast(second as <common type>)
        boolean equal = Boolean.TRUE.equals(
                functionInvoker.invoke(
                        metadata.resolveOperator(EQUAL, ImmutableList.of(commonType, commonType)),
                        connectorSession,
                        ImmutableList.of(
                                functionInvoker.invoke(metadata.getCoercion(expression.first().type(), commonType), connectorSession, ImmutableList.of(first)),
                                functionInvoker.invoke(metadata.getCoercion(expression.second().type(), commonType), connectorSession, ImmutableList.of(second)))));

        return equal ? null : first;
    }

    private Object evaluateInternal(Logical expression, Session session, Map<String, Object> bindings)
    {
        Boolean shortCircuit = switch (expression.operator()) {
            case AND -> Boolean.FALSE;
            case OR -> Boolean.TRUE;
        };

        boolean hasNull = false;
        for (Expression term : expression.terms()) {
            Object value = evaluate(term, session, bindings);

            if (shortCircuit.equals(value)) {
                return shortCircuit;
            }

            if (value == null) {
                hasNull = true;
            }
        }

        if (hasNull) {
            return null;
        }

        return !shortCircuit;
    }

    private Object evaluateInternal(IsNull expression, Session session, Map<String, Object> bindings)
    {
        return evaluate(expression.value(), session, bindings) == null;
    }

    private Object evaluateInternal(In expression, Session session, Map<String, Object> bindings)
    {
        Object value = evaluate(expression.value(), session, bindings);

        if (value == null) {
            return null;
        }

        ConnectorSession connectorSession = session.toConnectorSession();
        ResolvedFunction equals = metadata.resolveOperator(EQUAL, ImmutableList.of(expression.value().type(), expression.value().type()));

        boolean hasNull = false;

        List<Object> candidates = expression.valueList().stream()
                .map(item -> evaluate(item, session, bindings))
                .toList();

        for (Object candidate : candidates) {
            Object result = functionInvoker.invoke(equals, connectorSession, Arrays.asList(value, candidate));
            if (Boolean.TRUE.equals(result)) {
                return true;
            }

            if (result == null) {
                hasNull = true;
            }
        }

        return hasNull ? null : false;
    }

    private Object evaluateInternal(FieldReference expression, Session session, Map<String, Object> bindings)
    {
        SqlRow row = (SqlRow) evaluate(expression.base(), session, bindings);
        return readNativeValue(expression.type(), row.getRawFieldBlock(expression.field()), row.getRawIndex());
    }

    private Object evaluateInternal(Comparison expression, Session session, Map<String, Object> bindings)
    {
        Object left = evaluate(expression.left(), session, bindings);
        Type leftType = expression.left().type();

        Object right = evaluate(expression.right(), session, bindings);
        Type rightType = expression.right().type();

        return switch (expression.operator()) {
            case EQUAL -> evaluateOperator(OperatorType.EQUAL, leftType, rightType, left, right, session);
            case NOT_EQUAL -> {
                Object result = evaluateOperator(OperatorType.EQUAL, leftType, rightType, left, right, session);
                yield result == null ? null : !(Boolean) result;
            }
            case LESS_THAN -> evaluateOperator(OperatorType.LESS_THAN, leftType, rightType, left, right, session);
            case LESS_THAN_OR_EQUAL -> evaluateOperator(OperatorType.LESS_THAN_OR_EQUAL, leftType, rightType, left, right, session);
            case GREATER_THAN -> evaluateOperator(OperatorType.LESS_THAN, rightType, leftType, right, left, session);
            case GREATER_THAN_OR_EQUAL -> evaluateOperator(OperatorType.LESS_THAN_OR_EQUAL, rightType, leftType, right, left, session);
            case IDENTICAL -> evaluateOperator(OperatorType.IDENTICAL, leftType, rightType, left, right, session);
        };
    }

    private Object evaluateOperator(OperatorType operator, Type leftType, Type rightType, Object left, Object right, Session session)
    {
        return functionInvoker.invoke(
                metadata.resolveOperator(operator, ImmutableList.of(leftType, rightType)),
                session.toConnectorSession(),
                Arrays.asList(left, right));
    }

    private Object evaluateInternal(Coalesce expression, Session session, Map<String, Object> bindings)
    {
        for (Expression operand : expression.operands()) {
            Object value = evaluate(operand, session, bindings);
            if (value != null) {
                return value;
            }
        }

        return null;
    }

    private Object evaluateInternal(Cast cast, Session session, Map<String, Object> bindings)
    {
        return functionInvoker.invoke(
                metadata.getCoercion(cast.expression().type(), cast.type()),
                session.toConnectorSession(),
                singletonList(evaluate(cast.expression(), session, bindings)));
    }

    private Object evaluateInternal(Case expression, Session session, Map<String, Object> bindings)
    {
        for (WhenClause whenClause : expression.whenClauses()) {
            Object operand = evaluate(whenClause.getOperand(), session, bindings);

            if (Boolean.TRUE.equals(operand)) {
                return evaluate(whenClause.getResult(), session, bindings);
            }
        }

        return evaluate(expression.defaultValue(), session, bindings);
    }

    private Object evaluateInternal(Call call, Session session, Map<String, Object> bindings)
    {
        return functionInvoker.invoke(call.function(), session.toConnectorSession(), call.arguments().stream()
                .map(argument -> evaluate(argument, session, bindings))
                .collect(toList()));
    }

    private MethodHandle makeLambdaInvoker(Session session, Lambda lambda)
    {
        Map<String, Integer> mappings = new HashMap<>();
        for (int i = 0; i < lambda.arguments().size(); i++) {
            mappings.put(lambda.arguments().get(i).name(), i);
        }

        return LAMBDA_EVALUATOR.bindTo(this)
                .bindTo(session)
                .bindTo(lambda.body())
                .bindTo(mappings)
                .asVarargsCollector(Object[].class);
    }

    private Object evaluate(Session session, Expression body, Map<String, Integer> mappings, Object... arguments)
    {
        Map<String, Object> bindings = new HashMap<>();
        for (Map.Entry<String, Integer> entry : mappings.entrySet()) {
            bindings.put(entry.getKey(), arguments[entry.getValue()]);
        }

        return evaluate(body, session, bindings);
    }

    private Object evaluateInternal(Between expression, Session session, Map<String, Object> bindings)
    {
        Object value = evaluate(expression.value(), session, bindings);
        Object min = evaluate(expression.min(), session, bindings);
        Object max = evaluate(expression.max(), session, bindings);

        Object low = evaluateOperator(OperatorType.LESS_THAN_OR_EQUAL, expression.min().type(), expression.value().type(), min, value, session);
        Object high = evaluateOperator(OperatorType.LESS_THAN_OR_EQUAL, expression.value().type(), expression.max().type(), value, max, session);

        if (Boolean.FALSE.equals(low) || Boolean.FALSE.equals(high)) {
            return Boolean.FALSE;
        }

        if (Boolean.TRUE.equals(low) && Boolean.TRUE.equals(high)) {
            return Boolean.TRUE;
        }

        return null;
    }

    private Object evaluateInternal(Array expression, Session session, Map<String, Object> bindings)
    {
        List<Object> values = expression.elements().stream()
                .map(e -> evaluate(e, session, bindings))
                .toList();

        BlockBuilder builder = expression.elementType().createBlockBuilder(null, values.size());
        for (Object element : values) {
            writeNativeValue(expression.elementType(), builder, element);
        }

        return builder.build();
    }
}
