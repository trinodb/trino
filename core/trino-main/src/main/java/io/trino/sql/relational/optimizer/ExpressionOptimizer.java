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
package io.trino.sql.relational.optimizer;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.VariableReferenceExpression;
import io.trino.sql.tree.QualifiedName;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.OperatorNameUtil.mangleOperatorName;
import static io.trino.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY_NAME;
import static io.trino.operator.scalar.JsonStringToMapCast.JSON_STRING_TO_MAP_NAME;
import static io.trino.operator.scalar.JsonStringToRowCast.JSON_STRING_TO_ROW_NAME;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.SpecialForm.Form.BIND;
import static io.trino.type.JsonType.JSON;

public class ExpressionOptimizer
{
    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final Session session;

    public ExpressionOptimizer(Metadata metadata, FunctionManager functionManager, Session session)
    {
        this.metadata = metadata;
        this.functionManager = functionManager;
        this.session = session;
    }

    public RowExpression optimize(RowExpression expression)
    {
        return expression.accept(new Visitor(), null);
    }

    private class Visitor
            implements RowExpressionVisitor<RowExpression, Void>
    {
        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            if (call.getResolvedFunction().getSignature().getName().equals(mangleOperatorName(CAST))) {
                call = rewriteCast(call);
            }

            List<RowExpression> arguments = call.getArguments().stream()
                    .map(argument -> argument.accept(this, context))
                    .collect(toImmutableList());

            // TODO: optimize function calls with lambda arguments. For example, apply(x -> x + 2, 1)
            if (arguments.stream().allMatch(ConstantExpression.class::isInstance) && call.getResolvedFunction().isDeterministic()) {
                List<Object> constantArguments = arguments.stream()
                        .map(ConstantExpression.class::cast)
                        .map(ConstantExpression::getValue)
                        .collect(Collectors.toList());

                try {
                    InterpretedFunctionInvoker invoker = new InterpretedFunctionInvoker(functionManager);
                    return constant(invoker.invoke(call.getResolvedFunction(), session.toConnectorSession(), constantArguments), call.getType());
                }
                catch (RuntimeException e) {
                    // Do nothing. As a result, this specific tree will be left untouched. But irrelevant expressions will continue to get evaluated and optimized.
                }
            }

            return call(call.getResolvedFunction(), arguments);
        }

        @Override
        public RowExpression visitSpecialForm(SpecialForm specialForm, Void context)
        {
            switch (specialForm.getForm()) {
                // TODO: optimize these special forms
                case IF: {
                    checkState(specialForm.getArguments().size() == 3, "IF function should have 3 arguments. Get " + specialForm.getArguments().size());
                    RowExpression optimizedOperand = specialForm.getArguments().get(0).accept(this, context);
                    if (optimizedOperand instanceof ConstantExpression constantOperand) {
                        checkState(constantOperand.getType().equals(BOOLEAN), "Operand of IF function should be BOOLEAN type. Get type " + constantOperand.getType().getDisplayName());
                        if (Boolean.TRUE.equals(constantOperand.getValue())) {
                            return specialForm.getArguments().get(1).accept(this, context);
                        }
                        // FALSE and NULL
                        return specialForm.getArguments().get(2).accept(this, context);
                    }
                    List<RowExpression> arguments = specialForm.getArguments().stream()
                            .map(argument -> argument.accept(this, null))
                            .collect(toImmutableList());
                    return new SpecialForm(specialForm.getForm(), specialForm.getType(), arguments, specialForm.getFunctionDependencies());
                }
                case BIND: {
                    checkState(specialForm.getArguments().size() >= 1, BIND + " function should have at least 1 argument. Got " + specialForm.getArguments().size());

                    boolean allConstantExpression = true;
                    ImmutableList.Builder<RowExpression> optimizedArgumentsBuilder = ImmutableList.builder();
                    for (RowExpression argument : specialForm.getArguments()) {
                        RowExpression optimizedArgument = argument.accept(this, context);
                        if (!(optimizedArgument instanceof ConstantExpression)) {
                            allConstantExpression = false;
                        }
                        optimizedArgumentsBuilder.add(optimizedArgument);
                    }
                    if (allConstantExpression) {
                        // Here, optimizedArguments should be merged together into a new ConstantExpression.
                        // It's not implemented because it would be dead code anyways because visitLambda does not produce ConstantExpression.
                        throw new UnsupportedOperationException();
                    }
                    return new SpecialForm(specialForm.getForm(), specialForm.getType(), optimizedArgumentsBuilder.build(), specialForm.getFunctionDependencies());
                }
                case NULL_IF:
                case SWITCH:
                case WHEN:
                case BETWEEN:
                case IS_NULL:
                case COALESCE:
                case AND:
                case OR:
                case IN:
                case DEREFERENCE:
                case ROW_CONSTRUCTOR: {
                    List<RowExpression> arguments = specialForm.getArguments().stream()
                            .map(argument -> argument.accept(this, null))
                            .collect(toImmutableList());
                    return new SpecialForm(specialForm.getForm(), specialForm.getType(), arguments, specialForm.getFunctionDependencies());
                }
            }
            throw new IllegalArgumentException("Unsupported special form " + specialForm.getForm());
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return new LambdaDefinitionExpression(lambda.getArgumentTypes(), lambda.getArguments(), lambda.getBody().accept(this, context));
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return reference;
        }

        private CallExpression rewriteCast(CallExpression call)
        {
            if (call.getArguments().get(0) instanceof CallExpression innerCall) {
                // Optimization for CAST(JSON_PARSE(...) AS ARRAY/MAP/ROW)
                if (innerCall.getResolvedFunction().getSignature().getName().equals("json_parse")) {
                    checkArgument(innerCall.getType().equals(JSON));
                    checkArgument(innerCall.getArguments().size() == 1);
                    Type returnType = call.getType();
                    if (returnType instanceof ArrayType) {
                        return call(
                                metadata.getCoercion(session, QualifiedName.of(JSON_STRING_TO_ARRAY_NAME), VARCHAR, returnType),
                                innerCall.getArguments());
                    }
                    if (returnType instanceof MapType) {
                        return call(
                                metadata.getCoercion(session, QualifiedName.of(JSON_STRING_TO_MAP_NAME), VARCHAR, returnType),
                                innerCall.getArguments());
                    }
                    if (returnType instanceof RowType) {
                        return call(
                                metadata.getCoercion(session, QualifiedName.of(JSON_STRING_TO_ROW_NAME), VARCHAR, returnType),
                                innerCall.getArguments());
                    }
                }
            }

            return call(
                    metadata.getCoercion(session, call.getArguments().get(0).getType(), call.getType()),
                    call.getArguments());
        }
    }
}
