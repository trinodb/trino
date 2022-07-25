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
package io.trino.operator.project;

import com.google.common.collect.ImmutableList;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.VariableReferenceExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.relational.Expressions.field;

/**
 * Rewrite input references from columns in the input page (to the filter/project node)
 * into a compact list that can be used for method parameters.
 */
public final class PageFieldsToInputParametersRewriter
{
    private PageFieldsToInputParametersRewriter() {}

    public static Result rewritePageFieldsToInputParameters(RowExpression expression)
    {
        Visitor visitor = new Visitor();
        RowExpression rewrittenProjection = expression.accept(visitor, true);
        InputChannels inputChannels = new InputChannels(visitor.getInputChannels(), visitor.getEagerlyLoadedChannels());
        return new Result(rewrittenProjection, inputChannels);
    }

    private static class Visitor
            implements RowExpressionVisitor<RowExpression, Boolean>
    {
        private final Map<Integer, Integer> fieldToParameter = new HashMap<>();
        private final List<Integer> inputChannels = new ArrayList<>();
        private final Set<Integer> eagerlyLoadedChannels = new HashSet<>();
        private int nextParameter;

        public List<Integer> getInputChannels()
        {
            return ImmutableList.copyOf(inputChannels);
        }

        public List<Integer> getEagerlyLoadedChannels()
        {
            return ImmutableList.copyOf(eagerlyLoadedChannels);
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Boolean unconditionallyEvaluated)
        {
            if (unconditionallyEvaluated) {
                eagerlyLoadedChannels.add(reference.getField());
            }
            int parameter = getParameterForField(reference);
            return field(parameter, reference.getType());
        }

        private Integer getParameterForField(InputReferenceExpression reference)
        {
            return fieldToParameter.computeIfAbsent(reference.getField(), field -> {
                inputChannels.add(field);
                return nextParameter++;
            });
        }

        @Override
        public RowExpression visitCall(CallExpression call, Boolean unconditionallyEvaluated)
        {
            boolean containsLambdaExpression = call.getArguments().stream().anyMatch(LambdaDefinitionExpression.class::isInstance);
            return new CallExpression(
                    call.getResolvedFunction(),
                    call.getArguments().stream()
                            // Lambda expressions may use only some of their input references, e.g. transform(elements, x -> 1)
                            // TODO: Currently we fallback to assuming that all the arguments are conditionally evaluated when
                            //   a lambda expression is encountered for the sake of simplicity.
                            .map(expression -> expression.accept(this, unconditionallyEvaluated && !containsLambdaExpression))
                            .collect(toImmutableList()));
        }

        @Override
        public RowExpression visitSpecialForm(SpecialForm specialForm, Boolean unconditionallyEvaluated)
        {
            switch (specialForm.getForm()) {
                case IF:
                case SWITCH:
                case BETWEEN:
                case AND:
                case OR:
                case COALESCE:
                    List<RowExpression> arguments = specialForm.getArguments();
                    return new SpecialForm(
                            specialForm.getForm(),
                            specialForm.getType(),
                            IntStream.range(0, arguments.size()).boxed()
                                    // All the arguments after the first one are assumed to be conditionally evaluated
                                    .map(index -> arguments.get(index).accept(this, index == 0 && unconditionallyEvaluated))
                                    .collect(toImmutableList()),
                            specialForm.getFunctionDependencies());
                case BIND:
                case IN:
                case WHEN:
                case IS_NULL:
                case NULL_IF:
                case DEREFERENCE:
                case ROW_CONSTRUCTOR:
                    return new SpecialForm(
                            specialForm.getForm(),
                            specialForm.getType(),
                            specialForm.getArguments().stream()
                                    .map(expression -> expression.accept(this, unconditionallyEvaluated))
                                    .collect(toImmutableList()),
                            specialForm.getFunctionDependencies());
            }
            throw new IllegalArgumentException("Unsupported special form " + specialForm.getForm());
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Boolean unconditionallyEvaluated)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Boolean unconditionallyEvaluated)
        {
            return new LambdaDefinitionExpression(
                    lambda.getArgumentTypes(),
                    lambda.getArguments(),
                    lambda.getBody().accept(this, unconditionallyEvaluated));
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Boolean unconditionallyEvaluated)
        {
            return reference;
        }
    }

    public static class Result
    {
        private final RowExpression rewrittenExpression;
        private final InputChannels inputChannels;

        public Result(RowExpression rewrittenExpression, InputChannels inputChannels)
        {
            this.rewrittenExpression = rewrittenExpression;
            this.inputChannels = inputChannels;
        }

        public RowExpression getRewrittenExpression()
        {
            return rewrittenExpression;
        }

        public InputChannels getInputChannels()
        {
            return inputChannels;
        }
    }
}
