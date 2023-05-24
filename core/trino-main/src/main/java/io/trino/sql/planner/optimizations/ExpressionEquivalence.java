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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import io.trino.Session;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.SpecialForm.Form;
import io.trino.sql.relational.VariableReferenceExpression;
import io.trino.sql.tree.Expression;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.OperatorNameUtil.mangleOperatorName;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.relational.SpecialForm.Form.AND;
import static io.trino.sql.relational.SpecialForm.Form.OR;
import static io.trino.sql.relational.SqlToRowExpressionTranslator.translate;
import static java.lang.Integer.min;
import static java.util.Objects.requireNonNull;

public class ExpressionEquivalence
{
    private static final Ordering<RowExpression> ROW_EXPRESSION_ORDERING = Ordering.from(new RowExpressionComparator());
    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final TypeAnalyzer typeAnalyzer;
    private final CanonicalizationVisitor canonicalizationVisitor;

    public ExpressionEquivalence(Metadata metadata, FunctionManager functionManager, TypeAnalyzer typeAnalyzer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.canonicalizationVisitor = new CanonicalizationVisitor();
    }

    public boolean areExpressionsEquivalent(Session session, Expression leftExpression, Expression rightExpression, TypeProvider types)
    {
        Map<Symbol, Integer> symbolInput = new HashMap<>();
        int inputId = 0;
        for (Entry<Symbol, Type> entry : types.allTypes().entrySet()) {
            symbolInput.put(entry.getKey(), inputId);
            inputId++;
        }
        RowExpression leftRowExpression = toRowExpression(session, leftExpression, symbolInput, types);
        RowExpression rightRowExpression = toRowExpression(session, rightExpression, symbolInput, types);

        RowExpression canonicalizedLeft = leftRowExpression.accept(canonicalizationVisitor, null);
        RowExpression canonicalizedRight = rightRowExpression.accept(canonicalizationVisitor, null);

        return canonicalizedLeft.equals(canonicalizedRight);
    }

    private RowExpression toRowExpression(Session session, Expression expression, Map<Symbol, Integer> symbolInput, TypeProvider types)
    {
        return translate(
                expression,
                typeAnalyzer.getTypes(session, types, expression),
                symbolInput,
                metadata,
                functionManager,
                session,
                false);
    }

    private static class CanonicalizationVisitor
            implements RowExpressionVisitor<RowExpression, Void>
    {
        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            call = new CallExpression(
                    call.getResolvedFunction(),
                    call.getArguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(toImmutableList()));

            String callName = call.getResolvedFunction().getSignature().getName();

            if (callName.equals(mangleOperatorName(EQUAL)) || callName.equals(mangleOperatorName(IS_DISTINCT_FROM))) {
                // sort arguments
                return new CallExpression(
                        call.getResolvedFunction(),
                        ROW_EXPRESSION_ORDERING.sortedCopy(call.getArguments()));
            }

            return call;
        }

        @Override
        public RowExpression visitSpecialForm(SpecialForm specialForm, Void context)
        {
            specialForm = new SpecialForm(
                    specialForm.getForm(),
                    specialForm.getType(),
                    specialForm.getArguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(toImmutableList()),
                    specialForm.getFunctionDependencies());

            if (specialForm.getForm() == AND || specialForm.getForm() == OR) {
                // if we have nested calls (of the same type) flatten them
                List<RowExpression> flattenedArguments = flattenNestedCallArgs(specialForm);

                // only consider distinct arguments
                Set<RowExpression> distinctArguments = ImmutableSet.copyOf(flattenedArguments);
                if (distinctArguments.size() == 1) {
                    return Iterables.getOnlyElement(distinctArguments);
                }

                // canonicalize the argument order (i.e., sort them)
                List<RowExpression> sortedArguments = ROW_EXPRESSION_ORDERING.sortedCopy(distinctArguments);

                return new SpecialForm(specialForm.getForm(), BOOLEAN, sortedArguments, specialForm.getFunctionDependencies());
            }

            return specialForm;
        }

        public static List<RowExpression> flattenNestedCallArgs(SpecialForm specialForm)
        {
            Form form = specialForm.getForm();
            ImmutableList.Builder<RowExpression> newArguments = ImmutableList.builder();
            for (RowExpression argument : specialForm.getArguments()) {
                if (argument instanceof SpecialForm && form == ((SpecialForm) argument).getForm()) {
                    // same special form type, so flatten the args
                    newArguments.addAll(flattenNestedCallArgs((SpecialForm) argument));
                }
                else {
                    newArguments.add(argument);
                }
            }
            return newArguments.build();
        }

        @Override
        public RowExpression visitConstant(ConstantExpression constant, Void context)
        {
            return constant;
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression node, Void context)
        {
            return node;
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
    }

    private static class RowExpressionComparator
            implements Comparator<RowExpression>
    {
        private final Comparator<Object> classComparator = Ordering.arbitrary();
        private final ListComparator<RowExpression> argumentComparator = new ListComparator<>(this);

        @Override
        public int compare(RowExpression left, RowExpression right)
        {
            int result = classComparator.compare(left.getClass(), right.getClass());
            if (result != 0) {
                return result;
            }

            if (left instanceof CallExpression leftCall) {
                CallExpression rightCall = (CallExpression) right;
                return ComparisonChain.start()
                        .compare(leftCall.getResolvedFunction().toString(), rightCall.getResolvedFunction().toString())
                        .compare(leftCall.getArguments(), rightCall.getArguments(), argumentComparator)
                        .result();
            }

            if (left instanceof SpecialForm leftForm) {
                SpecialForm rightForm = (SpecialForm) right;
                return ComparisonChain.start()
                        .compare(leftForm.getForm(), rightForm.getForm())
                        .compare(leftForm.getArguments(), rightForm.getArguments(), argumentComparator)
                        .result();
            }

            if (left instanceof ConstantExpression leftConstant) {
                ConstantExpression rightConstant = (ConstantExpression) right;

                result = leftConstant.getType().getTypeSignature().toString().compareTo(right.getType().getTypeSignature().toString());
                if (result != 0) {
                    return result;
                }

                Object leftValue = leftConstant.getValue();
                Object rightValue = rightConstant.getValue();

                if (leftValue == null) {
                    if (rightValue == null) {
                        return 0;
                    }
                    return -1;
                }
                if (rightValue == null) {
                    return 1;
                }

                Class<?> javaType = leftConstant.getType().getJavaType();
                if (javaType == boolean.class) {
                    return ((Boolean) leftValue).compareTo((Boolean) rightValue);
                }
                if (javaType == byte.class || javaType == short.class || javaType == int.class || javaType == long.class) {
                    return Long.compare(((Number) leftValue).longValue(), ((Number) rightValue).longValue());
                }
                if (javaType == float.class || javaType == double.class) {
                    return Double.compare(((Number) leftValue).doubleValue(), ((Number) rightValue).doubleValue());
                }
                if (leftValue instanceof Comparable) {
                    try {
                        //noinspection unchecked,rawtypes
                        return ((Comparable) leftValue).compareTo(rightValue);
                    }
                    catch (RuntimeException ignored) {
                    }
                }

                // value is some random type (say regex), so we just randomly choose a greater value
                // todo: support all known type
                return -1;
            }

            if (left instanceof InputReferenceExpression leftInputReferenceExpression) {
                return Integer.compare(leftInputReferenceExpression.getField(), ((InputReferenceExpression) right).getField());
            }

            if (left instanceof LambdaDefinitionExpression leftLambda) {
                LambdaDefinitionExpression rightLambda = (LambdaDefinitionExpression) right;

                return ComparisonChain.start()
                        .compare(
                                leftLambda.getArgumentTypes(),
                                rightLambda.getArgumentTypes(),
                                new ListComparator<>(Comparator.comparing(Object::toString)))
                        .compare(
                                leftLambda.getArguments(),
                                rightLambda.getArguments(),
                                new ListComparator<>(Comparator.<String>naturalOrder()))
                        .compare(leftLambda.getBody(), rightLambda.getBody(), this)
                        .result();
            }

            if (left instanceof VariableReferenceExpression leftVariableReference) {
                VariableReferenceExpression rightVariableReference = (VariableReferenceExpression) right;

                return ComparisonChain.start()
                        .compare(leftVariableReference.getName(), rightVariableReference.getName())
                        .compare(leftVariableReference.getType(), rightVariableReference.getType(), Comparator.comparing(Object::toString))
                        .result();
            }

            throw new IllegalArgumentException("Unsupported RowExpression type " + left.getClass().getSimpleName());
        }
    }

    private static class ListComparator<T>
            implements Comparator<List<T>>
    {
        private final Comparator<T> elementComparator;

        public ListComparator(Comparator<T> elementComparator)
        {
            this.elementComparator = requireNonNull(elementComparator, "elementComparator is null");
        }

        @Override
        public int compare(List<T> left, List<T> right)
        {
            int compareLength = min(left.size(), right.size());
            for (int i = 0; i < compareLength; i++) {
                int result = elementComparator.compare(left.get(i), right.get(i));
                if (result != 0) {
                    return result;
                }
            }
            return Integer.compare(left.size(), right.size());
        }
    }

    private static <T> List<T> swapPair(List<T> pair)
    {
        requireNonNull(pair, "pair is null");
        checkArgument(pair.size() == 2, "Expected pair to have two elements");
        return ImmutableList.of(pair.get(1), pair.get(0));
    }
}
