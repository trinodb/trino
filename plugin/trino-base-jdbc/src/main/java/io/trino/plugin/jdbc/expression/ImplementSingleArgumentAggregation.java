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
package io.trino.plugin.jdbc.expression;

import com.google.common.collect.ImmutableSet;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.expressionType;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.singleInput;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.variable;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class ImplementSingleArgumentAggregation
        implements AggregateFunctionRule
{
    private static final Capture<Variable> INPUT = newCapture();

    private final Set<String> functionNames;
    private final Predicate<Type> inputType;
    private final BiPredicate<Type, Type> inputAndOutputPredicate;
    private final ResultExpressionFormatter resultExpression;
    private final ResultTypeHandleSupplier resultTypeHandle;

    private ImplementSingleArgumentAggregation(
            Set<String> functionNames,
            Predicate<Type> inputType,
            BiPredicate<Type, Type> inputAndOutputPredicate,
            ResultExpressionFormatter resultExpression,
            ResultTypeHandleSupplier resultTypeHandle)
    {
        requireNonNull(functionNames, "functionNames is null");
        checkArgument(!functionNames.isEmpty(), "functionNames cannot be empty");
        functionNames.forEach(name -> checkArgument(name.equals(name.toLowerCase(ENGLISH)), "Nont lowercase function name: %s", name));
        this.functionNames = ImmutableSet.copyOf(functionNames);
        this.inputType = requireNonNull(inputType, "inputType is null");
        this.inputAndOutputPredicate = inputAndOutputPredicate;
        this.resultExpression = requireNonNull(resultExpression, "functionNames is null");
        this.resultTypeHandle = requireNonNull(resultTypeHandle, "resultTypeHandle is null");
    }

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                // TODO (https://github.com/trinodb/trino/issues/6736) remove toLowerCase
                .with(functionName().matching(name -> functionNames.contains(name.toLowerCase(ENGLISH))))
                .with(singleInput().matching(
                        variable()
                                .with(expressionType().matching(inputType))
                                .capturedAs(INPUT)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        Variable input = captures.get(INPUT);
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(input.getName());
        Type inputType = columnHandle.getColumnType();
        Type outputType = aggregateFunction.getOutputType();

        if (!inputAndOutputPredicate.test(inputType, outputType)) {
            return Optional.empty();
        }

        Optional<JdbcTypeHandle> outputTypeHandle = resultTypeHandle.getOutputTypeHandle(outputType, inputType, columnHandle.getJdbcTypeHandle());
        if (outputTypeHandle.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new JdbcExpression(
                resultExpression.format(context.getIdentifierQuote().apply(columnHandle.getColumnName()), inputType),
                outputTypeHandle.get()));
    }

    public static BuilderExpectingInputType mapAggregation(String functionName)
    {
        return builder().setFunctionName(functionName);
    }

    public static BuilderExpectingInputType mapAggregation(String... functionNames)
    {
        return builder().setFunctionNames(functionNames);
    }

    private static Builder builder()
    {
        return new Builder();
    }

    public interface BuilderExpectingInputType
            // input type condition is optional
            extends BuilderExpectingResultType
    {
        BuilderExpectingResultType on(Type type);

        BuilderExpectingResultType on(Type... types);

        BuilderExpectingResultType on(Class<? extends Type> type);
    }

    public interface BuilderExpectingResultType
            // result type supplier is optional
            extends BuilderExpectingExpression
    {
        default BuilderExpectingExpression returning(Function<Type, Optional<JdbcTypeHandle>> resultTypeHandle)
        {
            return returning(Type.class, resultTypeHandle);
        }

        <T extends Type> BuilderExpectingExpression returning(Class<T> expectedOutputType, Function<T, Optional<JdbcTypeHandle>> resultTypeHandle);
    }

    public interface BuilderExpectingExpression
    {
        AggregateFunctionRule to(String expressionWithPlaceholder);
    }

    private static class Builder
            implements BuilderExpectingInputType, BuilderExpectingResultType, BuilderExpectingExpression
    {
        private Set<String> functionNames;
        private Predicate<Type> inputType = type -> true;

        private BiPredicate<Type, Type> inputAndOutputPredicate = Type::equals;
        private ResultTypeHandleSupplier resultTypeHandle = (outputType, inputType, inputTypeHandle) -> {
            verify(outputType.equals(inputType));
            return Optional.of(requireNonNull(inputTypeHandle, "inputTypeHandle is null"));
        };

        private ResultExpressionFormatter resultExpression;

        private Builder() {}

        public Builder setFunctionName(String functionName)
        {
            this.functionNames = ImmutableSet.of(functionName);
            return this;
        }

        public Builder setFunctionNames(String... functionNames)
        {
            this.functionNames = ImmutableSet.copyOf(functionNames);
            return this;
        }

        @Override
        public Builder on(Type type)
        {
            this.inputType = requireNonNull(type, "type is null")::equals;
            return this;
        }

        @Override
        public Builder on(Type... types)
        {
            this.inputType = ImmutableSet.copyOf(requireNonNull(types, "types is null"))::contains;
            return this;
        }

        @Override
        public Builder on(Class<? extends Type> type)
        {
            this.inputType = requireNonNull(type, "type is null")::isInstance;
            return this;
        }

        @Override
        public <T extends Type> Builder returning(Class<T> expectedOutputType, Function<T, Optional<JdbcTypeHandle>> resultTypeHandle)
        {
            requireNonNull(expectedOutputType, "expectedOutputType is null");
            requireNonNull(resultTypeHandle, "resultTypeHandle is null");
            inputAndOutputPredicate = (inputType, outputType) -> expectedOutputType.isInstance(outputType);
            this.resultTypeHandle = (outputType, inputType, inputTypeHandle) -> resultTypeHandle.apply(expectedOutputType.cast(outputType));
            return this;
        }

        @Override
        public AggregateFunctionRule to(String expressionWithPlaceholder)
        {
            requireNonNull(expressionWithPlaceholder, "expressionWithPlaceholder is null");
            this.resultExpression = (inputExpression, inputType) -> format(expressionWithPlaceholder, inputExpression);
            return build();
        }

        public AggregateFunctionRule build()
        {
            return new ImplementSingleArgumentAggregation(
                    functionNames,
                    inputType,
                    inputAndOutputPredicate,
                    resultExpression,
                    resultTypeHandle);
        }
    }

    private interface ResultExpressionFormatter
    {
        String format(String inputExpression, Type inputType);
    }

    private interface ResultTypeHandleSupplier
    {
        Optional<JdbcTypeHandle> getOutputTypeHandle(Type outputType, Type inputType, JdbcTypeHandle inputTypeHandle);
    }
}
