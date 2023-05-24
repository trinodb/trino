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
package io.trino.plugin.pinot.query;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Match;
import io.trino.matching.Pattern;
import io.trino.plugin.pinot.PinotException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.operator.transform.transformer.datetime.BaseDateTimeTransformer;
import org.apache.pinot.core.operator.transform.transformer.datetime.DateTimeTransformerFactory;
import org.apache.pinot.core.operator.transform.transformer.datetime.EpochToEpochTransformer;
import org.apache.pinot.segment.spi.AggregationFunctionType;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.immutableEnumMap;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_EXCEPTION;
import static io.trino.plugin.pinot.query.PinotPatterns.WILDCARD;
import static io.trino.plugin.pinot.query.PinotPatterns.aggregationFunction;
import static io.trino.plugin.pinot.query.PinotPatterns.aggregationFunctionType;
import static io.trino.plugin.pinot.query.PinotPatterns.expression;
import static io.trino.plugin.pinot.query.PinotPatterns.expressionType;
import static io.trino.plugin.pinot.query.PinotPatterns.function;
import static io.trino.plugin.pinot.query.PinotPatterns.identifier;
import static io.trino.plugin.pinot.query.PinotPatterns.singleInput;
import static io.trino.plugin.pinot.query.PinotPatterns.transformFunction;
import static io.trino.plugin.pinot.query.PinotPatterns.transformFunctionType;
import static io.trino.plugin.pinot.query.PinotSqlFormatter.getColumnHandle;
import static io.trino.plugin.pinot.query.PinotTransformFunctionTypeResolver.getTransformFunctionType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.pinot.common.function.TransformFunctionType.DATETIMECONVERT;
import static org.apache.pinot.common.function.TransformFunctionType.DATETRUNC;
import static org.apache.pinot.common.function.TransformFunctionType.TIMECONVERT;
import static org.apache.pinot.common.request.Literal.stringValue;
import static org.apache.pinot.common.request.context.ExpressionContext.Type.FUNCTION;
import static org.apache.pinot.common.request.context.ExpressionContext.Type.IDENTIFIER;
import static org.apache.pinot.common.request.context.ExpressionContext.Type.LITERAL;
import static org.apache.pinot.common.request.context.ExpressionContext.forFunction;
import static org.apache.pinot.common.request.context.ExpressionContext.forIdentifier;
import static org.apache.pinot.common.request.context.ExpressionContext.forLiteralContext;
import static org.apache.pinot.core.operator.transform.function.DateTruncTransformFunction.EXAMPLE_INVOCATION;
import static org.apache.pinot.core.operator.transform.transformer.timeunit.TimeUnitTransformerFactory.getTimeUnitTransformer;
import static org.apache.pinot.segment.spi.AggregationFunctionType.COUNT;
import static org.apache.pinot.segment.spi.AggregationFunctionType.getAggregationFunctionType;

public class PinotExpressionRewriter
{
    private static final Map<TransformFunctionType, RewriteRule<FunctionContext>> FUNCTION_RULE_MAP;
    private static final Map<AggregationFunctionType, RewriteRule<FunctionContext>> AGGREGATION_FUNCTION_RULE_MAP;
    private static final RewriteRule<FunctionContext> DEFAULT_REWRITE_RULE = new DefaultRewriteRule();

    private PinotExpressionRewriter() {}

    static {
        Map<TransformFunctionType, RewriteRule<FunctionContext>> functionMap = new HashMap<>();
        functionMap.put(DATETIMECONVERT, new DateTimeConvertRewriteRule());
        functionMap.put(TIMECONVERT, new TimeConvertRewriteRule());
        functionMap.put(DATETRUNC, new DateTruncRewriteRule());
        FUNCTION_RULE_MAP = immutableEnumMap(functionMap);

        Map<AggregationFunctionType, RewriteRule<FunctionContext>> aggregationFunctionMap = new HashMap<>();
        aggregationFunctionMap.put(COUNT, new CountStarRewriteRule());
        AGGREGATION_FUNCTION_RULE_MAP = immutableEnumMap(aggregationFunctionMap);
    }

    public static ExpressionContext rewriteExpression(SchemaTableName schemaTableName, ExpressionContext expressionContext, Map<String, ColumnHandle> columnHandles)
    {
        requireNonNull(expressionContext, "expressionContext is null");
        Context context = new Context() {
            @Override
            public SchemaTableName getSchemaTableName()
            {
                return schemaTableName;
            }

            @Override
            public Map<String, ColumnHandle> getColumnHandles()
            {
                return columnHandles;
            }
        };
        return rewriteExpression(expressionContext, context);
    }

    private static ExpressionContext rewriteExpression(ExpressionContext expressionContext, Context context)
    {
        switch (expressionContext.getType()) {
            case LITERAL:
                return expressionContext;
            case IDENTIFIER:
                return forIdentifier(getColumnHandle(expressionContext.getIdentifier(), context.getSchemaTableName(), context.getColumnHandles()).getColumnName());
            case FUNCTION:
                return forFunction(rewriteFunction(expressionContext.getFunction(), context));
        }
        throw new PinotException(PINOT_EXCEPTION, Optional.empty(), format("Unsupported expression type '%s'", expressionContext.getType()));
    }

    private static FunctionContext rewriteFunction(FunctionContext functionContext, Context context)
    {
        Optional<FunctionContext> result = Optional.empty();
        if (functionContext.getType() == FunctionContext.Type.TRANSFORM) {
            RewriteRule<FunctionContext> rule = FUNCTION_RULE_MAP.get(getTransformFunctionType(functionContext).orElseThrow());
            if (rule != null) {
                result = applyRule(rule, functionContext, context);
            }
        }
        else {
            checkState(functionContext.getType() == FunctionContext.Type.AGGREGATION, "Unexpected function type for '%s'", functionContext);
            RewriteRule<FunctionContext> rule = AGGREGATION_FUNCTION_RULE_MAP.get(getAggregationFunctionType(functionContext.getFunctionName()));
            if (rule != null) {
                result = applyRule(rule, functionContext, context);
            }
        }
        if (result.isPresent()) {
            return result.get();
        }
        result = applyRule(DEFAULT_REWRITE_RULE, functionContext, context);
        if (result.isPresent()) {
            return result.get();
        }
        throw new PinotException(PINOT_EXCEPTION, Optional.empty(), format("Unsupported function expression '%s'", functionContext));
    }

    private static <T> Optional<T> applyRule(RewriteRule<T> rule, T object, Context context)
    {
        Iterator<Match> iterator = rule.getPattern().match(object).iterator();
        while (iterator.hasNext()) {
            Match match = iterator.next();
            return Optional.of(rule.rewrite(object, match.captures(), context));
        }
        return Optional.empty();
    }

    private static class DateTimeConvertRewriteRule
            implements RewriteRule<FunctionContext>
    {
        @Override
        public Pattern<FunctionContext> getPattern()
        {
            return transformFunction().with(transformFunctionType().equalTo(DATETIMECONVERT));
        }

        @Override
        public FunctionContext rewrite(FunctionContext object, Captures captures, Context context)
        {
            // Extracted from org.apache.pinot.core.operator.transform.function.DateTimeConversionTransformFunction
            // The first argument must be an identifier or function and the 2nd, 3rd and 4th arguments must be literals
            verify(object.getArguments().size() == 4);
            verifyIsIdentifierOrFunction(object.getArguments().get(0));
            verifyTailArgumentsAllLiteral(object.getArguments());

            ImmutableList.Builder<ExpressionContext> argumentsBuilder = ImmutableList.builder();
            argumentsBuilder.add(rewriteExpression(object.getArguments().get(0), context));
            String inputFormat = object.getArguments().get(1).getLiteral().getValue().toString().toUpperCase(ENGLISH);
            argumentsBuilder.add(forLiteralContext(stringValue(inputFormat)));
            String outputFormat = object.getArguments().get(2).getLiteral().getValue().toString().toUpperCase(ENGLISH);
            argumentsBuilder.add(forLiteralContext(stringValue(outputFormat)));
            String granularity = object.getArguments().get(3).getLiteral().getValue().toString().toUpperCase(ENGLISH);
            BaseDateTimeTransformer<?, ?> dateTimeTransformer = DateTimeTransformerFactory.getDateTimeTransformer(inputFormat, outputFormat, granularity);
            // Even if the format is valid, make sure it is not a simple date format: format characters can be ambiguous due to lower casing
            checkState(dateTimeTransformer instanceof EpochToEpochTransformer, "Unsupported date format: simple date format not supported");
            argumentsBuilder.add(forLiteralContext(stringValue(granularity)));
            return new FunctionContext(object.getType(), object.getFunctionName(), argumentsBuilder.build());
        }
    }

    private static class TimeConvertRewriteRule
            implements RewriteRule<FunctionContext>
    {
        @Override
        public Pattern<FunctionContext> getPattern()
        {
            return transformFunction().with(transformFunctionType().equalTo(TIMECONVERT));
        }

        @Override
        public FunctionContext rewrite(FunctionContext object, Captures captures, Context context)
        {
            // Extracted from org.apache.pinot.core.operator.transform.function.DateTimeConversionTransformFunction
            // The first argument must be an identifier or function and the 2nd, and 3rd arguments must be literals
            verify(object.getArguments().size() == 3);
            verifyIsIdentifierOrFunction(object.getArguments().get(0));
            verifyTailArgumentsAllLiteral(object.getArguments());

            ImmutableList.Builder<ExpressionContext> argumentsBuilder = ImmutableList.builder();
            argumentsBuilder.add(rewriteExpression(object.getArguments().get(0), context));
            String inputTimeUnitArgument = object.getArguments().get(1).getLiteral().getValue().toString().toUpperCase(ENGLISH);
            TimeUnit inputTimeUnit = TimeUnit.valueOf(inputTimeUnitArgument);
            String outputTimeUnitArgument = object.getArguments().get(2).getLiteral().getValue().toString().toUpperCase(ENGLISH);
            // Check that this is a valid time unit transform
            getTimeUnitTransformer(inputTimeUnit, outputTimeUnitArgument);
            argumentsBuilder.add(forLiteralContext(stringValue(inputTimeUnitArgument)));
            argumentsBuilder.add(forLiteralContext(stringValue(outputTimeUnitArgument)));
            return new FunctionContext(object.getType(), object.getFunctionName(), argumentsBuilder.build());
        }
    }

    private static class DateTruncRewriteRule
            implements RewriteRule<FunctionContext>
    {
        @Override
        public Pattern<FunctionContext> getPattern()
        {
            return transformFunction().with(transformFunctionType().equalTo(DATETRUNC));
        }

        @Override
        public FunctionContext rewrite(FunctionContext object, Captures captures, Context context)
        {
            // Extracted from org.apache.pinot.core.operator.transform.function.DateTruncTransformFunction
            List<ExpressionContext> arguments = object.getArguments();
            checkState(arguments.size() >= 2 && arguments.size() <= 5,
                    "Between two to five arguments are required, example: %s", EXAMPLE_INVOCATION);

            ImmutableList.Builder<ExpressionContext> argumentsBuilder = ImmutableList.builder();

            checkState(arguments.get(0).getType() == LITERAL, "First argument must be a literal");
            String unit = arguments.get(0).getLiteral().getValue().toString().toLowerCase(ENGLISH);
            argumentsBuilder.add(forLiteralContext(stringValue(unit)));
            verifyIsIdentifierOrFunction(object.getArguments().get(1));
            ExpressionContext valueArgument = rewriteExpression(arguments.get(1), context);
            argumentsBuilder.add(valueArgument);
            if (arguments.size() >= 3) {
                checkState(arguments.get(2).getType() == LITERAL, "Unexpected 3rd argument: '%s'", arguments.get(2));
                String inputTimeUnitArgument = arguments.get(2).getLiteral().getValue().toString().toUpperCase(ENGLISH);
                // Ensure this is a valid TimeUnit
                TimeUnit inputTimeUnit = TimeUnit.valueOf(inputTimeUnitArgument);
                argumentsBuilder.add(forLiteralContext(stringValue(inputTimeUnit.name())));
                if (arguments.size() >= 4) {
                    checkState(arguments.get(3).getType() == LITERAL, "Unexpected 4th argument '%s'", arguments.get(3));
                    // Time zone is lower cased inside Pinot
                    argumentsBuilder.add(arguments.get(3));
                    if (arguments.size() >= 5) {
                        checkState(arguments.get(4).getType() == LITERAL, "Unexpected 5th argument: '%s'", arguments.get(4));
                        String outputTimeUnitArgument = arguments.get(4).getLiteral().getValue().toString().toUpperCase(ENGLISH);
                        // Ensure this is a valid TimeUnit
                        TimeUnit outputTimeUnit = TimeUnit.valueOf(outputTimeUnitArgument);
                        argumentsBuilder.add(forLiteralContext(stringValue(outputTimeUnit.name())));
                    }
                }
            }
            return new FunctionContext(object.getType(), object.getFunctionName(), argumentsBuilder.build());
        }
    }

    private static class CountStarRewriteRule
            implements RewriteRule<FunctionContext>
    {
        @Override
        public Pattern<FunctionContext> getPattern()
        {
            return aggregationFunction()
                    .with(aggregationFunctionType().equalTo(COUNT))
                    .with(singleInput().matching(expression()
                            .with(expressionType().equalTo(IDENTIFIER))
                            .with(identifier().equalTo(WILDCARD))));
        }

        @Override
        public FunctionContext rewrite(FunctionContext object, Captures captures, Context context)
        {
            return object;
        }
    }

    private static class DefaultRewriteRule
            implements RewriteRule<FunctionContext>
    {
        @Override
        public Pattern<FunctionContext> getPattern()
        {
            return function();
        }

        @Override
        public FunctionContext rewrite(FunctionContext object, Captures captures, Context context)
        {
            List<ExpressionContext> arguments = object.getArguments().stream().map(argument -> rewriteExpression(argument, context))
                    .collect(toImmutableList());
            return new FunctionContext(object.getType(), object.getFunctionName(), arguments);
        }
    }

    private static void verifyIsIdentifierOrFunction(ExpressionContext expressionContext)
    {
        verify(expressionContext.getType() == IDENTIFIER || expressionContext.getType() == FUNCTION);
    }

    private static void verifyTailArgumentsAllLiteral(List<ExpressionContext> arguments)
    {
        arguments.stream().skip(1)
                .forEach(argument -> verify(argument.getType() == LITERAL));
    }

    private interface Context
    {
        SchemaTableName getSchemaTableName();

        Map<String, ColumnHandle> getColumnHandles();
    }

    private interface RewriteRule<T>
    {
        Pattern<T> getPattern();

        T rewrite(T object, Captures captures, Context context);
    }
}
