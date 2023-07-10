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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Match;
import io.trino.matching.Pattern;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.plugin.pinot.PinotException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.segment.spi.AggregationFunctionType;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.immutableEnumMap;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_EXCEPTION;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_INVALID_PQL_GENERATED;
import static io.trino.plugin.pinot.query.DynamicTablePqlExtractor.quoteIdentifier;
import static io.trino.plugin.pinot.query.PinotPatterns.WILDCARD;
import static io.trino.plugin.pinot.query.PinotPatterns.aggregationFunction;
import static io.trino.plugin.pinot.query.PinotPatterns.aggregationFunctionType;
import static io.trino.plugin.pinot.query.PinotPatterns.arguments;
import static io.trino.plugin.pinot.query.PinotPatterns.binaryFunction;
import static io.trino.plugin.pinot.query.PinotPatterns.binaryFunctionPredicateValue;
import static io.trino.plugin.pinot.query.PinotPatterns.binaryOperator;
import static io.trino.plugin.pinot.query.PinotPatterns.binaryOperatorValue;
import static io.trino.plugin.pinot.query.PinotPatterns.childFilters;
import static io.trino.plugin.pinot.query.PinotPatterns.expression;
import static io.trino.plugin.pinot.query.PinotPatterns.expressionType;
import static io.trino.plugin.pinot.query.PinotPatterns.filter;
import static io.trino.plugin.pinot.query.PinotPatterns.filterPredicate;
import static io.trino.plugin.pinot.query.PinotPatterns.filterType;
import static io.trino.plugin.pinot.query.PinotPatterns.firstArgument;
import static io.trino.plugin.pinot.query.PinotPatterns.function;
import static io.trino.plugin.pinot.query.PinotPatterns.functionContext;
import static io.trino.plugin.pinot.query.PinotPatterns.identifier;
import static io.trino.plugin.pinot.query.PinotPatterns.predicate;
import static io.trino.plugin.pinot.query.PinotPatterns.predicateExpression;
import static io.trino.plugin.pinot.query.PinotPatterns.predicateType;
import static io.trino.plugin.pinot.query.PinotPatterns.predicateValuesList;
import static io.trino.plugin.pinot.query.PinotPatterns.secondArgument;
import static io.trino.plugin.pinot.query.PinotPatterns.singleInput;
import static io.trino.plugin.pinot.query.PinotPatterns.transformFunction;
import static io.trino.plugin.pinot.query.PinotPatterns.transformFunctionName;
import static io.trino.plugin.pinot.query.PinotPatterns.transformFunctionType;
import static io.trino.plugin.pinot.query.PinotTransformFunctionTypeResolver.getTransformFunctionType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.pinot.common.function.TransformFunctionType.CASE;
import static org.apache.pinot.common.function.TransformFunctionType.CAST;
import static org.apache.pinot.common.request.context.ExpressionContext.Type.IDENTIFIER;
import static org.apache.pinot.common.request.context.predicate.RangePredicate.UNBOUNDED;
import static org.apache.pinot.segment.spi.AggregationFunctionType.COUNT;
import static org.apache.pinot.segment.spi.AggregationFunctionType.getAggregationFunctionType;

public class PinotSqlFormatter
{
    private static final String MINUS = "minus";

    private static final List<Rule<FilterContext>> FILTER_RULES = ImmutableList.<Rule<FilterContext>>builder()
            .add(new AndOrFilterRule())
            .add(new PredicateFilterRule())
            .build();

    private static final List<Rule<Predicate>> GLOBAL_PREDICATE_RULES = ImmutableList.<Rule<Predicate>>builder()
            .add(new MinusZeroPredicateRule())
            .add(new BinaryOperatorPredicateRule())
            .build();

    private static final List<Rule<FunctionContext>> GLOBAL_FUNCTION_RULES = ImmutableList.of(new MinusFunctionRule());

    private static final Map<Predicate.Type, Rule<Predicate>> PREDICATE_RULE_MAP;
    private static final Map<TransformFunctionType, Rule<FunctionContext>> FUNCTION_RULE_MAP;
    private static final Map<AggregationFunctionType, Rule<FunctionContext>> AGGREGATION_FUNCTION_RULE_MAP;
    private static final Rule<FunctionContext> DEFAULT_FUNCTION_RULE = new DefaultFunctionRule();

    static {
        Map<Predicate.Type, Rule<Predicate>> predicateMap = new HashMap<>();
        predicateMap.put(Predicate.Type.IN, new ValuesListPredicateRule(Predicate.Type.IN, "IN"));
        predicateMap.put(Predicate.Type.NOT_IN, new ValuesListPredicateRule(Predicate.Type.NOT_IN, "NOT IN"));
        predicateMap.put(Predicate.Type.RANGE, new RangePredicateRule());
        predicateMap.put(Predicate.Type.REGEXP_LIKE, new BinaryFunctionPredicateRule(Predicate.Type.REGEXP_LIKE, "regexp_like"));
        predicateMap.put(Predicate.Type.TEXT_MATCH, new BinaryFunctionPredicateRule(Predicate.Type.TEXT_MATCH, "text_match"));
        predicateMap.put(Predicate.Type.JSON_MATCH, new BinaryFunctionPredicateRule(Predicate.Type.JSON_MATCH, "json_match"));
        predicateMap.put(Predicate.Type.IS_NULL, new ExpressionOnlyPredicate(Predicate.Type.IS_NULL, "IS NULL"));
        predicateMap.put(Predicate.Type.IS_NOT_NULL, new ExpressionOnlyPredicate(Predicate.Type.IS_NOT_NULL, "IS NOT NULL"));
        PREDICATE_RULE_MAP = immutableEnumMap(predicateMap);

        Map<TransformFunctionType, Rule<FunctionContext>> functionMap = new HashMap<>();
        functionMap.put(CASE, new CaseFunctionRule());
        functionMap.put(CAST, new CastFunctionRule());
        FUNCTION_RULE_MAP = immutableEnumMap(functionMap);

        Map<AggregationFunctionType, Rule<FunctionContext>> aggregationFunctionMap = new HashMap<>();
        aggregationFunctionMap.put(COUNT, new CountStarFunctionRule());
        AGGREGATION_FUNCTION_RULE_MAP = immutableEnumMap(aggregationFunctionMap);
    }

    private PinotSqlFormatter() {}

    public static String formatFilter(SchemaTableName schemaTableName, FilterContext filterContext, Map<String, ColumnHandle> columnHandles)
    {
        requireNonNull(filterContext, "filterContext is null");
        Context context = new Context() {
            @Override
            public SchemaTableName getSchemaTableName()
            {
                return schemaTableName;
            }

            @Override
            public Optional<Map<String, ColumnHandle>> getColumnHandles()
            {
                return Optional.of(columnHandles);
            }
        };
        return formatFilter(filterContext, context);
    }

    private static String formatFilter(FilterContext filterContext, Context context)
    {
        Optional<String> result = applyRules(FILTER_RULES, filterContext, context);
        if (result.isPresent()) {
            return result.get();
        }
        throw new PinotException(PINOT_INVALID_PQL_GENERATED, Optional.empty(), format("Unexpected filter type: '%s'", filterContext.getType()));
    }

    private static String formatPredicate(Predicate predicate, Context context)
    {
        Optional<String> result = applyRules(GLOBAL_PREDICATE_RULES, predicate, context);
        if (result.isPresent()) {
            return result.get();
        }
        Rule<Predicate> rule = PREDICATE_RULE_MAP.get(predicate.getType());
        if (rule != null) {
            result = applyRule(rule, predicate, context);
        }
        if (result.isPresent()) {
            return result.get();
        }
        throw new PinotException(PINOT_EXCEPTION, Optional.empty(), format("Unsupported predicate type '%s'", predicate.getType()));
    }

    public static String formatExpression(SchemaTableName schemaTableName, ExpressionContext expressionContext)
    {
        return formatExpression(schemaTableName, expressionContext, Optional.empty());
    }

    public static String formatExpression(SchemaTableName schemaTableName, ExpressionContext expressionContext, Optional<Map<String, ColumnHandle>> columnHandles)
    {
        requireNonNull(expressionContext, "expressionContext is null");
        Context context = new Context() {
            @Override
            public SchemaTableName getSchemaTableName()
            {
                return schemaTableName;
            }

            @Override
            public Optional<Map<String, ColumnHandle>> getColumnHandles()
            {
                return columnHandles;
            }
        };
        return formatExpression(expressionContext, context);
    }

    private static String formatExpression(ExpressionContext expressionContext, Context context)
    {
        switch (expressionContext.getType()) {
            case LITERAL:
                return singleQuoteValue(expressionContext.getLiteral().getValue().toString());
            case IDENTIFIER:
                if (context.getColumnHandles().isPresent()) {
                    return quoteIdentifier(getColumnHandle(expressionContext.getIdentifier(), context.getSchemaTableName(), context.getColumnHandles().get()).getColumnName());
                }
                return quoteIdentifier(expressionContext.getIdentifier());
            case FUNCTION:
                return formatFunction(expressionContext.getFunction(), context);
        }
        throw new PinotException(PINOT_EXCEPTION, Optional.empty(), format("Unsupported expression type '%s'", expressionContext.getType()));
    }

    private static String formatFunction(FunctionContext functionContext, Context context)
    {
        Optional<String> result = Optional.empty();
        if (functionContext.getType() == FunctionContext.Type.TRANSFORM) {
            Rule<FunctionContext> rule = FUNCTION_RULE_MAP.get(getTransformFunctionType(functionContext).orElseThrow());

            if (rule != null) {
                result = applyRule(rule, functionContext, context);
            }
            else {
                result = applyRules(GLOBAL_FUNCTION_RULES, functionContext, context);
            }
        }
        else {
            checkState(functionContext.getType() == FunctionContext.Type.AGGREGATION, "Unexpected function type for '%s'", functionContext);
            Rule<FunctionContext> rule = AGGREGATION_FUNCTION_RULE_MAP.get(getAggregationFunctionType(functionContext.getFunctionName()));
            if (rule != null) {
                result = applyRule(rule, functionContext, context);
            }
        }
        if (result.isPresent()) {
            return result.get();
        }
        result = applyRule(DEFAULT_FUNCTION_RULE, functionContext, context);
        if (result.isPresent()) {
            return result.get();
        }
        throw new PinotException(PINOT_EXCEPTION, Optional.empty(), format("Unsupported function expression '%s'", functionContext));
    }

    private static <T> Optional<String> applyRule(Rule<T> rule, T object, Context context)
    {
        Iterator<Match> iterator = rule.getPattern().match(object).iterator();
        while (iterator.hasNext()) {
            Match match = iterator.next();
            return Optional.of(rule.formatToSql(object, match.captures(), context));
        }
        return Optional.empty();
    }

    private static <T> Optional<String> applyRules(List<Rule<T>> rules, T object, Context context)
    {
        Optional<String> result = Optional.empty();
        for (Rule<T> rule : rules) {
            result = applyRule(rule, object, context);
            if (result.isPresent()) {
                break;
            }
        }
        return result;
    }

    private static String singleQuoteValue(String value)
    {
        return "'" + value.replaceAll("'", "''") + "'";
    }

    private static String singleQuoteValues(List<String> values)
    {
        return values.stream()
                .map(PinotSqlFormatter::singleQuoteValue)
                .collect(joining(", "));
    }

    public static String stripQuotes(String value)
    {
        if (value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    public static PinotColumnHandle getColumnHandle(String name, SchemaTableName schemaTableName, Map<String, ColumnHandle> columnHandles)
    {
        PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(name);
        if (columnHandle == null) {
            throw new ColumnNotFoundException(schemaTableName, name);
        }
        return columnHandle;
    }

    private interface Context
    {
        SchemaTableName getSchemaTableName();

        Optional<Map<String, ColumnHandle>> getColumnHandles();
    }

    private interface Rule<T>
    {
        Pattern<T> getPattern();

        String formatToSql(T object, Captures captures, Context context);
    }

    private static class AndOrFilterRule
            implements Rule<FilterContext>
    {
        private static final Capture<FilterContext.Type> FILTER_TYPE = newCapture();
        private static final Capture<List<FilterContext>> CHILD_FILTERS = newCapture();

        private static final Pattern<FilterContext> PATTERN = filter()
                .with(filterType().matching(contextType -> contextType == FilterContext.Type.AND || contextType == FilterContext.Type.OR))
                .with(filterType().capturedAs(FILTER_TYPE))
                .with(childFilters().capturedAs(CHILD_FILTERS));

        @Override
        public Pattern<FilterContext> getPattern()
        {
            return PATTERN;
        }

        @Override
        public String formatToSql(FilterContext object, Captures captures, Context context)
        {
            FilterContext.Type filterType = captures.get(FILTER_TYPE);
            List<FilterContext> childFilters = captures.get(CHILD_FILTERS);
            return format("%s(%s)", filterType.name(), childFilters.stream()
                    .map(filterContext -> formatFilter(filterContext, context))
                    .collect(joining(", ")));
        }
    }

    private static class PredicateFilterRule
            implements Rule<FilterContext>
    {
        private static final Capture<Predicate> PREDICATE = newCapture();
        private static final Pattern<FilterContext> PATTERN = filter()
                .with(filterPredicate().capturedAs(PREDICATE));

        @Override
        public Pattern<FilterContext> getPattern()
        {
            return PATTERN;
        }

        @Override
        public String formatToSql(FilterContext object, Captures captures, Context context)
        {
            Predicate predicate = captures.get(PREDICATE);
            return formatPredicate(predicate, context);
        }
    }

    // Pinot parses predicates like <identifier | function> [=|!=|>|<|>=|<=] <identifier | function>
    // as equals(minus(x, y), 0) which is not valid pql or valid pinot sql.
    // These patterns need to be rewritten to x op y here.
    private static class MinusZeroPredicateRule
            implements Rule<Predicate>
    {
        private static final Capture<ExpressionContext> FIRST_ARGUMENT = newCapture();
        private static final Capture<ExpressionContext> SECOND_ARGUMENT = newCapture();
        private static final Capture<String> BINARY_OPERATOR_NAME = newCapture();
        private static final Pattern<Predicate> PATTERN = predicate()
                .with(binaryOperatorValue().equalTo("0"))
                .with(binaryOperator().capturedAs(BINARY_OPERATOR_NAME))
                .with(predicateExpression().matching(expression()
                        .with(functionContext().matching(binaryFunction()
                                .with(firstArgument().capturedAs(FIRST_ARGUMENT))
                                .with(secondArgument().capturedAs(SECOND_ARGUMENT))
                                .with(transformFunctionName().matching(MINUS::equalsIgnoreCase))))));

        @Override
        public Pattern<Predicate> getPattern()
        {
            return PATTERN;
        }

        @Override
        public String formatToSql(Predicate object, Captures captures, Context context)
        {
            ExpressionContext first = captures.get(FIRST_ARGUMENT);
            ExpressionContext second = captures.get(SECOND_ARGUMENT);
            String operator = captures.get(BINARY_OPERATOR_NAME);
            return format("(%s) %s (%s)", formatExpression(first, context), operator, formatExpression(second, context));
        }
    }

    private static class BinaryOperatorPredicateRule
            implements Rule<Predicate>
    {
        private static final Capture<String> BINARY_OPERATOR_NAME = newCapture();
        private static final Capture<String> BINARY_OPERATOR_VALUE = newCapture();
        private static final Capture<ExpressionContext> PREDICATE_EXPRESSION = newCapture();
        private static final Pattern<Predicate> PATTERN = predicate()
                .with(binaryOperatorValue().capturedAs(BINARY_OPERATOR_VALUE))
                .with(binaryOperator().capturedAs(BINARY_OPERATOR_NAME))
                .with(predicateExpression().capturedAs(PREDICATE_EXPRESSION));

        @Override
        public Pattern<Predicate> getPattern()
        {
            return PATTERN;
        }

        @Override
        public String formatToSql(Predicate object, Captures captures, Context context)
        {
            ExpressionContext predicateExpression = captures.get(PREDICATE_EXPRESSION);
            String singleValue = captures.get(BINARY_OPERATOR_VALUE);
            String operator = captures.get(BINARY_OPERATOR_NAME);
            return format("(%s) %s %s", formatExpression(predicateExpression, context), operator, singleQuoteValue(singleValue));
        }
    }

    private static class ValuesListPredicateRule
            implements Rule<Predicate>
    {
        private static final Capture<List<String>> VALUES_LIST = newCapture();
        private static final Capture<ExpressionContext> PREDICATE_EXPRESSION = newCapture();
        private static final Pattern<Predicate> VALUES_LIST_PATTERN = predicate()
                .with(predicateValuesList().capturedAs(VALUES_LIST))
                .with(predicateExpression().capturedAs(PREDICATE_EXPRESSION));

        private final Pattern<Predicate> pattern;
        private final String operator;

        public ValuesListPredicateRule(Predicate.Type predicateType, String operator)
        {
            requireNonNull(predicateType, "predicateType is null");
            this.operator = requireNonNull(operator, "operator is null");
            pattern = VALUES_LIST_PATTERN.with(predicateType().equalTo(predicateType));
        }

        @Override
        public Pattern<Predicate> getPattern()
        {
            return pattern;
        }

        @Override
        public String formatToSql(Predicate object, Captures captures, Context context)
        {
            ExpressionContext predicateExpression = captures.get(PREDICATE_EXPRESSION);
            List<String> values = captures.get(VALUES_LIST);
            return format("%s %s (%s)", formatExpression(predicateExpression, context), operator, singleQuoteValues(values));
        }
    }

    private static class RangePredicateRule
            implements Rule<Predicate>
    {
        private static final Capture<ExpressionContext> PREDICATE_EXPRESSION = newCapture();

        private static final Pattern<Predicate> PATTERN = predicate()
                .with(predicateType().equalTo(Predicate.Type.RANGE))
                .with(predicateExpression().capturedAs(PREDICATE_EXPRESSION));

        @Override
        public Pattern<Predicate> getPattern()
        {
            return PATTERN;
        }

        @Override
        public String formatToSql(Predicate object, Captures captures, Context context)
        {
            RangePredicate rangePredicate = (RangePredicate) object;
            ExpressionContext predicateExpression = captures.get(PREDICATE_EXPRESSION);
            String expression = formatExpression(predicateExpression, context);

            // Single value range should have been rewritten in formatBinaryOperatorPredicate
            checkState(!rangePredicate.getLowerBound().equals(UNBOUNDED) && !rangePredicate.getUpperBound().equals(UNBOUNDED), "Unexpected range predicate '%s'", rangePredicate);
            if (rangePredicate.isUpperInclusive() && rangePredicate.isLowerInclusive()) {
                return format("(%s) BETWEEN %s AND %s", expression, singleQuoteValue(rangePredicate.getLowerBound()), singleQuoteValue(rangePredicate.getUpperBound()));
            }
            String leftOperator = rangePredicate.isLowerInclusive() ? ">=" : ">";
            String rightOperator = rangePredicate.isUpperInclusive() ? "<=" : "<";
            return format("(%1$s) %2$s %3$s AND (%1$s) %4$s %5$s", expression, leftOperator, singleQuoteValue(rangePredicate.getLowerBound()), rightOperator, singleQuoteValue(rangePredicate.getUpperBound()));
        }
    }

    private static class BinaryFunctionPredicateRule
            implements Rule<Predicate>
    {
        private static final Capture<String> BINARY_FUNCTION_VALUE = newCapture();
        private static final Capture<ExpressionContext> PREDICATE_EXPRESSION = newCapture();
        private static final Pattern<Predicate> BINARY_FUNCTION_PREDICATE = predicate()
                .with(binaryFunctionPredicateValue().capturedAs(BINARY_FUNCTION_VALUE))
                .with(predicateExpression().capturedAs(PREDICATE_EXPRESSION));

        private final Pattern<Predicate> pattern;
        private final String functionName;

        public BinaryFunctionPredicateRule(Predicate.Type predicateType, String functionName)
        {
            requireNonNull(predicateType, "predicateType is null");
            this.functionName = requireNonNull(functionName, "functionName is null");
            this.pattern = BINARY_FUNCTION_PREDICATE.with(predicateType().equalTo(predicateType));
        }

        @Override
        public Pattern<Predicate> getPattern()
        {
            return pattern;
        }

        @Override
        public String formatToSql(Predicate object, Captures captures, Context context)
        {
            String value = captures.get(BINARY_FUNCTION_VALUE);
            ExpressionContext predicateExpression = captures.get(PREDICATE_EXPRESSION);
            return format("%s(%s, %s)", functionName, formatExpression(predicateExpression, context), singleQuoteValue(value));
        }
    }

    private static class ExpressionOnlyPredicate
            implements Rule<Predicate>
    {
        private static final Capture<ExpressionContext> PREDICATE_EXPRESSION = newCapture();
        private static final Pattern<Predicate> PREDICATE_PATTERN = predicate()
                .with(predicateExpression().capturedAs(PREDICATE_EXPRESSION));

        private final Pattern<Predicate> pattern;
        private final String operator;

        public ExpressionOnlyPredicate(Predicate.Type predicateType, String operator)
        {
            requireNonNull(predicateType, "predicateType is null");
            this.operator = requireNonNull(operator, "operator is null");
            this.pattern = PREDICATE_PATTERN.with(predicateType().equalTo(predicateType));
        }

        @Override
        public Pattern<Predicate> getPattern()
        {
            return pattern;
        }

        @Override
        public String formatToSql(Predicate object, Captures captures, Context context)
        {
            ExpressionContext predicateExpression = captures.get(PREDICATE_EXPRESSION);
            return format("%s %s", formatExpression(predicateExpression, context), operator);
        }
    }

    // This is necessary because pinot renders <identifier | function> - <identifier - function>
    // as minus(x, y) which is valid pql for the broker but not valid sql for the pinot parser.
    private static class MinusFunctionRule
            implements Rule<FunctionContext>
    {
        private static final Capture<ExpressionContext> FIRST_ARGUMENT = newCapture();
        private static final Capture<ExpressionContext> SECOND_ARGUMENT = newCapture();
        private static final Pattern<FunctionContext> PATTERN = binaryFunction()
                .with(transformFunctionName().matching(MINUS::equalsIgnoreCase))
                .with(firstArgument().capturedAs(FIRST_ARGUMENT))
                .with(secondArgument().capturedAs(SECOND_ARGUMENT));

        @Override
        public Pattern<FunctionContext> getPattern()
        {
            return PATTERN;
        }

        @Override
        public String formatToSql(FunctionContext object, Captures captures, Context context)
        {
            ExpressionContext first = captures.get(FIRST_ARGUMENT);
            ExpressionContext second = captures.get(SECOND_ARGUMENT);
            return format("%s - %s", formatExpression(first, context), formatExpression(second, context));
        }
    }

    // Pinot parses cast as a function with the second argument being a literal instead of a type
    // The broker request parses it this way, so the reverse needs to be done here
    private static class CastFunctionRule
            implements Rule<FunctionContext>
    {
        private static final Capture<ExpressionContext> FIRST_ARGUMENT = newCapture();
        private static final Capture<ExpressionContext> SECOND_ARGUMENT = newCapture();
        private static final Pattern<FunctionContext> PATTERN = binaryFunction()
                .with(transformFunctionType().equalTo(CAST))
                .with(firstArgument().capturedAs(FIRST_ARGUMENT))
                .with(secondArgument().capturedAs(SECOND_ARGUMENT));

        @Override
        public Pattern<FunctionContext> getPattern()
        {
            return PATTERN;
        }

        @Override
        public String formatToSql(FunctionContext object, Captures captures, Context context)
        {
            ExpressionContext first = captures.get(FIRST_ARGUMENT);
            // Pinot interprets the second argument as a literal instead of a type
            ExpressionContext second = captures.get(SECOND_ARGUMENT);
            return format("CAST(%s AS %s)", formatExpression(first, context), stripQuotes(formatExpression(second, context)));
        }
    }

    // Pinot parses case statements as a function case(<when conditions>,... , <then statements>,... , <else condition>)
    // This is valid pql for the pinot broker but not valid sql for the pinot sql parser, so this needs to be rewritten here.
    private static class CaseFunctionRule
            implements Rule<FunctionContext>
    {
        private static final Capture<List<ExpressionContext>> ARGUMENTS = newCapture();
        private static final Pattern<FunctionContext> PATTERN = transformFunction()
                .with(transformFunctionType().equalTo(CASE))
                .with(arguments().capturedAs(ARGUMENTS));

        @Override
        public Pattern<FunctionContext> getPattern()
        {
            return PATTERN;
        }

        @Override
        public String formatToSql(FunctionContext object, Captures captures, Context context)
        {
            List<String> arguments = captures.get(ARGUMENTS).stream()
                    .map(expressionContext -> formatExpression(expressionContext, context))
                    .collect(toImmutableList());
            checkState(arguments.size() >= 2, "Unexpected expression '%s'", object);
            int whenStatements = arguments.size() / 2;
            StringBuilder builder = new StringBuilder("CASE ");
            builder.append("WHEN ")
                    .append(arguments.get(0))
                    .append(" THEN ")
                    .append(arguments.get(whenStatements));

            for (int index = 1; index < whenStatements; index++) {
                builder.append(" WHEN ")
                        .append(arguments.get(index))
                        .append(" THEN ")
                        .append(arguments.get(index + whenStatements));
            }

            if (arguments.size() % 2 != 0) {
                builder.append(" ELSE ")
                        .append(arguments.get(arguments.size() - 1));
            }
            return builder.append(" END").toString();
        }
    }

    private static class CountStarFunctionRule
            implements Rule<FunctionContext>
    {
        private static final Pattern<FunctionContext> PATTERN = aggregationFunction()
                .with(aggregationFunctionType().equalTo(COUNT))
                .with(singleInput().matching(expression()
                        .with(expressionType().equalTo(IDENTIFIER))
                        .with(identifier().equalTo(WILDCARD))));

        @Override
        public Pattern<FunctionContext> getPattern()
        {
            return PATTERN;
        }

        @Override
        public String formatToSql(FunctionContext object, Captures captures, Context context)
        {
            return format("%s(%s)", object.getFunctionName(), WILDCARD);
        }
    }

    private static class DefaultFunctionRule
            implements Rule<FunctionContext>
    {
        private static final Capture<List<ExpressionContext>> ARGUMENTS = newCapture();
        private static final Pattern<FunctionContext> PATTERN = function()
                .with(arguments().capturedAs(ARGUMENTS));

        @Override
        public Pattern<FunctionContext> getPattern()
        {
            return PATTERN;
        }

        @Override
        public String formatToSql(FunctionContext object, Captures captures, Context context)
        {
            return format("%s(%s)", object.getFunctionName(), captures.get(ARGUMENTS).stream()
                    .map(expressionContext -> formatExpression(expressionContext, context))
                    .collect(joining(", ")));
        }
    }
}
