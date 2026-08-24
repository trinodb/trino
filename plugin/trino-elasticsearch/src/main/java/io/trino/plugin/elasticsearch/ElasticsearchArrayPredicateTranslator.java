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
package io.trino.plugin.elasticsearch;

import io.trino.plugin.elasticsearch.client.IndexMetadata.DateTimeType;
import io.trino.plugin.elasticsearch.client.IndexMetadata.PrimitiveType;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Lambda;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;

/**
 * Exact Elasticsearch membership pushdown for primitive arrays.
 */
final class ElasticsearchArrayPredicateTranslator
{
    private ElasticsearchArrayPredicateTranslator() {}

    public static Optional<ElasticsearchRemotePredicate> translate(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments)
    {
        if (!(expression instanceof Call call)) {
            return Optional.empty();
        }

        return switch (call.getFunctionName().getName()) {
            case "contains" -> translateContains(call, assignments);
            case "arrays_overlap" -> translateArraysOverlap(call, assignments);
            case "any_match" -> translateAnyMatch(call, assignments);
            default -> Optional.empty();
        };
    }

    private static Optional<ElasticsearchRemotePredicate> translateContains(Call call, Map<String, ColumnHandle> assignments)
    {
        if (call.getArguments().size() != 2
                || !(call.getArguments().get(0) instanceof Variable variable)
                || !(call.getArguments().get(1) instanceof Constant constant)
                || constant.getValue() == null) {
            return Optional.empty();
        }

        ElasticsearchColumnHandle column = column(assignments, variable);
        Optional<Type> elementType = exactArrayElementType(column);
        if (elementType.isEmpty() || !constant.getType().equals(elementType.orElseThrow())) {
            return Optional.empty();
        }

        return Optional.of(new ElasticsearchRemotePredicate.Term(
                column.predicateName(),
                ElasticsearchRemotePredicateTranslator.getValue(elementType.orElseThrow(), constant.getValue())));
    }

    private static Optional<ElasticsearchRemotePredicate> translateArraysOverlap(Call call, Map<String, ColumnHandle> assignments)
    {
        if (call.getArguments().size() != 2
                || !(call.getArguments().get(0) instanceof Variable variable)) {
            return Optional.empty();
        }

        ElasticsearchColumnHandle column = column(assignments, variable);
        Optional<Type> elementType = exactArrayElementType(column);
        if (elementType.isEmpty()) {
            return Optional.empty();
        }

        return translateConstantArray(call.getArguments().get(1), elementType.orElseThrow())
                .map(values -> values.size() == 1
                        ? new ElasticsearchRemotePredicate.Term(column.predicateName(), values.getFirst())
                        : new ElasticsearchRemotePredicate.Terms(column.predicateName(), values));
    }

    private static Optional<ElasticsearchRemotePredicate> translateAnyMatch(Call call, Map<String, ColumnHandle> assignments)
    {
        if (call.getArguments().size() != 2
                || !(call.getArguments().get(0) instanceof Variable arrayVariable)
                || !(call.getArguments().get(1) instanceof Lambda lambda)
                || lambda.getArguments().size() != 1) {
            return Optional.empty();
        }

        ElasticsearchColumnHandle column = column(assignments, arrayVariable);
        Optional<Type> elementType = exactArrayElementType(column);
        if (elementType.isEmpty()) {
            return Optional.empty();
        }

        Variable lambdaVariable = lambda.getArguments().getFirst();
        if (!lambdaVariable.getType().equals(elementType.orElseThrow())) {
            return Optional.empty();
        }

        return translateAnyMatchBody(lambda.getBody(), lambdaVariable, column, elementType.orElseThrow());
    }

    private static Optional<ElasticsearchRemotePredicate> translateAnyMatchBody(
            ConnectorExpression expression,
            Variable lambdaVariable,
            ElasticsearchColumnHandle column,
            Type elementType)
    {
        if (!(expression instanceof Call call)) {
            return Optional.empty();
        }

        if (EQUAL_OPERATOR_FUNCTION_NAME.equals(call.getFunctionName())) {
            return translateAnyMatchEquality(call, lambdaVariable, column, elementType);
        }
        if (IN_PREDICATE_FUNCTION_NAME.equals(call.getFunctionName())) {
            return translateAnyMatchIn(call, lambdaVariable, column, elementType);
        }
        if (LESS_THAN_OPERATOR_FUNCTION_NAME.equals(call.getFunctionName())
                || LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(call.getFunctionName())
                || GREATER_THAN_OPERATOR_FUNCTION_NAME.equals(call.getFunctionName())
                || GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(call.getFunctionName())) {
            return translateAnyMatchRange(call, lambdaVariable, column, elementType);
        }
        if (OR_FUNCTION_NAME.equals(call.getFunctionName())) {
            return translateAnyMatchOr(call, lambdaVariable, column, elementType);
        }
        if (AND_FUNCTION_NAME.equals(call.getFunctionName())) {
            return translateAnyMatchAnd(call, lambdaVariable, column, elementType);
        }
        return Optional.empty();
    }

    private static Optional<ElasticsearchRemotePredicate> translateAnyMatchEquality(
            Call call,
            Variable lambdaVariable,
            ElasticsearchColumnHandle column,
            Type elementType)
    {
        if (call.getArguments().size() != 2) {
            return Optional.empty();
        }

        for (int variableIndex = 0; variableIndex < 2; variableIndex++) {
            if (isLambdaVariable(call.getArguments().get(variableIndex), lambdaVariable)
                    && call.getArguments().get(1 - variableIndex) instanceof Constant constant
                    && constant.getValue() != null
                    && constant.getType().equals(elementType)) {
                return Optional.of(new ElasticsearchRemotePredicate.Term(
                        column.predicateName(),
                        ElasticsearchRemotePredicateTranslator.getValue(elementType, constant.getValue())));
            }
        }
        return Optional.empty();
    }

    private static Optional<ElasticsearchRemotePredicate> translateAnyMatchIn(
            Call call,
            Variable lambdaVariable,
            ElasticsearchColumnHandle column,
            Type elementType)
    {
        if (call.getArguments().size() != 2 || !isLambdaVariable(call.getArguments().get(0), lambdaVariable)) {
            return Optional.empty();
        }

        return translateConstantArray(call.getArguments().get(1), elementType)
                .map(values -> values.size() == 1
                        ? new ElasticsearchRemotePredicate.Term(column.predicateName(), values.getFirst())
                        : new ElasticsearchRemotePredicate.Terms(column.predicateName(), values));
    }

    private static Optional<ElasticsearchRemotePredicate> translateAnyMatchRange(
            Call call,
            Variable lambdaVariable,
            ElasticsearchColumnHandle column,
            Type elementType)
    {
        if (!supportsExactRange(elementType) || call.getArguments().size() != 2) {
            return Optional.empty();
        }

        boolean variableOnLeft = isLambdaVariable(call.getArguments().get(0), lambdaVariable);
        boolean variableOnRight = isLambdaVariable(call.getArguments().get(1), lambdaVariable);
        if (variableOnLeft == variableOnRight) {
            return Optional.empty();
        }

        ConnectorExpression constantExpression = call.getArguments().get(variableOnLeft ? 1 : 0);
        if (!(constantExpression instanceof Constant constant)
                || constant.getValue() == null
                || !constant.getType().equals(elementType)) {
            return Optional.empty();
        }

        Object value = ElasticsearchRemotePredicateTranslator.getValue(elementType, constant.getValue());
        boolean lessThan = LESS_THAN_OPERATOR_FUNCTION_NAME.equals(call.getFunctionName());
        boolean lessThanOrEqual = LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(call.getFunctionName());
        boolean greaterThan = GREATER_THAN_OPERATOR_FUNCTION_NAME.equals(call.getFunctionName());
        boolean inclusive = lessThanOrEqual || GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(call.getFunctionName());

        boolean upperBound = variableOnLeft ? (lessThan || lessThanOrEqual) : (greaterThan || GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(call.getFunctionName()));
        ElasticsearchRemotePredicate.Bound bound = new ElasticsearchRemotePredicate.Bound(value, inclusive);
        return Optional.of(new ElasticsearchRemotePredicate.Range(
                column.predicateName(),
                upperBound ? Optional.empty() : Optional.of(bound),
                upperBound ? Optional.of(bound) : Optional.empty()));
    }

    private static Optional<ElasticsearchRemotePredicate> translateAnyMatchOr(
            Call call,
            Variable lambdaVariable,
            ElasticsearchColumnHandle column,
            Type elementType)
    {
        if (call.getArguments().isEmpty()) {
            return Optional.empty();
        }

        List<ElasticsearchRemotePredicate> predicates = new ArrayList<>(call.getArguments().size());
        for (ConnectorExpression argument : call.getArguments()) {
            Optional<ElasticsearchRemotePredicate> translated = translateAnyMatchBody(argument, lambdaVariable, column, elementType);
            if (translated.isEmpty()) {
                return Optional.empty();
            }
            predicates.add(translated.orElseThrow());
        }
        if (predicates.size() == 1) {
            return Optional.of(predicates.getFirst());
        }
        return Optional.of(new ElasticsearchRemotePredicate.Or(predicates));
    }

    private static Optional<ElasticsearchRemotePredicate> translateAnyMatchAnd(
            Call call,
            Variable lambdaVariable,
            ElasticsearchColumnHandle column,
            Type elementType)
    {
        if (call.getArguments().isEmpty()) {
            return Optional.empty();
        }

        Optional<ElasticsearchRemotePredicate.Bound> lower = Optional.empty();
        Optional<ElasticsearchRemotePredicate.Bound> upper = Optional.empty();
        for (ConnectorExpression argument : call.getArguments()) {
            Optional<ElasticsearchRemotePredicate> translated = translateAnyMatchBody(argument, lambdaVariable, column, elementType);
            if (translated.isEmpty() || !(translated.orElseThrow() instanceof ElasticsearchRemotePredicate.Range range)) {
                return Optional.empty();
            }
            if (range.lower().isPresent()) {
                if (lower.isPresent()) {
                    return Optional.empty();
                }
                lower = range.lower();
            }
            if (range.upper().isPresent()) {
                if (upper.isPresent()) {
                    return Optional.empty();
                }
                upper = range.upper();
            }
        }
        if (lower.isEmpty() && upper.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ElasticsearchRemotePredicate.Range(column.predicateName(), lower, upper));
    }

    private static Optional<List<Object>> translateConstantArray(ConnectorExpression expression, Type elementType)
    {
        if (expression instanceof Constant constant
                && constant.getType() instanceof ArrayType constantArrayType
                && constantArrayType.getElementType().equals(elementType)
                && constant.getValue() instanceof Block values) {
            if (values.getPositionCount() == 0) {
                return Optional.empty();
            }
            List<Object> translatedValues = new ArrayList<>(values.getPositionCount());
            for (int position = 0; position < values.getPositionCount(); position++) {
                if (values.isNull(position)) {
                    return Optional.empty();
                }
                translatedValues.add(ElasticsearchRemotePredicateTranslator.getValue(
                        elementType,
                        readNativeValue(elementType, values, position)));
            }
            return Optional.of(translatedValues);
        }

        if (expression instanceof Call arrayConstructor
                && ARRAY_CONSTRUCTOR_FUNCTION_NAME.equals(arrayConstructor.getFunctionName())
                && !arrayConstructor.getArguments().isEmpty()) {
            List<Object> translatedValues = new ArrayList<>(arrayConstructor.getArguments().size());
            for (ConnectorExpression argument : arrayConstructor.getArguments()) {
                if (!(argument instanceof Constant constant)
                        || constant.getValue() == null
                        || !constant.getType().equals(elementType)) {
                    return Optional.empty();
                }
                translatedValues.add(ElasticsearchRemotePredicateTranslator.getValue(elementType, constant.getValue()));
            }
            return Optional.of(translatedValues);
        }
        return Optional.empty();
    }

    private static boolean isLambdaVariable(ConnectorExpression expression, Variable lambdaVariable)
    {
        return expression instanceof Variable variable && variable.equals(lambdaVariable);
    }

    private static boolean supportsExactRange(Type elementType)
    {
        return elementType.equals(TINYINT)
                || elementType.equals(SMALLINT)
                || elementType.equals(INTEGER)
                || elementType.equals(BIGINT)
                || elementType.equals(REAL)
                || elementType.equals(DOUBLE)
                || elementType.equals(TIMESTAMP_MILLIS);
    }

    private static ElasticsearchColumnHandle column(Map<String, ColumnHandle> assignments, Variable variable)
    {
        ColumnHandle column = assignments.get(variable.getName());
        return column instanceof ElasticsearchColumnHandle elasticsearchColumn ? elasticsearchColumn : null;
    }

    private static Optional<Type> exactArrayElementType(ElasticsearchColumnHandle column)
    {
        if (column == null || !(column.type() instanceof ArrayType arrayType)) {
            return Optional.empty();
        }

        Type elementType = arrayType.getElementType();
        if (elementType.equals(TIMESTAMP_MILLIS)) {
            return column.elasticsearchType() instanceof DateTimeType ? Optional.of(elementType) : Optional.empty();
        }
        if (!(column.elasticsearchType() instanceof PrimitiveType primitiveType)) {
            return Optional.empty();
        }

        boolean supportedElementType = elementType.equals(TINYINT)
                || elementType.equals(SMALLINT)
                || elementType.equals(INTEGER)
                || elementType.equals(BIGINT)
                || elementType.equals(REAL)
                || elementType.equals(DOUBLE)
                || elementType.equals(BOOLEAN)
                || elementType instanceof VarcharType
                || elementType.getBaseName().equalsIgnoreCase("ipaddress");
        if (!supportedElementType) {
            return Optional.empty();
        }

        if (primitiveType.name().equalsIgnoreCase("text") && primitiveType.keyword().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(elementType);
    }
}
