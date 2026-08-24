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
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
                || !(call.getArguments().get(0) instanceof Variable variable)
                || !(call.getArguments().get(1) instanceof Constant constant)
                || !(constant.getType() instanceof ArrayType constantArrayType)
                || !(constant.getValue() instanceof Block values)) {
            return Optional.empty();
        }

        ElasticsearchColumnHandle column = column(assignments, variable);
        Optional<Type> elementType = exactArrayElementType(column);
        if (elementType.isEmpty() || !constantArrayType.getElementType().equals(elementType.orElseThrow()) || values.getPositionCount() == 0) {
            return Optional.empty();
        }

        List<Object> translatedValues = new ArrayList<>(values.getPositionCount());
        for (int position = 0; position < values.getPositionCount(); position++) {
            if (values.isNull(position)) {
                // A NULL element can make arrays_overlap indeterminate when there is no non-null match. Keep the
                // predicate in Trino rather than claiming exact remote semantics.
                return Optional.empty();
            }
            translatedValues.add(ElasticsearchRemotePredicateTranslator.getValue(
                    elementType.orElseThrow(),
                    readNativeValue(elementType.orElseThrow(), values, position)));
        }

        if (translatedValues.size() == 1) {
            return Optional.of(new ElasticsearchRemotePredicate.Term(column.predicateName(), translatedValues.getFirst()));
        }
        return Optional.of(new ElasticsearchRemotePredicate.Terms(column.predicateName(), translatedValues));
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
