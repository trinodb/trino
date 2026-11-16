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
package io.trino.plugin.pinot.query.expression;

import io.airlift.slice.Slice;
import io.trino.plugin.base.expression.ConnectorExpressionRule.RewriteContext;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.plugin.pinot.query.DynamicTablePqlExtractor.quoteIdentifier;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;

final class PinotExpressionRules
{
    private PinotExpressionRules() {}

    static Optional<String> columnReference(RewriteContext<String> context, Variable variable)
    {
        PinotColumnHandle column = (PinotColumnHandle) context.getAssignment(variable.getName());
        if (column.isAggregate()) {
            // Aggregate columns belong in the HAVING clause, which does not receive pushed down expressions
            return Optional.empty();
        }
        return Optional.of(quoteIdentifier(column.getColumnName()));
    }

    static boolean isSupportedLiteralType(Type type)
    {
        return type instanceof VarcharType || type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT);
    }

    /**
     * Formats a non-null constant of a supported type as a PQL literal.
     */
    static Optional<String> literal(Constant constant)
    {
        if (constant.getValue() == null || !isSupportedLiteralType(constant.getType())) {
            return Optional.empty();
        }
        return Optional.of(literal(constant.getType(), constant.getValue()));
    }

    static String literal(Type type, Object value)
    {
        if (type instanceof VarcharType) {
            return "'" + ((Slice) value).toStringUtf8().replace("'", "''") + "'";
        }
        return String.valueOf((long) value);
    }

    static String jsonPath(String path)
    {
        return "\"" + path.replace("\"", "\"\"") + "\"";
    }

    /**
     * Builds a {@code JSON_MATCH} call. The filter is itself a PQL string literal, so quotes are escaped.
     */
    static String jsonMatch(String column, String filter)
    {
        return "JSON_MATCH(%s, '%s')".formatted(column, filter.replace("'", "''"));
    }

    static Optional<String> pathOf(Constant path)
    {
        if (path.getValue() instanceof Slice slice) {
            return Optional.of(slice.toStringUtf8());
        }
        return Optional.empty();
    }
}
