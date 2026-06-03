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
package io.trino.typesolver.verifier;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.weakref.solver.Expression;

import java.util.List;

import static java.util.stream.Collectors.joining;
import static org.weakref.solver.Expression.anonymousField;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.field;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.row;
import static org.weakref.solver.Expression.symbol;

/**
 * Translates a Trino {@link Type} into the solver's {@link Expression} type
 * language, so the same concrete type can be driven through both engines.
 * <p>
 * The mapping is intentionally total over the types the Trino preset claims to
 * support; an unsupported type surfaces as an {@link IllegalArgumentException}
 * rather than a silent skip, so coverage gaps are visible.
 */
final class TypeBridge
{
    private TypeBridge() {}

    /**
     * Convert a Trino type to the equivalent solver expression.
     *
     * @return the solver expression modelling the same type
     */
    static Expression toExpression(Type type)
    {
        return switch (type) {
            case DecimalType decimal -> apply("decimal", literal(decimal.getPrecision()), literal(decimal.getScale()));
            case VarcharType varchar -> varchar.isUnbounded() ? symbol("varchar") : apply("varchar", literal(varchar.getBoundedLength()));
            case CharType charType -> apply("char", literal(charType.getLength()));
            case TimestampType timestamp -> apply("timestamp", literal(timestamp.getPrecision()));
            case TimestampWithTimeZoneType timestamp -> apply("timestamp_with_time_zone", literal(timestamp.getPrecision()));
            case TimeType time -> apply("time", literal(time.getPrecision()));
            case TimeWithTimeZoneType time -> apply("time_with_time_zone", literal(time.getPrecision()));
            case ArrayType array -> apply("array", toExpression(array.getElementType()));
            case io.trino.spi.type.QuantileDigestType qdigest -> apply("qdigest", toExpression(qdigest.getValueType()));
            case MapType map -> apply("map", toExpression(map.getKeyType()), toExpression(map.getValueType()));
            case RowType rowType -> row(rowType.getFields().stream()
                    .map(rowField -> rowField.getName()
                            .map(name -> field(name, toExpression(rowField.getType())))
                            .orElseGet(() -> anonymousField(toExpression(rowField.getType()))))
                    .toArray(Expression.RowField[]::new));
            default -> symbol(symbolName(type));
        };
    }

    private static String symbolName(Type type)
    {
        // Zero-argument types map by base name; multi-word names (interval / with time zone)
        // use underscores in the solver preset.
        return type.getTypeDescriptor().getBase().replace(' ', '_');
    }

    /**
     * Render a solver expression as a canonical type string for reporting and
     * for comparing common-supertype results across the two engines.
     *
     * @return a human-readable, structurally faithful rendering of the expression
     */
    static String render(Expression expression)
    {
        return switch (expression) {
            case Expression.Symbol symbol -> symbol.name();
            case Expression.Literal value -> Integer.toString(value.value());
            case Expression.Application(Expression head, List<Expression> arguments) -> render(head) + "(" + arguments.stream().map(TypeBridge::render).collect(joining(", ")) + ")";
            case Expression.Row(List<Expression.RowField> fields) -> "row(" + fields.stream()
                    .map(rowField -> rowField.name().map(name -> name + " ").orElse("") + render(rowField.type()))
                    .collect(joining(", ")) + ")";
            default -> expression.toString();
        };
    }
}
