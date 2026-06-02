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
package io.trino.lib;

import org.weakref.solver.Expression;
import org.weakref.solver.RequireComparable;
import org.weakref.solver.RequireOrderable;
import org.weakref.solver.TypeLibrary;
import org.weakref.solver.TypeScheme;

import java.util.List;

import static org.weakref.solver.Expression.BinaryOperator.ADD;
import static org.weakref.solver.Expression.BinaryOperator.MAX;
import static org.weakref.solver.Expression.BinaryOperator.MIN;
import static org.weakref.solver.Expression.BinaryOperator.SUBTRACT;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.function;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.operation;
import static org.weakref.solver.Expression.symbol;
import static org.weakref.solver.Expression.variable;
import static org.weakref.solver.Expression.variadicFunction;

public final class TrinoOperators
{
    private static final List<String> NUMERIC_TYPES = List.of("tinyint", "smallint", "integer", "bigint", "real", "double", "number");
    private static final List<String> ARITHMETIC_OPERATORS = List.of("+", "-", "*", "/", "%");
    private static final List<String> EQUALITY_OPERATORS = List.of("=", "<>");
    private static final List<String> ORDER_OPERATORS = List.of("<", "<=", ">", ">=");

    private TrinoOperators() {}

    public static TypeLibrary.Builder register(TypeLibrary.Builder builder)
    {
        for (String name : EQUALITY_OPERATORS) {
            builder.registerFunction(name, new TypeScheme(
                    List.of(variable("@T")),
                    List.of(new RequireComparable("@T")),
                    function(List.of(variable("@T"), variable("@T")), symbol("boolean"))));
        }
        for (String name : ORDER_OPERATORS) {
            builder.registerFunction(name, new TypeScheme(
                    List.of(variable("@T")),
                    List.of(new RequireOrderable("@T")),
                    function(List.of(variable("@T"), variable("@T")), symbol("boolean"))));
        }

        builder.registerFunction("and", function(List.of(symbol("boolean"), symbol("boolean")), symbol("boolean")));
        builder.registerFunction("or", function(List.of(symbol("boolean"), symbol("boolean")), symbol("boolean")));
        builder.registerFunction("not", function(List.of(symbol("boolean")), symbol("boolean")));

        for (String numericType : NUMERIC_TYPES) {
            for (String op : ARITHMETIC_OPERATORS) {
                builder.registerFunction(op, function(List.of(symbol(numericType), symbol(numericType)), symbol(numericType)));
            }
            // Unary negate for each numeric primitive.
            builder.registerFunction("-", function(List.of(symbol(numericType)), symbol(numericType)));
        }

        // Unary negate for decimal: returns the same decimal shape.
        builder.registerFunction("-", new TypeScheme(
                List.of(variable("@p"), variable("@s")),
                List.of(),
                function(
                        List.of(apply("decimal", variable("@p"), variable("@s"))),
                        apply("decimal", variable("@p"), variable("@s")))));

        // Decimal addition/subtraction: p = min(38, max(p1-s1, p2-s2) + max(s1, s2) + 1); s = max(s1, s2).
        for (String op : List.of("+", "-")) {
            builder.registerFunction(op, decimalAdditiveScheme());
        }

        // Decimal multiplication:
        //   naturalPrecision = p1 + p2
        //   intDigits = naturalPrecision - (s1 + s2)
        //   precision = min(38, naturalPrecision)
        //   scale = min(s1 + s2, max(6, 38 - intDigits))
        builder.registerFunction("*", decimalMultiplyScheme());

        // Decimal division: s = max(6, s1 + p2 + 1); p = min(38, p1 - s1 + s2 + s).
        builder.registerFunction("/", decimalDivisionScheme());

        // Decimal modulo: s = max(s1, s2); p = min(p1 - s1, p2 - s2) + s.
        builder.registerFunction("%", decimalModuloScheme());

        // Varchar concatenation: n = n1 + n2.
        builder.registerFunction("||", new TypeScheme(
                List.of(variable("@n1"), variable("@n2")),
                List.of(),
                function(
                        List.of(
                                apply("varchar", variable("@n1")),
                                apply("varchar", variable("@n2"))),
                        apply("varchar", operation(ADD, variable("@n1"), variable("@n2"))))));

        // Char concatenation: n = n1 + n2.
        builder.registerFunction("||", new TypeScheme(
                List.of(variable("@n1"), variable("@n2")),
                List.of(),
                function(
                        List.of(
                                apply("char", variable("@n1")),
                                apply("char", variable("@n2"))),
                        apply("char", operation(ADD, variable("@n1"), variable("@n2"))))));

        builder.registerFunction("coalesce", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                variadicFunction(List.of(variable("@T")), variable("@T"), variable("@T"))));

        registerTemporalArithmetic(builder);
        registerSubscript(builder);

        return builder;
    }

    private static void registerSubscript(TypeLibrary.Builder builder)
    {
        // array subscript: a[i] → element type.
        builder.registerFunction("subscript", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(apply("array", variable("@T")), symbol("bigint")), variable("@T"))));

        // map subscript: m[k] → value type.
        builder.registerFunction("subscript", new TypeScheme(
                List.of(variable("@K"), variable("@V")),
                List.of(),
                function(List.of(apply("map", variable("@K"), variable("@V")), variable("@K")), variable("@V"))));
    }

    private static void registerTemporalArithmetic(TypeLibrary.Builder builder)
    {
        // date ± interval_year_to_month → date
        builder.registerFunction("+", function(List.of(symbol("date"), symbol("interval_year_to_month")), symbol("date")));
        builder.registerFunction("+", function(List.of(symbol("interval_year_to_month"), symbol("date")), symbol("date")));
        builder.registerFunction("-", function(List.of(symbol("date"), symbol("interval_year_to_month")), symbol("date")));

        // date ± interval_day_to_second → timestamp(3)  (approximation — promotes to timestamp to carry sub-day precision)
        builder.registerFunction("+", function(List.of(symbol("date"), symbol("interval_day_to_second")), apply("timestamp", literal(3))));
        builder.registerFunction("+", function(List.of(symbol("interval_day_to_second"), symbol("date")), apply("timestamp", literal(3))));
        builder.registerFunction("-", function(List.of(symbol("date"), symbol("interval_day_to_second")), apply("timestamp", literal(3))));

        // date - date → interval_day_to_second
        builder.registerFunction("-", function(List.of(symbol("date"), symbol("date")), symbol("interval_day_to_second")));

        // timestamp(p) ± interval_* → timestamp(p)
        for (String interval : List.of("interval_day_to_second", "interval_year_to_month")) {
            builder.registerFunction("+", new TypeScheme(
                    List.of(variable("@p")),
                    List.of(),
                    function(List.of(apply("timestamp", variable("@p")), symbol(interval)), apply("timestamp", variable("@p")))));
            builder.registerFunction("+", new TypeScheme(
                    List.of(variable("@p")),
                    List.of(),
                    function(List.of(symbol(interval), apply("timestamp", variable("@p"))), apply("timestamp", variable("@p")))));
            builder.registerFunction("-", new TypeScheme(
                    List.of(variable("@p")),
                    List.of(),
                    function(List.of(apply("timestamp", variable("@p")), symbol(interval)), apply("timestamp", variable("@p")))));
        }

        // timestamp(p1) - timestamp(p2) → interval_day_to_second
        builder.registerFunction("-", new TypeScheme(
                List.of(variable("@p1"), variable("@p2")),
                List.of(),
                function(List.of(apply("timestamp", variable("@p1")), apply("timestamp", variable("@p2"))), symbol("interval_day_to_second"))));

        // time(p) ± interval_day_to_second → time(p)
        builder.registerFunction("+", new TypeScheme(
                List.of(variable("@p")),
                List.of(),
                function(List.of(apply("time", variable("@p")), symbol("interval_day_to_second")), apply("time", variable("@p")))));
        builder.registerFunction("+", new TypeScheme(
                List.of(variable("@p")),
                List.of(),
                function(List.of(symbol("interval_day_to_second"), apply("time", variable("@p"))), apply("time", variable("@p")))));
        builder.registerFunction("-", new TypeScheme(
                List.of(variable("@p")),
                List.of(),
                function(List.of(apply("time", variable("@p")), symbol("interval_day_to_second")), apply("time", variable("@p")))));

        // time(p1) - time(p2) → interval_day_to_second
        builder.registerFunction("-", new TypeScheme(
                List.of(variable("@p1"), variable("@p2")),
                List.of(),
                function(List.of(apply("time", variable("@p1")), apply("time", variable("@p2"))), symbol("interval_day_to_second"))));

        // interval ± interval (same kind) → interval
        for (String interval : List.of("interval_day_to_second", "interval_year_to_month")) {
            builder.registerFunction("+", function(List.of(symbol(interval), symbol(interval)), symbol(interval)));
            builder.registerFunction("-", function(List.of(symbol(interval), symbol(interval)), symbol(interval)));

            // interval × bigint (both orders) / interval / bigint → interval
            builder.registerFunction("*", function(List.of(symbol(interval), symbol("bigint")), symbol(interval)));
            builder.registerFunction("*", function(List.of(symbol("bigint"), symbol(interval)), symbol(interval)));
            builder.registerFunction("/", function(List.of(symbol(interval), symbol("bigint")), symbol(interval)));

            // Unary negate for intervals.
            builder.registerFunction("-", function(List.of(symbol(interval)), symbol(interval)));
        }
    }

    private static TypeScheme decimalAdditiveScheme()
    {
        // maxScale = max(s1, s2); intDigits = max(p1-s1, p2-s2);
        // precision = min(38, maxScale + intDigits + 1);
        // scale = min(maxScale, precision - intDigits).
        Expression maxScale = operation(MAX, variable("@s1"), variable("@s2"));
        Expression intDigits = operation(
                MAX,
                operation(SUBTRACT, variable("@p1"), variable("@s1")),
                operation(SUBTRACT, variable("@p2"), variable("@s2")));
        Expression precision = operation(
                MIN,
                literal(38),
                operation(ADD, operation(ADD, maxScale, intDigits), literal(1)));
        return new TypeScheme(
                List.of(variable("@p1"), variable("@s1"), variable("@p2"), variable("@s2")),
                List.of(),
                function(
                        List.of(
                                apply("decimal", variable("@p1"), variable("@s1")),
                                apply("decimal", variable("@p2"), variable("@s2"))),
                        apply("decimal",
                                precision,
                                operation(MIN, maxScale, operation(SUBTRACT, precision, intDigits)))));
    }

    private static TypeScheme decimalMultiplyScheme()
    {
        Expression sumScale = operation(ADD, variable("@s1"), variable("@s2"));
        Expression naturalPrecision = operation(ADD, variable("@p1"), variable("@p2"));
        Expression intDigits = operation(SUBTRACT, naturalPrecision, sumScale);
        return new TypeScheme(
                List.of(variable("@p1"), variable("@s1"), variable("@p2"), variable("@s2")),
                List.of(),
                function(
                        List.of(
                                apply("decimal", variable("@p1"), variable("@s1")),
                                apply("decimal", variable("@p2"), variable("@s2"))),
                        apply("decimal",
                                operation(MIN, literal(38), naturalPrecision),
                                operation(
                                        MIN,
                                        sumScale,
                                        operation(MAX, literal(6), operation(SUBTRACT, literal(38), intDigits))))));
    }

    private static TypeScheme decimalDivisionScheme()
    {
        // naturalScale = max(6, s1 + p2 + 1)
        // integerDigits = p1 - s1 + s2
        // precision = min(38, integerDigits + naturalScale)
        // scale = min(naturalScale, max(6, 38 - integerDigits))
        Expression naturalScale = operation(
                MAX,
                literal(6),
                operation(ADD, operation(ADD, variable("@s1"), variable("@p2")), literal(1)));
        Expression integerDigits = operation(
                ADD,
                operation(SUBTRACT, variable("@p1"), variable("@s1")),
                variable("@s2"));
        return new TypeScheme(
                List.of(variable("@p1"), variable("@s1"), variable("@p2"), variable("@s2")),
                List.of(),
                function(
                        List.of(
                                apply("decimal", variable("@p1"), variable("@s1")),
                                apply("decimal", variable("@p2"), variable("@s2"))),
                        apply("decimal",
                                operation(MIN, literal(38), operation(ADD, integerDigits, naturalScale)),
                                operation(
                                        MIN,
                                        naturalScale,
                                        operation(MAX, literal(6), operation(SUBTRACT, literal(38), integerDigits))))));
    }

    private static TypeScheme decimalModuloScheme()
    {
        // scale = max(s1, s2)
        // precision = min(p1 - s1, p2 - s2) + scale
        Expression scale = operation(MAX, variable("@s1"), variable("@s2"));
        return new TypeScheme(
                List.of(variable("@p1"), variable("@s1"), variable("@p2"), variable("@s2")),
                List.of(),
                function(
                        List.of(
                                apply("decimal", variable("@p1"), variable("@s1")),
                                apply("decimal", variable("@p2"), variable("@s2"))),
                        apply("decimal",
                                operation(
                                        ADD,
                                        operation(
                                                MIN,
                                                operation(SUBTRACT, variable("@p1"), variable("@s1")),
                                                operation(SUBTRACT, variable("@p2"), variable("@s2"))),
                                        scale),
                                scale)));
    }
}
