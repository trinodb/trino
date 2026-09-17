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
package io.trino.typesolver;

import io.trino.spi.type.CharType;
import io.trino.spi.type.TemplateParameter;
import io.trino.spi.type.TypeTemplate.TypeApplication;

import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.type.TypeTemplates.argument;
import static io.trino.spi.type.TypeTemplates.numericVariable;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.typesolver.Expression.BinaryOperator.ADD;
import static io.trino.typesolver.Expression.BinaryOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.typesolver.Expression.BinaryOperator.LESS_THAN_OR_EQUAL;
import static io.trino.typesolver.Expression.BinaryOperator.MIN;
import static io.trino.typesolver.Expression.BinaryOperator.SUBTRACT;
import static io.trino.typesolver.Expression.apply;
import static io.trino.typesolver.Expression.literal;
import static io.trino.typesolver.Expression.operation;
import static io.trino.typesolver.Expression.symbol;
import static io.trino.typesolver.Expression.variable;
import static io.trino.typesolver.TypeConstructor.Trait.ABSENT;
import static io.trino.typesolver.TypeConstructor.Trait.PRESENT;
import static io.trino.typesolver.TypeConstructor.Trait.STRUCTURAL;
import static io.trino.typesolver.TypeConstructor.parametric;
import static io.trino.typesolver.TypeConstructor.primitive;

public final class TrinoPreset
{
    private static final List<TypeConstructor> TYPE_CONSTRUCTORS = List.of(
            primitive("unknown"),
            primitive("boolean"),
            primitive("tinyint"),
            primitive("smallint"),
            primitive("integer"),
            primitive("bigint"),
            primitive("real"),
            primitive("double"),
            parametric(
                    "decimal",
                    List.of(numericParameter("p"), numericParameter("s")),
                    List.of(
                            new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("p"), literal(1))),
                            new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("p"), literal(38))),
                            new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("s"), literal(0))),
                            new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("s"), variable("p"))))),
            primitive("number"),
            primitive("varchar"),
            parametric(
                    "varchar",
                    List.of(numericParameter("n")),
                    List.of(
                            new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("n"), literal(0))),
                            new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("n"), literal(Integer.MAX_VALUE))))),
            // The upper bound is checked when Trino materializes the descriptor. Keeping only the
            // lower bound here permits calculated return types during overload specificity probing.
            parametric(
                    "char",
                    List.of(numericParameter("n")),
                    List.of(
                            new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("n"), literal(0))))),
            primitive("varbinary"),
            primitive("date"),
            numericParameterType("time"),
            numericParameterType("time_with_time_zone"),
            numericParameterType("timestamp"),
            numericParameterType("timestamp_with_time_zone"),
            primitive("interval_day_to_second"),
            primitive("interval_year_to_month"),
            primitive("json", PRESENT, ABSENT),
            primitive("uuid"),
            primitive("ipaddress"),
            primitive("variant", PRESENT, ABSENT),
            primitive("hyperloglog", ABSENT, ABSENT),
            primitive("p4hyperloglog", ABSENT, ABSENT),
            primitive("tdigest", ABSENT, ABSENT),
            new TypeConstructor(new TypeApplication("qdigest", List.of(typeParameter("e"))), List.of(), false, ABSENT, ABSENT),
            primitive("setdigest", ABSENT, ABSENT),
            primitive("color", PRESENT, ABSENT),
            primitive("json2016", ABSENT, ABSENT),
            new TypeConstructor(new TypeApplication("map", List.of(typeParameter("k"), typeParameter("v"))), List.of(new RequireComparable("k")), false, STRUCTURAL, ABSENT),
            new TypeConstructor(new TypeApplication("array", List.of(typeParameter("e"))), List.of(), false, STRUCTURAL, STRUCTURAL),
            new TypeConstructor(new TypeApplication("row", List.of(typeParameter("T"))), List.of(), true, STRUCTURAL, STRUCTURAL));

    private TrinoPreset() {}

    public enum CharVarcharCoercion
    {
        SQL_STANDARD,
        LEGACY,
    }

    public static List<TypeConstructor> typeConstructors()
    {
        return TYPE_CONSTRUCTORS;
    }

    private static TypeConstructor numericParameterType(String name)
    {
        return parametric(name, List.of(numericParameter("p")), List.of());
    }

    private static TemplateParameter numericParameter(String name)
    {
        return argument(numericVariable(name));
    }

    private static TemplateParameter typeParameter(String name)
    {
        return argument(typeVariable(name));
    }

    public static List<CoercionRule> coercionRules()
    {
        return coercionRules(CharVarcharCoercion.SQL_STANDARD);
    }

    public static List<CoercionRule> coercionRules(CharVarcharCoercion charVarcharCoercion)
    {
        List<CoercionRule> rules = new ArrayList<>(List.of(
                new SelfCoercion(),
                new PatternCoercion(symbol("unknown"), variable("X"), List.of()),
                // Magic-literal argument types: a string argument implicitly compiles into the
                // form the function declares (TypeCoercion's special cases for varchar and char)
                new PatternCoercion(apply("varchar", variable("n")), symbol("joniregexp"), List.of()),
                new PatternCoercion(apply("varchar", variable("n")), symbol("re2jregexp"), List.of()),
                new PatternCoercion(apply("varchar", variable("n")), symbol("jsonpath"), List.of()),
                new PatternCoercion(apply("varchar", variable("n")), symbol("codepoints"), List.of()),
                new PatternCoercion(apply("char", variable("n")), symbol("joniregexp"), List.of()),
                new PatternCoercion(apply("char", variable("n")), symbol("re2jregexp"), List.of()),
                new PatternCoercion(apply("char", variable("n")), symbol("jsonpath"), List.of()),
                new PatternCoercion(apply("char", variable("n")), symbol("codepoints"), List.of()),
                new PatternCoercion(symbol("varchar"), symbol("joniregexp"), List.of()),
                new PatternCoercion(symbol("varchar"), symbol("re2jregexp"), List.of()),
                new PatternCoercion(symbol("varchar"), symbol("jsonpath"), List.of()),
                new PatternCoercion(symbol("varchar"), symbol("codepoints"), List.of()),
                new PrimitiveTypeCoercion("tinyint", "smallint"),
                new PrimitiveTypeCoercion("tinyint", "integer"),
                new PrimitiveTypeCoercion("tinyint", "bigint"),
                new PrimitiveTypeCoercion("tinyint", "real"),
                new PrimitiveTypeCoercion("tinyint", "double"),
                new PrimitiveTypeCoercion("smallint", "integer"),
                new PrimitiveTypeCoercion("smallint", "bigint"),
                new PrimitiveTypeCoercion("smallint", "real"),
                new PrimitiveTypeCoercion("smallint", "double"),
                new PrimitiveTypeCoercion("integer", "bigint"),
                new PrimitiveTypeCoercion("integer", "real"),
                new PrimitiveTypeCoercion("integer", "double"),
                new PrimitiveTypeCoercion("bigint", "real"),
                new PrimitiveTypeCoercion("bigint", "double"),
                new PrimitiveTypeCoercion("real", "double"),
                new PrimitiveTypeCoercion("p4hyperloglog", "hyperloglog"),
                new PrimitiveTypeCoercion("tinyint", "number"),
                new PrimitiveTypeCoercion("smallint", "number"),
                new PrimitiveTypeCoercion("integer", "number"),
                new PrimitiveTypeCoercion("bigint", "number"),
                new PatternCoercion(
                        apply("decimal", variable("p"), variable("s")),
                        symbol("number"),
                        List.of()),
                new ParametricTypeCovariantCoercion("row"),
                new ParametricTypeCovariantCoercion("array"),
                new ParametricTypeCovariantCoercion("map"),
                // Integral inputs use the same precision cap as decimal-to-decimal coercions.
                new PatternCoercion(
                        symbol("tinyint"),
                        apply("decimal", variable("p"), variable("s")),
                        List.of(
                                new NumericRelation(operation(
                                        GREATER_THAN_OR_EQUAL,
                                        variable("p"),
                                        operation(MIN, literal(38), operation(ADD, literal(3), variable("s"))))))),
                new PatternCoercion(
                        symbol("smallint"),
                        apply("decimal", variable("p"), variable("s")),
                        List.of(
                                new NumericRelation(operation(
                                        GREATER_THAN_OR_EQUAL,
                                        variable("p"),
                                        operation(MIN, literal(38), operation(ADD, literal(5), variable("s"))))))),
                new PatternCoercion(
                        symbol("integer"),
                        apply("decimal", variable("p"), variable("s")),
                        List.of(
                                new NumericRelation(operation(
                                        GREATER_THAN_OR_EQUAL,
                                        variable("p"),
                                        operation(MIN, literal(38), operation(ADD, literal(10), variable("s"))))))),
                new PatternCoercion(
                        symbol("bigint"),
                        apply("decimal", variable("p"), variable("s")),
                        List.of(
                                new NumericRelation(operation(
                                        GREATER_THAN_OR_EQUAL,
                                        variable("p"),
                                        operation(MIN, literal(38), operation(ADD, literal(19), variable("s"))))))),
                new PatternCoercion(
                        apply("varchar", variable("n1")),
                        apply("varchar", variable("n2")),
                        List.of(
                                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("n1"), variable("n2"))))),
                new PatternCoercion(
                        apply("varchar", variable("n")),
                        symbol("varchar"),
                        List.of()),
                new PatternCoercion(
                        apply("char", variable("n1")),
                        apply("char", variable("n2")),
                        List.of(
                                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("n1"), variable("n2"))))),
                // Decimal widening is lossy at the top end, mirroring the engine's clamped common
                // supertype: the target precision needs min(38, integer digits + target scale), so
                // decimal(36, 1) flows into decimal(38, 20) even though its 35 integer digits do
                // not fit — exactly what comparing it against decimal(37, 20) requires
                new PatternCoercion(
                        apply("decimal", variable("p1"), variable("s1")),
                        apply("decimal", variable("p2"), variable("s2")),
                        List.of(
                                new NumericRelation(operation(
                                        GREATER_THAN_OR_EQUAL,
                                        variable("p2"),
                                        operation(
                                                MIN,
                                                literal(38),
                                                operation(ADD, operation(SUBTRACT, variable("p1"), variable("s1")), variable("s2"))))),
                                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("s1"), variable("s2"))))),
                new PatternCoercion(
                        apply("decimal", variable("p"), variable("s")),
                        symbol("real"),
                        List.of()),
                new PatternCoercion(
                        apply("decimal", variable("p"), variable("s")),
                        symbol("double"),
                        List.of()),
                new PatternCoercion(
                        symbol("date"),
                        apply("timestamp", variable("p")),
                        List.of()),
                new PatternCoercion(
                        symbol("date"),
                        apply("timestamp_with_time_zone", variable("p")),
                        List.of()),
                new PatternCoercion(
                        apply("time", variable("p1")),
                        apply("time", variable("p2")),
                        List.of(
                                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("p1"), variable("p2"))))),
                new PatternCoercion(
                        apply("time", variable("p1")),
                        apply("time_with_time_zone", variable("p2")),
                        List.of(
                                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("p1"), variable("p2"))))),
                new PatternCoercion(
                        apply("time_with_time_zone", variable("p1")),
                        apply("time_with_time_zone", variable("p2")),
                        List.of(
                                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("p1"), variable("p2"))))),
                new PatternCoercion(
                        apply("timestamp", variable("p1")),
                        apply("timestamp", variable("p2")),
                        List.of(
                                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("p1"), variable("p2"))))),
                new PatternCoercion(
                        apply("timestamp", variable("p1")),
                        apply("timestamp_with_time_zone", variable("p2")),
                        List.of(
                                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("p1"), variable("p2"))))),
                new PatternCoercion(
                        apply("timestamp_with_time_zone", variable("p1")),
                        apply("timestamp_with_time_zone", variable("p2")),
                        List.of(
                                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("p1"), variable("p2")))))));

        switch (charVarcharCoercion) {
            case SQL_STANDARD -> {
                rules.add(new PatternCoercion(
                        apply("char", variable("n1")),
                        apply("varchar", variable("n2")),
                        List.of(new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("n1"), variable("n2"))))));
                rules.add(new PatternCoercion(
                        apply("char", variable("n")),
                        symbol("varchar"),
                        List.of()));
            }
            case LEGACY -> {
                rules.add(new PatternCoercion(
                        apply("varchar", variable("n1")),
                        apply("char", variable("n2")),
                        List.of(new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("n1"), variable("n2"))))));
                // Unbounded varchar coerces only to char(MAX_LENGTH). Pin the target length
                // through the numeric channel because unbounded varchar carries no length.
                rules.add(new PatternCoercion(
                        symbol("varchar"),
                        apply("char", variable("n")),
                        List.of(
                                new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("n"), literal((int) CharType.MAX_LENGTH))),
                                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("n"), literal((int) CharType.MAX_LENGTH))))));
            }
        }
        return List.copyOf(rules);
    }

    public static List<CoercionRule> castRules()
    {
        // Cast-only rules — valid as explicit CAST but NOT as implicit coercion.
        // (Rules already in coercionRules() automatically work as casts too via TypeSystem.castPlan.)
        List<CoercionRule> rules = new ArrayList<>(List.of(
                // Numeric / boolean parse casts: varchar -> ...
                new PatternCoercion(apply("varchar", variable("n")), symbol("tinyint"), List.of()),
                new PatternCoercion(apply("varchar", variable("n")), symbol("smallint"), List.of()),
                new PatternCoercion(apply("varchar", variable("n")), symbol("integer"), List.of()),
                new PatternCoercion(apply("varchar", variable("n")), symbol("bigint"), List.of()),
                new PatternCoercion(apply("varchar", variable("n")), symbol("real"), List.of()),
                new PatternCoercion(apply("varchar", variable("n")), symbol("double"), List.of()),
                new PatternCoercion(apply("varchar", variable("n")), symbol("boolean"), List.of()),
                new PatternCoercion(apply("varchar", variable("n1")), apply("decimal", variable("p"), variable("s")), List.of()),
                new PatternCoercion(apply("varchar", variable("n")), symbol("date"), List.of()),
                new PatternCoercion(apply("varchar", variable("n1")), apply("time", variable("p")), List.of()),
                new PatternCoercion(apply("varchar", variable("n1")), apply("timestamp", variable("p")), List.of()),
                new PatternCoercion(apply("varchar", variable("n1")), apply("time_with_time_zone", variable("p")), List.of()),
                new PatternCoercion(apply("varchar", variable("n1")), apply("timestamp_with_time_zone", variable("p")), List.of()),
                new PatternCoercion(apply("varchar", variable("n")), symbol("varbinary"), List.of()),
                new PatternCoercion(symbol("varchar"), apply("time_with_time_zone", variable("p")), List.of()),
                new PatternCoercion(symbol("varchar"), apply("timestamp_with_time_zone", variable("p")), List.of()),
                new PatternCoercion(symbol("varchar"), symbol("varbinary"), List.of()),

                // Numeric / boolean format casts: ... -> varchar
                new PatternCoercion(symbol("tinyint"), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(symbol("smallint"), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(symbol("integer"), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(symbol("bigint"), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(symbol("real"), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(symbol("double"), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(symbol("boolean"), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(apply("decimal", variable("p"), variable("s")), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(symbol("date"), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(apply("time", variable("p")), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(apply("timestamp", variable("p")), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(apply("time_with_time_zone", variable("p")), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(apply("time_with_time_zone", variable("p")), symbol("varchar"), List.of()),
                new PatternCoercion(apply("timestamp_with_time_zone", variable("p")), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(apply("timestamp_with_time_zone", variable("p")), symbol("varchar"), List.of()),
                new PatternCoercion(symbol("interval_year_to_month"), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(symbol("interval_year_to_month"), symbol("varchar"), List.of()),
                new PatternCoercion(symbol("interval_day_to_second"), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(symbol("interval_day_to_second"), symbol("varchar"), List.of()),

                // Numeric narrowing (cast-only): bigint -> integer, etc.
                new PrimitiveTypeCoercion("bigint", "integer"),
                new PrimitiveTypeCoercion("bigint", "smallint"),
                new PrimitiveTypeCoercion("bigint", "tinyint"),
                new PrimitiveTypeCoercion("integer", "smallint"),
                new PrimitiveTypeCoercion("integer", "tinyint"),
                new PrimitiveTypeCoercion("smallint", "tinyint"),
                new PrimitiveTypeCoercion("double", "real"),
                new PrimitiveTypeCoercion("double", "bigint"),
                new PrimitiveTypeCoercion("double", "integer"),
                new PrimitiveTypeCoercion("real", "bigint"),
                new PrimitiveTypeCoercion("real", "integer"),

                // char -> varchar (reverse of the implicit varchar -> char).
                new PatternCoercion(
                        apply("char", variable("n1")),
                        apply("varchar", variable("n2")),
                        List.of()),
                new PatternCoercion(apply("char", variable("n")), symbol("varchar"), List.of()),
                // char parse casts (char is trimmed then parsed). Trino supports these targets but
                // not char->decimal/time/uuid/ipaddress, so those are intentionally omitted.
                new PatternCoercion(apply("char", variable("n")), symbol("tinyint"), List.of()),
                new PatternCoercion(apply("char", variable("n")), symbol("smallint"), List.of()),
                new PatternCoercion(apply("char", variable("n")), symbol("integer"), List.of()),
                new PatternCoercion(apply("char", variable("n")), symbol("bigint"), List.of()),
                new PatternCoercion(apply("char", variable("n")), symbol("real"), List.of()),
                new PatternCoercion(apply("char", variable("n")), symbol("double"), List.of()),
                new PatternCoercion(apply("char", variable("n")), symbol("number"), List.of()),
                new PatternCoercion(apply("char", variable("n")), symbol("varbinary"), List.of()),
                new PatternCoercion(apply("char", variable("n")), symbol("date"), List.of()),
                new PatternCoercion(apply("char", variable("n1")), apply("timestamp", variable("p")), List.of()),
                new PatternCoercion(apply("char", variable("n1")), apply("timestamp_with_time_zone", variable("p")), List.of()),
                new PatternCoercion(apply("char", variable("n1")), apply("char", variable("n2")), List.of()),

                // JSON conversions. Scalar types ↔ JSON are direct casts.
                new PrimitiveTypeCoercion("boolean", "json"),
                new PrimitiveTypeCoercion("tinyint", "json"),
                new PrimitiveTypeCoercion("smallint", "json"),
                new PrimitiveTypeCoercion("integer", "json"),
                new PrimitiveTypeCoercion("bigint", "json"),
                new PrimitiveTypeCoercion("real", "json"),
                new PrimitiveTypeCoercion("double", "json"),
                new PatternCoercion(apply("decimal", variable("p"), variable("s")), symbol("json"), List.of()),
                new PatternCoercion(apply("varchar", variable("n")), symbol("json"), List.of()),
                new PrimitiveTypeCoercion("json", "boolean"),
                new PrimitiveTypeCoercion("json", "tinyint"),
                new PrimitiveTypeCoercion("json", "smallint"),
                new PrimitiveTypeCoercion("json", "integer"),
                new PrimitiveTypeCoercion("json", "bigint"),
                new PrimitiveTypeCoercion("json", "real"),
                new PrimitiveTypeCoercion("json", "double"),
                new PatternCoercion(symbol("json"), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(symbol("json"), apply("decimal", variable("p"), variable("s")), List.of()),

                // Compositional container → JSON casts: valid iff elements cast to JSON.
                new PatternCoercion(
                        apply("array", variable("T")),
                        symbol("json"),
                        List.of(new RequireCastableTo(variable("T"), symbol("json")))),
                new PatternCoercion(
                        apply("map", variable("K"), variable("V")),
                        symbol("json"),
                        List.of(
                                new RequireCastableTo(variable("K"), symbol("varchar")),
                                new RequireCastableTo(variable("V"), symbol("json")))),

                // Reverse JSON container casts.
                new PatternCoercion(
                        symbol("json"),
                        apply("array", variable("T")),
                        List.of(new RequireCastableFrom(variable("T"), symbol("json")))),
                new PatternCoercion(
                        symbol("json"),
                        apply("map", variable("K"), variable("V")),
                        List.of(
                                new RequireCastableFrom(variable("K"), symbol("varchar")),
                                new RequireCastableFrom(variable("V"), symbol("json")))),
                new JsonToRowCastRule(),
                new RowToJsonCastRule(),

                // Unbounded varchar → bounded varchar: cast with truncation.
                new PatternCoercion(symbol("varchar"), apply("varchar", variable("n")), List.of()),
                // varchar narrowing casts (implicit rules only widen): varchar -> char(n) and varchar(n1) -> varchar(n2).
                new PatternCoercion(apply("varchar", variable("n1")), apply("varchar", variable("n2")), List.of()),
                new PatternCoercion(apply("varchar", variable("n1")), apply("char", variable("n2")), List.of()),
                new PatternCoercion(symbol("varchar"), apply("char", variable("n")), List.of()),

                // UUID casts.
                new PatternCoercion(apply("varchar", variable("n")), symbol("uuid"), List.of()),
                new PatternCoercion(symbol("uuid"), apply("varchar", variable("n")), List.of()),

                // IPAddress casts. ipaddress -> varchar is unbounded-only (added in unboundedVarcharCasts);
                // varchar(n) -> ipaddress accepts any length.
                new PatternCoercion(apply("varchar", variable("n")), symbol("ipaddress"), List.of()),

                // Temporal narrowing casts.
                new PatternCoercion(apply("timestamp", variable("p")), symbol("date"), List.of()),
                new PatternCoercion(apply("timestamp_with_time_zone", variable("p1")), apply("timestamp", variable("p2")), List.of()),
                new PatternCoercion(apply("time_with_time_zone", variable("p1")), apply("time", variable("p2")), List.of()),
                // time / time-with-time-zone explicit casts (the implicit rules only cover widening).
                // Casting to a timestamp target combines the time with the current date.
                new PatternCoercion(apply("time", variable("p1")), apply("time", variable("p2")), List.of()),
                new PatternCoercion(apply("time", variable("p1")), apply("time_with_time_zone", variable("p2")), List.of()),
                new PatternCoercion(apply("time", variable("p1")), apply("timestamp", variable("p2")), List.of()),
                new PatternCoercion(apply("time", variable("p1")), apply("timestamp_with_time_zone", variable("p2")), List.of()),
                new PatternCoercion(apply("time_with_time_zone", variable("p1")), apply("timestamp", variable("p2")), List.of()),
                new PatternCoercion(apply("time_with_time_zone", variable("p1")), apply("timestamp_with_time_zone", variable("p2")), List.of()),
                new PatternCoercion(apply("time_with_time_zone", variable("p1")), apply("time_with_time_zone", variable("p2")), List.of()),
                // timestamp / timestamp-with-time-zone explicit casts (implicit rules only widen).
                new PatternCoercion(apply("timestamp", variable("p1")), apply("timestamp", variable("p2")), List.of()),
                new PatternCoercion(apply("timestamp", variable("p1")), apply("timestamp_with_time_zone", variable("p2")), List.of()),
                new PatternCoercion(apply("timestamp", variable("p1")), apply("time", variable("p2")), List.of()),
                new PatternCoercion(apply("timestamp", variable("p1")), apply("time_with_time_zone", variable("p2")), List.of()),
                new PatternCoercion(apply("timestamp_with_time_zone", variable("p")), symbol("date"), List.of()),
                new PatternCoercion(apply("timestamp_with_time_zone", variable("p1")), apply("time", variable("p2")), List.of()),
                new PatternCoercion(apply("timestamp_with_time_zone", variable("p1")), apply("time_with_time_zone", variable("p2")), List.of()),
                new PatternCoercion(apply("timestamp_with_time_zone", variable("p1")), apply("timestamp_with_time_zone", variable("p2")), List.of()),

                // Decimal narrowing (any decimal → any decimal, cast-only).
                new PatternCoercion(
                        apply("decimal", variable("p1"), variable("s1")),
                        apply("decimal", variable("p2"), variable("s2")),
                        List.of()),
                // Numeric <-> decimal explicit casts (unguarded; the implicit int->decimal rules are
                // precision-guarded, decimal narrows to the integer types, and real/double widen to decimal).
                new PatternCoercion(symbol("tinyint"), apply("decimal", variable("p"), variable("s")), List.of()),
                new PatternCoercion(symbol("smallint"), apply("decimal", variable("p"), variable("s")), List.of()),
                new PatternCoercion(symbol("integer"), apply("decimal", variable("p"), variable("s")), List.of()),
                new PatternCoercion(symbol("bigint"), apply("decimal", variable("p"), variable("s")), List.of()),
                new PatternCoercion(symbol("real"), apply("decimal", variable("p"), variable("s")), List.of()),
                new PatternCoercion(symbol("double"), apply("decimal", variable("p"), variable("s")), List.of()),
                new PatternCoercion(apply("decimal", variable("p"), variable("s")), symbol("tinyint"), List.of()),
                new PatternCoercion(apply("decimal", variable("p"), variable("s")), symbol("smallint"), List.of()),
                new PatternCoercion(apply("decimal", variable("p"), variable("s")), symbol("integer"), List.of()),
                new PatternCoercion(apply("decimal", variable("p"), variable("s")), symbol("bigint"), List.of()),

                // Remaining scalar casts Trino supports.
                new PrimitiveTypeCoercion("real", "tinyint"),
                new PrimitiveTypeCoercion("real", "smallint"),
                new PrimitiveTypeCoercion("double", "tinyint"),
                new PrimitiveTypeCoercion("double", "smallint"),
                new PrimitiveTypeCoercion("varbinary", "uuid"),
                new PrimitiveTypeCoercion("uuid", "varbinary"),
                new PrimitiveTypeCoercion("varbinary", "ipaddress"),
                new PrimitiveTypeCoercion("ipaddress", "varbinary"),
                new PrimitiveTypeCoercion("date", "json"),
                new PrimitiveTypeCoercion("json", "number"),
                new PatternCoercion(apply("timestamp", variable("p")), symbol("json"), List.of()),
                // Number casts: cast-only directions (implicit directions are in coercionRules).
                new PrimitiveTypeCoercion("real", "number"),
                new PrimitiveTypeCoercion("double", "number"),
                new PatternCoercion(apply("varchar", variable("n")), symbol("number"), List.of()),
                new PrimitiveTypeCoercion("number", "tinyint"),
                new PrimitiveTypeCoercion("number", "smallint"),
                new PrimitiveTypeCoercion("number", "integer"),
                new PrimitiveTypeCoercion("number", "bigint"),
                new PrimitiveTypeCoercion("number", "real"),
                new PrimitiveTypeCoercion("number", "double"),
                new PatternCoercion(symbol("number"), apply("varchar", variable("n")), List.of()),
                new PatternCoercion(symbol("number"), apply("decimal", variable("p"), variable("s")), List.of()),
                new PrimitiveTypeCoercion("boolean", "number"),
                new PrimitiveTypeCoercion("number", "json"),
                // boolean <-> numeric casts (true/false <-> 1/0), and char -> boolean.
                new PrimitiveTypeCoercion("boolean", "tinyint"),
                new PrimitiveTypeCoercion("boolean", "smallint"),
                new PrimitiveTypeCoercion("boolean", "integer"),
                new PrimitiveTypeCoercion("boolean", "bigint"),
                new PrimitiveTypeCoercion("boolean", "real"),
                new PrimitiveTypeCoercion("boolean", "double"),
                new PatternCoercion(symbol("boolean"), apply("decimal", variable("p"), variable("s")), List.of()),
                new PrimitiveTypeCoercion("tinyint", "boolean"),
                new PrimitiveTypeCoercion("smallint", "boolean"),
                new PrimitiveTypeCoercion("integer", "boolean"),
                new PrimitiveTypeCoercion("bigint", "boolean"),
                new PrimitiveTypeCoercion("real", "boolean"),
                new PrimitiveTypeCoercion("double", "boolean"),
                new PatternCoercion(apply("decimal", variable("p"), variable("s")), symbol("boolean"), List.of()),
                new PatternCoercion(apply("char", variable("n")), symbol("boolean"), List.of()),
                // Sketch types cast to/from varbinary.
                new PrimitiveTypeCoercion("hyperloglog", "varbinary"),
                new PrimitiveTypeCoercion("varbinary", "hyperloglog"),
                new PrimitiveTypeCoercion("tdigest", "varbinary"),
                new PrimitiveTypeCoercion("varbinary", "tdigest"),
                new PrimitiveTypeCoercion("setdigest", "varbinary"),
                new PrimitiveTypeCoercion("varbinary", "setdigest"),
                new PrimitiveTypeCoercion("setdigest", "hyperloglog"),
                new PrimitiveTypeCoercion("hyperloglog", "p4hyperloglog"),
                new PatternCoercion(apply("qdigest", variable("T")), symbol("varbinary"), List.of()),
                new PatternCoercion(symbol("varbinary"), apply("qdigest", variable("T")), List.of())));
        rules.addAll(unboundedVarcharCasts());
        rules.addAll(variantCasts());
        return List.copyOf(rules);
    }

    /// Variant casts: variant converts to and from the scalar and temporal types Trino supports,
    /// plus `varchar -> variant` for any length and `variant -> varchar` unbounded only.
    private static List<CoercionRule> variantCasts()
    {
        Expression variant = symbol("variant");
        List<CoercionRule> rules = new ArrayList<>();
        for (String scalar : List.of("boolean", "tinyint", "smallint", "integer", "bigint", "real", "double", "date", "varbinary", "json", "uuid")) {
            rules.add(new PrimitiveTypeCoercion("variant", scalar));
            rules.add(new PrimitiveTypeCoercion(scalar, "variant"));
        }
        for (String temporal : List.of("time", "timestamp", "timestamp_with_time_zone")) {
            rules.add(new PatternCoercion(variant, apply(temporal, variable("p")), List.of()));
            rules.add(new PatternCoercion(apply(temporal, variable("p")), variant, List.of()));
        }
        rules.add(new PatternCoercion(apply("decimal", variable("p"), variable("s")), variant, List.of()));
        rules.add(new PatternCoercion(variant, apply("decimal", variable("p"), variable("s")), List.of()));
        rules.add(new PatternCoercion(apply("varchar", variable("n")), variant, List.of()));
        rules.add(new PatternCoercion(symbol("varchar"), variant, List.of()));
        rules.add(new PatternCoercion(variant, symbol("varchar"), List.of()));
        // Containers cast to/from variant element-wise (like json).
        rules.add(new PatternCoercion(apply("array", variable("T")), variant, List.of(new RequireCastableTo(variable("T"), variant))));
        rules.add(new PatternCoercion(variant, apply("array", variable("T")), List.of(new RequireCastableFrom(variable("T"), variant))));
        // Map <-> variant requires a varchar key (variant objects are keyed by strings, like json).
        for (Expression key : List.of(apply("varchar", variable("n")), symbol("varchar"))) {
            rules.add(new PatternCoercion(
                    apply("map", key, variable("V")),
                    variant,
                    List.of(new RequireCastableTo(variable("V"), variant))));
            rules.add(new PatternCoercion(
                    variant,
                    apply("map", key, variable("V")),
                    List.of(new RequireCastableFrom(variable("V"), variant))));
        }
        rules.add(new RowToVariantCastRule());
        rules.add(new VariantToRowCastRule());
        return rules;
    }

    /// Unbounded varchar (the `symbol("varchar")` form) casts wherever bounded
    /// `varchar(n)` does. Trino models unbounded varchar as `varchar(2^31-1)`, so the
    /// same cast surface applies; the preset keeps the two forms distinct, so the bounded cast rules
    /// are mirrored here for the symbol form.
    private static List<CoercionRule> unboundedVarcharCasts()
    {
        Expression varchar = symbol("varchar");
        List<Expression> targets = List.of(
                symbol("tinyint"),
                symbol("smallint"),
                symbol("integer"),
                symbol("bigint"),
                symbol("real"),
                symbol("double"),
                symbol("boolean"),
                symbol("number"),
                symbol("date"),
                symbol("json"),
                symbol("uuid"),
                symbol("ipaddress"),
                apply("decimal", variable("p"), variable("s")),
                apply("time", variable("p")),
                apply("timestamp", variable("p")));
        List<CoercionRule> rules = new ArrayList<>();
        for (Expression target : targets) {
            rules.add(new PatternCoercion(varchar, target, List.of()));
            rules.add(new PatternCoercion(target, varchar, List.of()));
        }
        return rules;
    }

    public static TypeSystem typeSystem()
    {
        return typeSystem(CharVarcharCoercion.SQL_STANDARD);
    }

    public static TypeSystem typeSystem(CharVarcharCoercion charVarcharCoercion)
    {
        return new TypeSystem(typeConstructors(), coercionRules(charVarcharCoercion), castRules());
    }
}
