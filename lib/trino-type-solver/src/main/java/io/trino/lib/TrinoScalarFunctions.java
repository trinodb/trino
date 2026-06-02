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

import org.weakref.solver.RequireComparable;
import org.weakref.solver.RequireOrderable;
import org.weakref.solver.TypeLibrary;
import org.weakref.solver.TypeScheme;

import java.util.List;

import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.function;
import static org.weakref.solver.Expression.symbol;
import static org.weakref.solver.Expression.variable;
import static org.weakref.solver.Expression.variadicFunction;

/**
 * Registers a representative set of Trino's built-in scalar function signatures. Not exhaustive —
 * covers string, numeric, temporal, json, array, map, regexp, hash, encoding, url, conditional,
 * and miscellaneous categories. Aggregate and window functions are intentionally excluded (they
 * use different resolution paths).
 */
public final class TrinoScalarFunctions
{
    private TrinoScalarFunctions() {}

    public static TypeLibrary.Builder register(TypeLibrary.Builder builder)
    {
        registerStringFunctions(builder);
        registerNumericFunctions(builder);
        registerTemporalFunctions(builder);
        registerJsonFunctions(builder);
        registerArrayFunctions(builder);
        registerMapFunctions(builder);
        registerRegexpFunctions(builder);
        registerHashFunctions(builder);
        registerEncodingFunctions(builder);
        registerUrlFunctions(builder);
        registerConditionalFunctions(builder);
        registerMiscellaneous(builder);
        return builder;
    }

    private static void registerStringFunctions(TypeLibrary.Builder builder)
    {
        builder.registerFunction("length", function(List.of(symbol("varchar")), symbol("bigint")));
        builder.registerFunction("length", function(List.of(symbol("varbinary")), symbol("bigint")));
        builder.registerFunction("char_length", function(List.of(symbol("varchar")), symbol("bigint")));
        builder.registerFunction("character_length", function(List.of(symbol("varchar")), symbol("bigint")));
        builder.registerFunction("concat", new TypeScheme(
                List.of(),
                List.of(),
                variadicFunction(List.of(symbol("varchar"), symbol("varchar")), symbol("varchar"), symbol("varchar"))));
        builder.registerFunction("substr", function(List.of(symbol("varchar"), symbol("bigint")), symbol("varchar")));
        builder.registerFunction("substr", function(List.of(symbol("varchar"), symbol("bigint"), symbol("bigint")), symbol("varchar")));
        builder.registerFunction("substring", function(List.of(symbol("varchar"), symbol("bigint")), symbol("varchar")));
        builder.registerFunction("substring", function(List.of(symbol("varchar"), symbol("bigint"), symbol("bigint")), symbol("varchar")));
        builder.registerFunction("upper", function(List.of(symbol("varchar")), symbol("varchar")));
        builder.registerFunction("lower", function(List.of(symbol("varchar")), symbol("varchar")));
        builder.registerFunction("trim", function(List.of(symbol("varchar")), symbol("varchar")));
        builder.registerFunction("ltrim", function(List.of(symbol("varchar")), symbol("varchar")));
        builder.registerFunction("rtrim", function(List.of(symbol("varchar")), symbol("varchar")));
        builder.registerFunction("replace", function(List.of(symbol("varchar"), symbol("varchar")), symbol("varchar")));
        builder.registerFunction("replace", function(List.of(symbol("varchar"), symbol("varchar"), symbol("varchar")), symbol("varchar")));
        builder.registerFunction("reverse", function(List.of(symbol("varchar")), symbol("varchar")));
        builder.registerFunction("repeat", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(variable("@T"), symbol("bigint")), apply("array", variable("@T")))));
        builder.registerFunction("position", function(List.of(symbol("varchar"), symbol("varchar")), symbol("bigint")));
        builder.registerFunction("strpos", function(List.of(symbol("varchar"), symbol("varchar")), symbol("bigint")));
        builder.registerFunction("strpos", function(List.of(symbol("varchar"), symbol("varchar"), symbol("bigint")), symbol("bigint")));
        builder.registerFunction("lpad", function(List.of(symbol("varchar"), symbol("bigint"), symbol("varchar")), symbol("varchar")));
        builder.registerFunction("rpad", function(List.of(symbol("varchar"), symbol("bigint"), symbol("varchar")), symbol("varchar")));
        builder.registerFunction("split", function(List.of(symbol("varchar"), symbol("varchar")), apply("array", symbol("varchar"))));
        builder.registerFunction("split", function(List.of(symbol("varchar"), symbol("varchar"), symbol("bigint")), apply("array", symbol("varchar"))));
        builder.registerFunction("split_part", function(List.of(symbol("varchar"), symbol("varchar"), symbol("bigint")), symbol("varchar")));
        builder.registerFunction("split_to_map", function(List.of(symbol("varchar"), symbol("varchar"), symbol("varchar")), apply("map", symbol("varchar"), symbol("varchar"))));
        builder.registerFunction("chr", function(List.of(symbol("bigint")), symbol("varchar")));
        builder.registerFunction("codepoint", function(List.of(symbol("varchar")), symbol("integer")));
        builder.registerFunction("starts_with", function(List.of(symbol("varchar"), symbol("varchar")), symbol("boolean")));
        builder.registerFunction("ends_with", function(List.of(symbol("varchar"), symbol("varchar")), symbol("boolean")));
        builder.registerFunction("like", function(List.of(symbol("varchar"), symbol("varchar")), symbol("boolean")));
        builder.registerFunction("like", function(List.of(symbol("varchar"), symbol("varchar"), symbol("varchar")), symbol("boolean")));
        builder.registerFunction("concat_ws", new TypeScheme(
                List.of(),
                List.of(),
                variadicFunction(List.of(symbol("varchar"), symbol("varchar")), symbol("varchar"), symbol("varchar"))));
        builder.registerFunction("format", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                variadicFunction(List.of(symbol("varchar")), variable("@T"), symbol("varchar"))));
        builder.registerFunction("levenshtein_distance", function(List.of(symbol("varchar"), symbol("varchar")), symbol("bigint")));
        builder.registerFunction("soundex", function(List.of(symbol("varchar")), symbol("varchar")));
        builder.registerFunction("normalize", function(List.of(symbol("varchar")), symbol("varchar")));
        builder.registerFunction("normalize", function(List.of(symbol("varchar"), symbol("varchar")), symbol("varchar")));
        builder.registerFunction("hamming_distance", function(List.of(symbol("varchar"), symbol("varchar")), symbol("bigint")));
    }

    private static void registerNumericFunctions(TypeLibrary.Builder builder)
    {
        // abs: polymorphic over numeric types. Register for common ones.
        for (String t : List.of("tinyint", "smallint", "integer", "bigint", "real", "double", "number")) {
            builder.registerFunction("abs", function(List.of(symbol(t)), symbol(t)));
        }
        builder.registerFunction("abs", new TypeScheme(
                List.of(variable("@p"), variable("@s")),
                List.of(),
                function(List.of(apply("decimal", variable("@p"), variable("@s"))), apply("decimal", variable("@p"), variable("@s")))));

        // ceil/floor/round
        for (String name : List.of("ceil", "ceiling", "floor")) {
            for (String t : List.of("tinyint", "smallint", "integer", "bigint", "real", "double", "number")) {
                builder.registerFunction(name, function(List.of(symbol(t)), symbol(t)));
            }
        }
        builder.registerFunction("round", function(List.of(symbol("double")), symbol("double")));
        builder.registerFunction("round", function(List.of(symbol("real")), symbol("real")));
        builder.registerFunction("round", function(List.of(symbol("double"), symbol("integer")), symbol("double")));
        builder.registerFunction("round", function(List.of(symbol("real"), symbol("integer")), symbol("real")));
        builder.registerFunction("round", function(List.of(symbol("bigint")), symbol("bigint")));
        builder.registerFunction("round", function(List.of(symbol("integer")), symbol("integer")));
        builder.registerFunction("round", function(List.of(symbol("number")), symbol("number")));
        builder.registerFunction("round", function(List.of(symbol("number"), symbol("integer")), symbol("number")));
        builder.registerFunction("truncate", function(List.of(symbol("double")), symbol("double")));
        builder.registerFunction("truncate", function(List.of(symbol("double"), symbol("integer")), symbol("double")));
        builder.registerFunction("truncate", function(List.of(symbol("number")), symbol("number")));

        // Transcendentals (double-based).
        for (String name : List.of("sqrt", "cbrt", "exp", "ln", "log2", "log10", "sin", "cos", "tan", "asin", "acos", "atan", "sinh", "cosh", "tanh", "degrees", "radians")) {
            builder.registerFunction(name, function(List.of(symbol("double")), symbol("double")));
        }
        builder.registerFunction("log", function(List.of(symbol("double"), symbol("double")), symbol("double")));
        builder.registerFunction("atan2", function(List.of(symbol("double"), symbol("double")), symbol("double")));
        builder.registerFunction("pow", function(List.of(symbol("double"), symbol("double")), symbol("double")));
        builder.registerFunction("power", function(List.of(symbol("double"), symbol("double")), symbol("double")));

        // Constants.
        builder.registerFunction("pi", function(List.of(), symbol("double")));
        builder.registerFunction("e", function(List.of(), symbol("double")));
        builder.registerFunction("nan", function(List.of(), symbol("double")));
        builder.registerFunction("infinity", function(List.of(), symbol("double")));

        // Classification.
        builder.registerFunction("is_nan", function(List.of(symbol("double")), symbol("boolean")));
        builder.registerFunction("is_nan", function(List.of(symbol("real")), symbol("boolean")));
        builder.registerFunction("is_nan", function(List.of(symbol("number")), symbol("boolean")));
        builder.registerFunction("is_finite", function(List.of(symbol("double")), symbol("boolean")));
        builder.registerFunction("is_finite", function(List.of(symbol("real")), symbol("boolean")));
        builder.registerFunction("is_finite", function(List.of(symbol("number")), symbol("boolean")));
        builder.registerFunction("is_infinite", function(List.of(symbol("double")), symbol("boolean")));
        builder.registerFunction("is_infinite", function(List.of(symbol("real")), symbol("boolean")));
        builder.registerFunction("is_infinite", function(List.of(symbol("number")), symbol("boolean")));

        // Sign / mod / random.
        for (String t : List.of("tinyint", "smallint", "integer", "bigint")) {
            builder.registerFunction("sign", function(List.of(symbol(t)), symbol(t)));
        }
        builder.registerFunction("sign", function(List.of(symbol("double")), symbol("double")));
        builder.registerFunction("sign", function(List.of(symbol("real")), symbol("real")));
        builder.registerFunction("sign", function(List.of(symbol("number")), symbol("number")));
        builder.registerFunction("mod", function(List.of(symbol("bigint"), symbol("bigint")), symbol("bigint")));
        builder.registerFunction("mod", function(List.of(symbol("integer"), symbol("integer")), symbol("integer")));
        builder.registerFunction("mod", function(List.of(symbol("double"), symbol("double")), symbol("double")));
        builder.registerFunction("mod", function(List.of(symbol("number"), symbol("number")), symbol("number")));
        builder.registerFunction("random", function(List.of(), symbol("double")));
        builder.registerFunction("random", function(List.of(symbol("bigint")), symbol("bigint")));
        builder.registerFunction("random", function(List.of(symbol("bigint"), symbol("bigint")), symbol("bigint")));
        builder.registerFunction("rand", function(List.of(), symbol("double")));

        // Bitwise.
        builder.registerFunction("bitwise_and", function(List.of(symbol("bigint"), symbol("bigint")), symbol("bigint")));
        builder.registerFunction("bitwise_or", function(List.of(symbol("bigint"), symbol("bigint")), symbol("bigint")));
        builder.registerFunction("bitwise_xor", function(List.of(symbol("bigint"), symbol("bigint")), symbol("bigint")));
        builder.registerFunction("bitwise_not", function(List.of(symbol("bigint")), symbol("bigint")));
        builder.registerFunction("bitwise_left_shift", function(List.of(symbol("bigint"), symbol("integer")), symbol("bigint")));
        builder.registerFunction("bitwise_right_shift", function(List.of(symbol("bigint"), symbol("integer")), symbol("bigint")));
        builder.registerFunction("bit_count", function(List.of(symbol("bigint"), symbol("bigint")), symbol("bigint")));
    }

    private static void registerTemporalFunctions(TypeLibrary.Builder builder)
    {
        builder.registerFunction("current_date", function(List.of(), symbol("date")));
        builder.registerFunction("current_time", function(List.of(), apply("time_with_time_zone", org.weakref.solver.Expression.literal(3))));
        builder.registerFunction("current_timestamp", function(List.of(), apply("timestamp_with_time_zone", org.weakref.solver.Expression.literal(3))));
        builder.registerFunction("current_timezone", function(List.of(), symbol("varchar")));
        builder.registerFunction("now", function(List.of(), apply("timestamp_with_time_zone", org.weakref.solver.Expression.literal(3))));
        builder.registerFunction("localtime", function(List.of(), apply("time", org.weakref.solver.Expression.literal(3))));
        builder.registerFunction("localtimestamp", function(List.of(), apply("timestamp", org.weakref.solver.Expression.literal(3))));

        // date_trunc / date_add / date_diff: variants per type.
        for (String name : List.of("date_trunc")) {
            builder.registerFunction(name, new TypeScheme(
                    List.of(variable("@p")),
                    List.of(),
                    function(List.of(symbol("varchar"), apply("timestamp", variable("@p"))), apply("timestamp", variable("@p")))));
            builder.registerFunction(name, new TypeScheme(
                    List.of(variable("@p")),
                    List.of(),
                    function(List.of(symbol("varchar"), apply("timestamp_with_time_zone", variable("@p"))), apply("timestamp_with_time_zone", variable("@p")))));
            builder.registerFunction(name, new TypeScheme(
                    List.of(variable("@p")),
                    List.of(),
                    function(List.of(symbol("varchar"), apply("time", variable("@p"))), apply("time", variable("@p")))));
            builder.registerFunction(name, function(List.of(symbol("varchar"), symbol("date")), symbol("date")));
        }
        for (String name : List.of("date_add")) {
            builder.registerFunction(name, new TypeScheme(
                    List.of(variable("@p")),
                    List.of(),
                    function(List.of(symbol("varchar"), symbol("bigint"), apply("timestamp", variable("@p"))), apply("timestamp", variable("@p")))));
            builder.registerFunction(name, function(List.of(symbol("varchar"), symbol("bigint"), symbol("date")), symbol("date")));
        }
        builder.registerFunction("date_diff", new TypeScheme(
                List.of(variable("@p1"), variable("@p2")),
                List.of(),
                function(List.of(symbol("varchar"), apply("timestamp", variable("@p1")), apply("timestamp", variable("@p2"))), symbol("bigint"))));
        builder.registerFunction("date_diff", function(List.of(symbol("varchar"), symbol("date"), symbol("date")), symbol("bigint")));

        // Format / parse.
        builder.registerFunction("date_format", new TypeScheme(
                List.of(variable("@p")),
                List.of(),
                function(List.of(apply("timestamp", variable("@p")), symbol("varchar")), symbol("varchar"))));
        builder.registerFunction("date_parse", function(List.of(symbol("varchar"), symbol("varchar")), apply("timestamp", org.weakref.solver.Expression.literal(3))));
        builder.registerFunction("format_datetime", new TypeScheme(
                List.of(variable("@p")),
                List.of(),
                function(List.of(apply("timestamp", variable("@p")), symbol("varchar")), symbol("varchar"))));
        builder.registerFunction("parse_datetime", function(List.of(symbol("varchar"), symbol("varchar")), apply("timestamp_with_time_zone", org.weakref.solver.Expression.literal(3))));

        // Unix time.
        builder.registerFunction("to_unixtime", new TypeScheme(
                List.of(variable("@p")),
                List.of(),
                function(List.of(apply("timestamp", variable("@p"))), symbol("double"))));
        builder.registerFunction("from_unixtime", function(List.of(symbol("double")), apply("timestamp_with_time_zone", org.weakref.solver.Expression.literal(3))));
        builder.registerFunction("from_unixtime", function(List.of(symbol("double"), symbol("varchar")), apply("timestamp_with_time_zone", org.weakref.solver.Expression.literal(3))));

        // at_timezone.
        builder.registerFunction("at_timezone", new TypeScheme(
                List.of(variable("@p")),
                List.of(),
                function(List.of(apply("timestamp_with_time_zone", variable("@p")), symbol("varchar")), apply("timestamp_with_time_zone", variable("@p")))));
        builder.registerFunction("with_timezone", new TypeScheme(
                List.of(variable("@p")),
                List.of(),
                function(List.of(apply("timestamp", variable("@p")), symbol("varchar")), apply("timestamp_with_time_zone", variable("@p")))));

        // Extract parts.
        for (String part : List.of("year", "month", "day", "hour", "minute", "second", "millisecond", "day_of_week", "day_of_month", "day_of_year", "week", "week_of_year", "quarter", "yow", "year_of_week")) {
            builder.registerFunction(part, new TypeScheme(
                    List.of(variable("@p")),
                    List.of(),
                    function(List.of(apply("timestamp", variable("@p"))), symbol("bigint"))));
            builder.registerFunction(part, function(List.of(symbol("date")), symbol("bigint")));
        }
        for (String part : List.of("hour", "minute", "second", "millisecond")) {
            builder.registerFunction(part, new TypeScheme(
                    List.of(variable("@p")),
                    List.of(),
                    function(List.of(apply("time", variable("@p"))), symbol("bigint"))));
        }
        builder.registerFunction("timezone_hour", new TypeScheme(
                List.of(variable("@p")),
                List.of(),
                function(List.of(apply("timestamp_with_time_zone", variable("@p"))), symbol("bigint"))));
        builder.registerFunction("timezone_minute", new TypeScheme(
                List.of(variable("@p")),
                List.of(),
                function(List.of(apply("timestamp_with_time_zone", variable("@p"))), symbol("bigint"))));
        builder.registerFunction("last_day_of_month", function(List.of(symbol("date")), symbol("date")));
        builder.registerFunction("to_iso8601", new TypeScheme(
                List.of(variable("@p")),
                List.of(),
                function(List.of(apply("timestamp", variable("@p"))), symbol("varchar"))));
        builder.registerFunction("to_iso8601", function(List.of(symbol("date")), symbol("varchar")));
        builder.registerFunction("from_iso8601_timestamp", function(List.of(symbol("varchar")), apply("timestamp_with_time_zone", org.weakref.solver.Expression.literal(3))));
        builder.registerFunction("from_iso8601_date", function(List.of(symbol("varchar")), symbol("date")));
    }

    private static void registerJsonFunctions(TypeLibrary.Builder builder)
    {
        builder.registerFunction("json_parse", function(List.of(symbol("varchar")), symbol("json")));
        builder.registerFunction("json_format", function(List.of(symbol("json")), symbol("varchar")));
        builder.registerFunction("json_extract", function(List.of(symbol("json"), symbol("varchar")), symbol("json")));
        builder.registerFunction("json_extract_scalar", function(List.of(symbol("json"), symbol("varchar")), symbol("varchar")));
        builder.registerFunction("json_size", function(List.of(symbol("json"), symbol("varchar")), symbol("bigint")));
        builder.registerFunction("json_array_length", function(List.of(symbol("json")), symbol("bigint")));
        builder.registerFunction("json_array_length", function(List.of(symbol("varchar")), symbol("bigint")));
        builder.registerFunction("is_json_scalar", function(List.of(symbol("json")), symbol("boolean")));
        builder.registerFunction("is_json_scalar", function(List.of(symbol("varchar")), symbol("boolean")));
        builder.registerFunction("json_array_contains", new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireComparable("@T")),
                function(List.of(symbol("json"), variable("@T")), symbol("boolean"))));
        builder.registerFunction("json_array_contains", new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireComparable("@T")),
                function(List.of(symbol("varchar"), variable("@T")), symbol("boolean"))));
        builder.registerFunction("json_array_get", function(List.of(symbol("json"), symbol("bigint")), symbol("json")));
    }

    private static void registerArrayFunctions(TypeLibrary.Builder builder)
    {
        // Cardinality.
        builder.registerFunction("cardinality", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(apply("array", variable("@T"))), symbol("bigint"))));
        builder.registerFunction("element_at", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(apply("array", variable("@T")), symbol("bigint")), variable("@T"))));
        builder.registerFunction("slice", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(apply("array", variable("@T")), symbol("bigint"), symbol("bigint")), apply("array", variable("@T")))));
        builder.registerFunction("concat", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                variadicFunction(List.of(apply("array", variable("@T")), apply("array", variable("@T"))), apply("array", variable("@T")), apply("array", variable("@T")))));
        builder.registerFunction("reverse", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(apply("array", variable("@T"))), apply("array", variable("@T")))));
        builder.registerFunction("shuffle", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(apply("array", variable("@T"))), apply("array", variable("@T")))));
        builder.registerFunction("array_distinct", new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireComparable("@T")),
                function(List.of(apply("array", variable("@T"))), apply("array", variable("@T")))));
        builder.registerFunction("array_union", new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireComparable("@T")),
                function(List.of(apply("array", variable("@T")), apply("array", variable("@T"))), apply("array", variable("@T")))));
        builder.registerFunction("array_intersect", new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireComparable("@T")),
                function(List.of(apply("array", variable("@T")), apply("array", variable("@T"))), apply("array", variable("@T")))));
        builder.registerFunction("array_except", new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireComparable("@T")),
                function(List.of(apply("array", variable("@T")), apply("array", variable("@T"))), apply("array", variable("@T")))));
        builder.registerFunction("array_position", new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireComparable("@T")),
                function(List.of(apply("array", variable("@T")), variable("@T")), symbol("bigint"))));
        builder.registerFunction("array_sort", new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireOrderable("@T")),
                function(List.of(apply("array", variable("@T"))), apply("array", variable("@T")))));
        // array_sort(array(T), function(T, T) -> integer) -> array(T) — comparator overload.
        // Exercises a lambda whose return type is concrete (integer) and whose args repeat the same type var T.
        builder.registerFunction("array_sort", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(
                        List.of(
                                apply("array", variable("@T")),
                                function(List.of(variable("@T"), variable("@T")), symbol("integer"))),
                        apply("array", variable("@T")))));
        builder.registerFunction("array_min", new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireOrderable("@T")),
                function(List.of(apply("array", variable("@T"))), variable("@T"))));
        builder.registerFunction("array_max", new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireOrderable("@T")),
                function(List.of(apply("array", variable("@T"))), variable("@T"))));
        builder.registerFunction("array_join", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(apply("array", variable("@T")), symbol("varchar")), symbol("varchar"))));
        builder.registerFunction("array_join", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(apply("array", variable("@T")), symbol("varchar"), symbol("varchar")), symbol("varchar"))));
        builder.registerFunction("contains", new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireComparable("@T")),
                function(List.of(apply("array", variable("@T")), variable("@T")), symbol("boolean"))));
        builder.registerFunction("sequence", function(List.of(symbol("bigint"), symbol("bigint")), apply("array", symbol("bigint"))));
        builder.registerFunction("sequence", function(List.of(symbol("bigint"), symbol("bigint"), symbol("bigint")), apply("array", symbol("bigint"))));
        builder.registerFunction("sequence", function(List.of(symbol("date"), symbol("date")), apply("array", symbol("date"))));
        builder.registerFunction("sequence", function(List.of(symbol("date"), symbol("date"), symbol("interval_day_to_second")), apply("array", symbol("date"))));
        builder.registerFunction("flatten", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(apply("array", apply("array", variable("@T")))), apply("array", variable("@T")))));
        builder.registerFunction("array_agg", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(variable("@T")), apply("array", variable("@T")))));
        builder.registerFunction("arrays_overlap", new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireComparable("@T")),
                function(List.of(apply("array", variable("@T")), apply("array", variable("@T"))), symbol("boolean"))));
        builder.registerFunction("zip", new TypeScheme(
                List.of(variable("@A"), variable("@B")),
                List.of(),
                function(List.of(apply("array", variable("@A")), apply("array", variable("@B"))),
                        apply("array", apply("row", variable("@A"), variable("@B"))))));
        builder.registerFunction("zip_with", new TypeScheme(
                List.of(variable("@A"), variable("@B"), variable("@R")),
                List.of(),
                function(List.of(
                                apply("array", variable("@A")),
                                apply("array", variable("@B")),
                                function(List.of(variable("@A"), variable("@B")), variable("@R"))),
                        apply("array", variable("@R")))));
        // combinations(array(T), integer) -> array(array(T)).
        // Exercises a nested parametric type (array of array) in the return position.
        builder.registerFunction("combinations", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(
                        List.of(apply("array", variable("@T")), symbol("integer")),
                        apply("array", apply("array", variable("@T"))))));

        // Higher-order functions.
        builder.registerFunction("filter", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(apply("array", variable("@T")), function(List.of(variable("@T")), symbol("boolean"))),
                        apply("array", variable("@T")))));
        builder.registerFunction("transform", new TypeScheme(
                List.of(variable("@T"), variable("@R")),
                List.of(),
                function(List.of(apply("array", variable("@T")), function(List.of(variable("@T")), variable("@R"))),
                        apply("array", variable("@R")))));
        builder.registerFunction("reduce", new TypeScheme(
                List.of(variable("@T"), variable("@S"), variable("@R")),
                List.of(),
                function(List.of(
                                apply("array", variable("@T")),
                                variable("@S"),
                                function(List.of(variable("@S"), variable("@T")), variable("@S")),
                                function(List.of(variable("@S")), variable("@R"))),
                        variable("@R"))));
        builder.registerFunction("any_match", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(apply("array", variable("@T")), function(List.of(variable("@T")), symbol("boolean"))), symbol("boolean"))));
        builder.registerFunction("all_match", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(apply("array", variable("@T")), function(List.of(variable("@T")), symbol("boolean"))), symbol("boolean"))));
        builder.registerFunction("none_match", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(apply("array", variable("@T")), function(List.of(variable("@T")), symbol("boolean"))), symbol("boolean"))));
    }

    private static void registerMapFunctions(TypeLibrary.Builder builder)
    {
        builder.registerFunction("cardinality", new TypeScheme(
                List.of(variable("@K"), variable("@V")),
                List.of(),
                function(List.of(apply("map", variable("@K"), variable("@V"))), symbol("bigint"))));
        builder.registerFunction("element_at", new TypeScheme(
                List.of(variable("@K"), variable("@V")),
                List.of(),
                function(List.of(apply("map", variable("@K"), variable("@V")), variable("@K")), variable("@V"))));
        builder.registerFunction("map_keys", new TypeScheme(
                List.of(variable("@K"), variable("@V")),
                List.of(),
                function(List.of(apply("map", variable("@K"), variable("@V"))), apply("array", variable("@K")))));
        builder.registerFunction("map_values", new TypeScheme(
                List.of(variable("@K"), variable("@V")),
                List.of(),
                function(List.of(apply("map", variable("@K"), variable("@V"))), apply("array", variable("@V")))));
        builder.registerFunction("map_concat", new TypeScheme(
                List.of(variable("@K"), variable("@V")),
                List.of(),
                variadicFunction(List.of(apply("map", variable("@K"), variable("@V"))), apply("map", variable("@K"), variable("@V")), apply("map", variable("@K"), variable("@V")))));
        builder.registerFunction("map", new TypeScheme(
                List.of(variable("@K"), variable("@V")),
                List.of(),
                function(List.of(apply("array", variable("@K")), apply("array", variable("@V"))), apply("map", variable("@K"), variable("@V")))));
        builder.registerFunction("map_entries", new TypeScheme(
                List.of(variable("@K"), variable("@V")),
                List.of(),
                function(List.of(apply("map", variable("@K"), variable("@V"))), apply("array", apply("row", variable("@K"), variable("@V"))))));
        builder.registerFunction("map_from_entries", new TypeScheme(
                List.of(variable("@K"), variable("@V")),
                List.of(),
                function(List.of(apply("array", apply("row", variable("@K"), variable("@V")))), apply("map", variable("@K"), variable("@V")))));
        builder.registerFunction("map_filter", new TypeScheme(
                List.of(variable("@K"), variable("@V")),
                List.of(),
                function(List.of(apply("map", variable("@K"), variable("@V")), function(List.of(variable("@K"), variable("@V")), symbol("boolean"))),
                        apply("map", variable("@K"), variable("@V")))));
        builder.registerFunction("transform_keys", new TypeScheme(
                List.of(variable("@K1"), variable("@V"), variable("@K2")),
                List.of(),
                function(List.of(apply("map", variable("@K1"), variable("@V")), function(List.of(variable("@K1"), variable("@V")), variable("@K2"))),
                        apply("map", variable("@K2"), variable("@V")))));
        builder.registerFunction("transform_values", new TypeScheme(
                List.of(variable("@K"), variable("@V1"), variable("@V2")),
                List.of(),
                function(List.of(apply("map", variable("@K"), variable("@V1")), function(List.of(variable("@K"), variable("@V1")), variable("@V2"))),
                        apply("map", variable("@K"), variable("@V2")))));
        // multimap_from_entries(array(row(K, V))) -> map(K, array(V)).
        // Exercises binding type vars K, V from row field positions inside an array pattern,
        // and constructing a nested container (map with array-typed value) in the return.
        builder.registerFunction("multimap_from_entries", new TypeScheme(
                List.of(variable("@K"), variable("@V")),
                List.of(),
                function(
                        List.of(apply("array", apply("row", variable("@K"), variable("@V")))),
                        apply("map", variable("@K"), apply("array", variable("@V"))))));
        // map_zip_with(map(K, V1), map(K, V2), function(K, V1, V2) -> V3) -> map(K, V3).
        // Exercises sharing a single type var (K) across three positions: two map keys and a lambda arg,
        // plus a 3-arg lambda and four type vars in one signature.
        builder.registerFunction("map_zip_with", new TypeScheme(
                List.of(variable("@K"), variable("@V1"), variable("@V2"), variable("@V3")),
                List.of(),
                function(
                        List.of(
                                apply("map", variable("@K"), variable("@V1")),
                                apply("map", variable("@K"), variable("@V2")),
                                function(List.of(variable("@K"), variable("@V1"), variable("@V2")), variable("@V3"))),
                        apply("map", variable("@K"), variable("@V3")))));
    }

    private static void registerRegexpFunctions(TypeLibrary.Builder builder)
    {
        builder.registerFunction("regexp_like", function(List.of(symbol("varchar"), symbol("varchar")), symbol("boolean")));
        builder.registerFunction("regexp_extract", function(List.of(symbol("varchar"), symbol("varchar")), symbol("varchar")));
        builder.registerFunction("regexp_extract", function(List.of(symbol("varchar"), symbol("varchar"), symbol("bigint")), symbol("varchar")));
        builder.registerFunction("regexp_extract_all", function(List.of(symbol("varchar"), symbol("varchar")), apply("array", symbol("varchar"))));
        builder.registerFunction("regexp_extract_all", function(List.of(symbol("varchar"), symbol("varchar"), symbol("bigint")), apply("array", symbol("varchar"))));
        builder.registerFunction("regexp_replace", function(List.of(symbol("varchar"), symbol("varchar")), symbol("varchar")));
        builder.registerFunction("regexp_replace", function(List.of(symbol("varchar"), symbol("varchar"), symbol("varchar")), symbol("varchar")));
        builder.registerFunction("regexp_split", function(List.of(symbol("varchar"), symbol("varchar")), apply("array", symbol("varchar"))));
        builder.registerFunction("regexp_position", function(List.of(symbol("varchar"), symbol("varchar")), symbol("bigint")));
        builder.registerFunction("regexp_count", function(List.of(symbol("varchar"), symbol("varchar")), symbol("bigint")));
    }

    private static void registerHashFunctions(TypeLibrary.Builder builder)
    {
        for (String name : List.of("md5", "sha1", "sha256", "sha512", "xxhash64", "murmur3", "spooky_hash_v2_32", "spooky_hash_v2_64")) {
            builder.registerFunction(name, function(List.of(symbol("varbinary")), symbol("varbinary")));
        }
        builder.registerFunction("crc32", function(List.of(symbol("varbinary")), symbol("bigint")));
    }

    private static void registerEncodingFunctions(TypeLibrary.Builder builder)
    {
        builder.registerFunction("to_base64", function(List.of(symbol("varbinary")), symbol("varchar")));
        builder.registerFunction("from_base64", function(List.of(symbol("varchar")), symbol("varbinary")));
        builder.registerFunction("to_base64url", function(List.of(symbol("varbinary")), symbol("varchar")));
        builder.registerFunction("from_base64url", function(List.of(symbol("varchar")), symbol("varbinary")));
        builder.registerFunction("to_base32", function(List.of(symbol("varbinary")), symbol("varchar")));
        builder.registerFunction("from_base32", function(List.of(symbol("varchar")), symbol("varbinary")));
        builder.registerFunction("to_hex", function(List.of(symbol("varbinary")), symbol("varchar")));
        builder.registerFunction("from_hex", function(List.of(symbol("varchar")), symbol("varbinary")));
        builder.registerFunction("to_big_endian_64", function(List.of(symbol("bigint")), symbol("varbinary")));
        builder.registerFunction("from_big_endian_64", function(List.of(symbol("varbinary")), symbol("bigint")));
        builder.registerFunction("to_big_endian_32", function(List.of(symbol("integer")), symbol("varbinary")));
        builder.registerFunction("from_big_endian_32", function(List.of(symbol("varbinary")), symbol("integer")));
        builder.registerFunction("to_ieee754_32", function(List.of(symbol("real")), symbol("varbinary")));
        builder.registerFunction("from_ieee754_32", function(List.of(symbol("varbinary")), symbol("real")));
        builder.registerFunction("to_ieee754_64", function(List.of(symbol("double")), symbol("varbinary")));
        builder.registerFunction("from_ieee754_64", function(List.of(symbol("varbinary")), symbol("double")));
        builder.registerFunction("to_utf8", function(List.of(symbol("varchar")), symbol("varbinary")));
        builder.registerFunction("from_utf8", function(List.of(symbol("varbinary")), symbol("varchar")));
        builder.registerFunction("from_utf8", function(List.of(symbol("varbinary"), symbol("varchar")), symbol("varchar")));
    }

    private static void registerUrlFunctions(TypeLibrary.Builder builder)
    {
        builder.registerFunction("url_encode", function(List.of(symbol("varchar")), symbol("varchar")));
        builder.registerFunction("url_decode", function(List.of(symbol("varchar")), symbol("varchar")));
        builder.registerFunction("url_extract_fragment", function(List.of(symbol("varchar")), symbol("varchar")));
        builder.registerFunction("url_extract_host", function(List.of(symbol("varchar")), symbol("varchar")));
        builder.registerFunction("url_extract_parameter", function(List.of(symbol("varchar"), symbol("varchar")), symbol("varchar")));
        builder.registerFunction("url_extract_path", function(List.of(symbol("varchar")), symbol("varchar")));
        builder.registerFunction("url_extract_port", function(List.of(symbol("varchar")), symbol("bigint")));
        builder.registerFunction("url_extract_protocol", function(List.of(symbol("varchar")), symbol("varchar")));
        builder.registerFunction("url_extract_query", function(List.of(symbol("varchar")), symbol("varchar")));
    }

    private static void registerConditionalFunctions(TypeLibrary.Builder builder)
    {
        builder.registerFunction("nullif", new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireComparable("@T")),
                function(List.of(variable("@T"), variable("@T")), variable("@T"))));
        builder.registerFunction("greatest", new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireOrderable("@T")),
                variadicFunction(List.of(variable("@T")), variable("@T"), variable("@T"))));
        builder.registerFunction("least", new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireOrderable("@T")),
                variadicFunction(List.of(variable("@T")), variable("@T"), variable("@T"))));
        builder.registerFunction("if", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(symbol("boolean"), variable("@T"), variable("@T")), variable("@T"))));
        builder.registerFunction("try", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(variable("@T")), variable("@T"))));
        builder.registerFunction("typeof", new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                function(List.of(variable("@T")), symbol("varchar"))));
    }

    private static void registerMiscellaneous(TypeLibrary.Builder builder)
    {
        // UUID.
        builder.registerFunction("uuid", function(List.of(), symbol("uuid")));
        // IPAddress conversions.
        builder.registerFunction("ip_prefix", function(List.of(symbol("ipaddress"), symbol("bigint")), symbol("varchar")));
        builder.registerFunction("ip_subnet_min", function(List.of(symbol("varchar")), symbol("ipaddress")));
        builder.registerFunction("ip_subnet_max", function(List.of(symbol("varchar")), symbol("ipaddress")));
        builder.registerFunction("is_subnet_of", function(List.of(symbol("varchar"), symbol("ipaddress")), symbol("boolean")));
        // Misc.
        builder.registerFunction("hash_counter", function(List.of(symbol("bigint"), symbol("integer"), symbol("integer")), symbol("bigint")));
        builder.registerFunction("fail", function(List.of(symbol("varchar")), symbol("unknown")));
        builder.registerFunction("fail", function(List.of(symbol("bigint"), symbol("varchar")), symbol("unknown")));
    }
}
