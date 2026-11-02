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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.json.Json;
import io.trino.json.JsonArray;
import io.trino.json.JsonItems;
import io.trino.json.TypedValue;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TrinoNumber;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

class TestJsonRepresentations
{
    @Test
    void testExtractionRepresentations()
    {
        for (String document : List.of(
                "{\"a\":{\"é\":[1,\"line\\nquote\\\"\",null]},\"a\":2}",
                "[0,1.20,3e2,true,null,{\"x\":1,\"x\":2}]",
                "{\"0\":1,\"\":2,\"a.b\":3}",
                "null",
                "1",
                "\"text\"",
                "[]",
                "{}")) {
            Slice text = utf8Slice(document);
            for (Json json : List.of(Json.unchecked(text), JsonItems.fromText(text), JsonItems.parseToTree(text))) {
                Slice reference = JsonItems.toText(json);
                for (String pattern : List.of(
                        "$",
                        "$.a",
                        "$.a.é[0]",
                        "$.a.é[1]",
                        "$.a.é[2]",
                        "$.a.missing",
                        "$[0]",
                        "$[5].x",
                        "$[99]",
                        "$[\"-1\"]",
                        "$[2147483648]",
                        "$[\"+0\"]",
                        "$[\"-0\"]",
                        "$[\"00\"]",
                        "$[\"٠\"]",
                        "$[\"\"]",
                        "$[\"a.b\"]",
                        "$.missing")) {
                    JsonPath path = new JsonPath(pattern);
                    assertThat(JsonFunctions.jsonExtractScalar(json, path))
                            .as("scalar %s in %s", pattern, document)
                            .isEqualTo(JsonFunctions.varcharJsonExtractScalar(reference, path));
                    assertThat(JsonFunctions.jsonSize(json, path))
                            .as("size %s in %s", pattern, document)
                            .isEqualTo(JsonFunctions.varcharJsonSize(reference, path));
                    Json expected = JsonFunctions.varcharJsonExtract(reference, path);
                    Json actual = JsonFunctions.jsonExtract(json, path);
                    assertThat(actual)
                            .as("extract %s in %s", pattern, document)
                            .isEqualTo(expected);
                }
            }
        }
    }

    @Test
    void testTypedScalars()
    {
        for (Json scalar : List.of(
                new TypedValue(DOUBLE, 1.0),
                new TypedValue(DOUBLE, Double.NaN),
                new TypedValue(DOUBLE, Double.POSITIVE_INFINITY),
                new TypedValue(DOUBLE, Double.NEGATIVE_INFINITY))) {
            Json json = new JsonArray(List.of(scalar));
            JsonPath path = new JsonPath("$[0]");
            assertThat(JsonFunctions.jsonExtract(json, path)).isSameAs(scalar);
            assertThat(JsonFunctions.jsonExtract(json, path).materializeScalar().type()).isEqualTo(DOUBLE);
            assertThat(JsonFunctions.jsonExtractScalar(json, path))
                    .isEqualTo(JsonFunctions.varcharJsonExtractScalar(JsonItems.toText(json), path));
        }
    }

    @Test
    void testBigintContainsTokenSemantics()
    {
        for (Json scalar : List.of(
                new TypedValue(BIGINT, 1L),
                new TypedValue(createDecimalType(1, 0), 1L),
                new TypedValue(createDecimalType(38, 0), Int128.valueOf(1)),
                new TypedValue(createDecimalType(38, 0), Int128.valueOf(Long.MIN_VALUE)),
                new TypedValue(createDecimalType(38, 0), Int128.valueOf(Long.MAX_VALUE)),
                new TypedValue(createDecimalType(38, 0), Int128.valueOf("9223372036854775808")),
                new TypedValue(createDecimalType(38, 0), Int128.valueOf("-9223372036854775809")),
                new TypedValue(createDecimalType(2, 1), 10L),
                new TypedValue(createDecimalType(2, 1), 15L),
                new TypedValue(createDecimalType(38, 1), Int128.valueOf(10)),
                new TypedValue(NUMBER, TrinoNumber.from(new BigDecimal("0.000"))),
                new TypedValue(NUMBER, TrinoNumber.from(new BigDecimal("1.00"))),
                new TypedValue(NUMBER, TrinoNumber.from(new BigDecimal("1.5"))),
                new TypedValue(NUMBER, TrinoNumber.from(new BigDecimal("1E+3"))),
                new TypedValue(NUMBER, TrinoNumber.from(new BigDecimal("1E+50"))),
                new TypedValue(NUMBER, TrinoNumber.from(new BigDecimal("9223372036854775808"))),
                new TypedValue(NUMBER, TrinoNumber.from(new BigDecimal("-9223372036854775809"))),
                new TypedValue(DOUBLE, 1.0),
                new TypedValue(DOUBLE, Double.NaN))) {
            Json tree = new JsonArray(List.of(scalar));
            for (Json json : List.of(tree, Json.of(tree.encoding()), Json.unchecked(JsonItems.toText(tree)))) {
                for (long value : new long[] {Long.MIN_VALUE, -1, 0, 1, 2, 1000, Long.MAX_VALUE}) {
                    assertThat(JsonFunctions.jsonArrayContains(json, value))
                            .as("contains %s in %s", value, JsonItems.toText(json).toStringUtf8())
                            .isEqualTo(JsonFunctions.varcharJsonArrayContains(JsonItems.toText(json), value));
                }
            }
        }
    }

    @Test
    void testDoubleContainsTokenSemantics()
    {
        for (Json scalar : List.of(
                new TypedValue(BIGINT, 1L),
                new TypedValue(DOUBLE, 1.0),
                new TypedValue(REAL, (long) floatToRawIntBits(0.1f)),
                new TypedValue(createDecimalType(2, 0), 1L),
                new TypedValue(createDecimalType(2, 1), 10L),
                new TypedValue(NUMBER, TrinoNumber.from(new BigDecimal("1.00"))),
                new TypedValue(NUMBER, TrinoNumber.from(new BigDecimal("1E+50"))),
                new TypedValue(DOUBLE, Double.NaN),
                new TypedValue(DOUBLE, Double.POSITIVE_INFINITY))) {
            Json tree = new JsonArray(List.of(scalar));
            for (Json json : List.of(tree, Json.of(tree.encoding()), Json.unchecked(JsonItems.toText(tree)))) {
                for (double value : new double[] {0.1, (double) 0.1f, 1, 1e50, Double.NaN, Double.POSITIVE_INFINITY}) {
                    assertThat(JsonFunctions.jsonArrayContains(json, value))
                            .as("contains %s in %s", value, JsonItems.toText(json).toStringUtf8())
                            .isEqualTo(JsonFunctions.varcharJsonArrayContains(JsonItems.toText(json), value));
                }
            }
        }
    }
}
