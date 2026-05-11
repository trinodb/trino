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
package io.trino.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.json.JsonItemEncoding.TypeTag.BIGINT;
import static io.trino.json.JsonItemEncoding.TypeTag.BOOLEAN;
import static io.trino.json.JsonItemEncoding.TypeTag.DOUBLE;
import static io.trino.json.JsonItemEncoding.TypeTag.INTEGER;
import static io.trino.json.JsonItemEncoding.TypeTag.VARCHAR;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static org.assertj.core.api.Assertions.assertThat;

/// Verifies the JsonNode → Json adapter: Jackson tree walking, per-number-node-type
/// discrimination (IntNode → INTEGER, LongNode → BIGINT, DecimalNode → DECIMAL, etc.),
/// and round-trip equivalence between parser+adapter and direct builder construction.
class TestJsonItems
{
    private static final JsonMapper MAPPER = new JsonMapper();

    @Test
    void testNullNode()
    {
        Json result = JsonItems.fromJsonNode(parse("null"));
        assertThat(result.kind()).isEqualTo(Json.Kind.NULL);
    }

    @Test
    void testBooleanNode()
    {
        Json t = JsonItems.fromJsonNode(parse("true"));
        Json f = JsonItems.fromJsonNode(parse("false"));
        assertThat(t.scalarType()).isEqualTo(BOOLEAN);
        assertThat(f.scalarType()).isEqualTo(BOOLEAN);
    }

    @Test
    void testStringNode()
    {
        Json result = JsonItems.fromJsonNode(parse("\"hello\""));
        assertThat(result.scalarType()).isEqualTo(VARCHAR);
    }

    @Test
    void testIntegerNumber()
    {
        // Jackson parses small integers as IntNode → INTEGER tag.
        Json result = JsonItems.fromJsonNode(parse("42"));
        assertThat(result.scalarType()).isEqualTo(INTEGER);
    }

    @Test
    void testLongNumber()
    {
        // 1e10 doesn't fit in int → LongNode → BIGINT.
        Json result = JsonItems.fromJsonNode(parse("10000000000"));
        assertThat(result.scalarType()).isEqualTo(BIGINT);
    }

    @Test
    void testFloatingPointAsDouble()
    {
        // Default Jackson parses "1.5" as DoubleNode. DecimalNode only appears when
        // the JsonMapper is configured with USE_BIG_DECIMAL_FOR_FLOATS — which is the
        // production setting for JsonInputFunctions; tests using that mapper would
        // see DECIMAL here. The adapter handles both shapes; this test just confirms
        // the default-mapper path.
        Json result = JsonItems.fromJsonNode(parse("1.5"));
        assertThat(result.scalarType()).isEqualTo(DOUBLE);
    }

    @Test
    void testEmptyArray()
    {
        Json result = JsonItems.fromJsonNode(parse("[]"));
        assertThat(result.isArray()).isTrue();
        assertThat(result.arraySize()).isEqualTo(0);
    }

    @Test
    void testNestedArray()
    {
        Json result = JsonItems.fromJsonNode(parse("[1, [2, 3], 4]"));
        assertThat(result.isArray()).isTrue();
        assertThat(result.arraySize()).isEqualTo(3);
        assertThat(result.arrayElement(0).scalarType()).isEqualTo(INTEGER);
        assertThat(result.arrayElement(1).isArray()).isTrue();
        assertThat(result.arrayElement(1).arraySize()).isEqualTo(2);
        assertThat(result.arrayElement(2).scalarType()).isEqualTo(INTEGER);
    }

    @Test
    void testEmptyObject()
    {
        Json result = JsonItems.fromJsonNode(parse("{}"));
        assertThat(result.isObject()).isTrue();
        assertThat(result.objectSize()).isEqualTo(0);
    }

    @Test
    void testNestedObject()
    {
        Json result = JsonItems.fromJsonNode(parse("{\"a\": 1, \"b\": {\"c\": \"x\"}, \"d\": null}"));
        assertThat(result.isObject()).isTrue();
        assertThat(result.objectSize()).isEqualTo(3);
        assertThat(result.objectMember("a")).isPresent()
                .get().satisfies(j -> assertThat(j.scalarType()).isEqualTo(INTEGER));
        assertThat(result.objectMember("b")).isPresent()
                .get().satisfies(j -> assertThat(j.isObject()).isTrue());
        assertThat(result.objectMember("d")).isPresent()
                .get().satisfies(j -> assertThat(j.kind()).isEqualTo(Json.Kind.NULL));
    }

    @Test
    void testNumberLiteralFormPicksTheType()
    {
        // SQL:2023 9.42: a JSON number is the value of the signed numeric literal with the
        // same characters. An exponent makes that literal approximate (DOUBLE); without one
        // it is exact (DECIMAL), and an integral one is an integer type.
        assertThat(JsonItems.fromText(utf8Slice("12.3")).materializeScalar())
                .isEqualTo(new TypedValue(createDecimalType(3, 1), 123L));

        assertThat(JsonItems.fromText(utf8Slice("1.23E1")).materializeScalar())
                .isEqualTo(new TypedValue(DoubleType.DOUBLE, 12.3e0));

        assertThat(JsonItems.fromText(utf8Slice("0e1000")).materializeScalar())
                .isEqualTo(new TypedValue(DoubleType.DOUBLE, 0e0));

        assertThat(JsonItems.fromText(utf8Slice("12")).materializeScalar())
                .isEqualTo(new TypedValue(IntegerType.INTEGER, 12L));
    }

    @Test
    void testRoundTripParity()
    {
        // The adapter output should match what JsonItemBuilder produces directly for
        // equivalent content.
        Json viaAdapter = JsonItems.fromJsonNode(parse("{\"k\": 1}"));
        Json viaBuilder = JsonItemBuilder.encodeObject(o -> o.bigint("k", 1));
        // BIGINT tag in builder vs INTEGER tag from Jackson — different scalar tags so
        // we explicitly check structural shape, not byte equality.
        assertThat(viaAdapter.objectSize()).isEqualTo(viaBuilder.objectSize());
        assertThat(viaAdapter.objectMember("k")).isPresent();
        assertThat(viaBuilder.objectMember("k")).isPresent();
    }

    @Test
    void testKeyOrderPreservedInEncoding()
    {
        // Insertion order is preserved in the encoding (separate concern from semantic
        // equality, which is order-independent per SQL:2023 §9.46).
        Json result = JsonItems.fromJsonNode(parse("{\"b\": 1, \"a\": 2}"));
        List<String> keys = new ArrayList<>();
        result.forEachObjectMember((k, _) -> keys.add(k));
        assertThat(keys).containsExactly("b", "a");
    }

    @Test
    void testStringWithUnicode()
    {
        Json result = JsonItems.fromJsonNode(parse("\"héllo\""));
        assertThat(result.scalarType()).isEqualTo(VARCHAR);
        // Verify the bytes round-trip via the encoded form.
        Json expected = JsonItemBuilder.encodeVarchar(utf8Slice("héllo"));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testArrayBelowIndexedThresholdUsesLinearForm()
    {
        Json result = JsonItems.fromJsonNode(parse("[1, 2, 3]"));
        // Linear ARRAY tag (3) — see ItemTag.ARRAY.encoded().
        assertThat(result.encoding().getByte(1)).isEqualTo(JsonItemEncoding.ItemTag.ARRAY.encoded());
        assertThat(result.arraySize()).isEqualTo(3);
    }

    @Test
    void testArrayAtOrAboveThresholdUsesIndexedForm()
    {
        // INDEXED_CONTAINER_THRESHOLD = 8: an 8-element array crosses into ARRAY_INDEXED.
        Json result = JsonItems.fromJsonNode(parse("[1, 2, 3, 4, 5, 6, 7, 8]"));
        assertThat(result.encoding().getByte(1)).isEqualTo(JsonItemEncoding.ItemTag.ARRAY_INDEXED.encoded());
        assertThat(result.arraySize()).isEqualTo(8);
        // O(1) random access via arrayElement should return the right values.
        for (int i = 0; i < 8; i++) {
            assertThat(result.arrayElement(i).materializeScalar().getLongValue()).isEqualTo(i + 1);
        }
    }

    @Test
    void testObjectAtOrAboveThresholdUsesIndexedForm()
    {
        Json result = JsonItems.fromJsonNode(parse(
                "{\"d\":1, \"a\":2, \"f\":3, \"b\":4, \"e\":5, \"c\":6, \"h\":7, \"g\":8}"));
        assertThat(result.encoding().getByte(1)).isEqualTo(JsonItemEncoding.ItemTag.OBJECT_INDEXED.encoded());
        assertThat(result.objectSize()).isEqualTo(8);
        // Insertion order preserved in iteration.
        List<String> keys = new ArrayList<>();
        result.forEachObjectMember((k, _) -> keys.add(k));
        assertThat(keys).containsExactly("d", "a", "f", "b", "e", "c", "h", "g");
        // O(log n) lookup hits the right value for each key (sort-permutation works).
        assertThat(result.objectMember("a").orElseThrow().materializeScalar().getLongValue()).isEqualTo(2);
        assertThat(result.objectMember("h").orElseThrow().materializeScalar().getLongValue()).isEqualTo(7);
        assertThat(result.objectMember("missing")).isEmpty();
    }

    @Test
    void testNestedIndexedArrays()
    {
        // Outer 8-element array containing inner 8-element arrays — both should be ARRAY_INDEXED.
        Json result = JsonItems.fromJsonNode(parse(
                "[[1,2,3,4,5,6,7,8], [9,10,11,12,13,14,15,16], [], [], [], [], [], []]"));
        assertThat(result.encoding().getByte(1)).isEqualTo(JsonItemEncoding.ItemTag.ARRAY_INDEXED.encoded());
        assertThat(result.arrayElement(0).arraySize()).isEqualTo(8);
        assertThat(result.arrayElement(0).arrayElement(7).materializeScalar().getLongValue()).isEqualTo(8);
    }

    @Test
    void testTreeFormAndJacksonFormProduceEqualBytes()
    {
        // The canonical encoding is deterministic: the same logical value encodes to the same
        // bytes whichever construction path built it, so a tree-form Json and a Jackson-form Json
        // of that value must produce identical bytes (and therefore equal / hash alike) regardless
        // of size.
        String[] inputs = {
                "null",
                "true",
                "[1, 2, 3]",
                "{\"a\":1, \"b\":2}",
                // Size at INDEXED threshold — both forms must agree on the INDEXED layout.
                "[1, 2, 3, 4, 5, 6, 7, 8]",
                "{\"d\":1, \"a\":2, \"f\":3, \"b\":4, \"e\":5, \"c\":6, \"h\":7, \"g\":8}",
                "[[1,2,3,4,5,6,7,8], [9,10,11,12,13,14,15,16]]",
        };
        for (String input : inputs) {
            Json jacksonForm = JsonItems.fromJsonNode(parse(input));
            Json treeForm = JsonItems.parseToTree(utf8Slice(input));
            assertThat(treeForm.encoding())
                    .as("encoding must match Jackson-form for: %s", input)
                    .isEqualTo(jacksonForm.encoding());
            assertThat(treeForm).isEqualTo(jacksonForm);
            assertThat(treeForm.hashCode()).isEqualTo(jacksonForm.hashCode());
        }
    }

    @Test
    void testTreeFormObjectWithDuplicateKeysFallsBackToPlain()
    {
        // Duplicate keys aren't representable in OBJECT_INDEXED (binary-search on the sort
        // permutation assumes uniqueness), so even a large duplicate-key object must emit
        // the plain OBJECT layout.
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < 10; i++) {
            json.append("\"k\":").append(i);
            if (i < 9) {
                json.append(",");
            }
        }
        json.append("}");
        Json treeForm = JsonItems.parseToTree(utf8Slice(json.toString()));
        assertThat(treeForm.encoding().getByte(1)).isEqualTo(JsonItemEncoding.ItemTag.OBJECT.encoded());
    }

    private static JsonNode parse(String text)
    {
        try {
            return MAPPER.readTree(text);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
