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

import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestTypedValue
{
    @Test
    void testIsJsonItem()
    {
        Json item = new TypedValue(BIGINT, 1L);
        assertThat(item).isInstanceOf(TypedValue.class);
    }

    @Test
    void testRecordEquality()
    {
        TypedValue a = new TypedValue(BIGINT, 1L);
        TypedValue b = new TypedValue(BIGINT, 1L);
        TypedValue c = new TypedValue(BIGINT, 2L);
        TypedValue d = new TypedValue(INTEGER, 1L);

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a).isNotEqualTo(c);

        // A scalar is the number it denotes: the SQL type carrying it is not part of the JSON
        // value, so BIGINT 1 and INTEGER 1 are the same value.
        assertThat(a).isEqualTo(d);
        assertThat(a.hashCode()).isEqualTo(d.hashCode());
    }

    @Test
    void testRecordAccessors()
    {
        TypedValue v = new TypedValue(BIGINT, 1L);
        assertThat(v.type()).isEqualTo(BIGINT);
        assertThat(v.value()).isEqualTo(1L);
    }

    @Test
    void testFromValueAsObject()
    {
        assertThat(TypedValue.fromValueAsObject(BIGINT, 1L))
                .isEqualTo(new TypedValue(BIGINT, 1L));
        assertThat(TypedValue.fromValueAsObject(VARCHAR, utf8Slice("abc")))
                .isEqualTo(new TypedValue(VARCHAR, utf8Slice("abc")));
    }

    @Test
    void testTypeMismatchInPrimitiveConstructorThrows()
    {
        assertThatThrownBy(() -> new TypedValue(VARCHAR, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("long value does not match");
    }

    @Test
    void testTypeMismatchInObjectConstructorThrows()
    {
        assertThatThrownBy(() -> new TypedValue(BIGINT, "not a long"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match the type bigint");
    }

    @Test
    void testEqualsWithNonEncodableTypeDoesNotThrow()
    {
        // DATE has no wire encoding (path-engine-only scalar). Cross-permit equals
        // must return false without invoking the encoder — Object.equals is documented
        // never to throw on a foreign-class argument, and symmetry between
        // dateTyped.equals(jsonNull) and jsonNull.equals(dateTyped) must hold.
        Json dateTyped = new TypedValue(DATE, 1L);
        Json jsonNull = JsonNullValue.INSTANCE;
        Json jsonError = JsonErrorValue.INSTANCE;
        Json encodedBigint = JsonItemBuilder.encodeBigint(1L);

        assertThat(dateTyped.equals(jsonNull)).isFalse();
        assertThat(jsonNull.equals(dateTyped)).isFalse();

        assertThat(dateTyped.equals(jsonError)).isFalse();
        assertThat(jsonError.equals(dateTyped)).isFalse();

        assertThat(dateTyped.equals(encodedBigint)).isFalse();
        assertThat(encodedBigint.equals(dateTyped)).isFalse();

        // Symmetric self-equality across the non-encodable type still works field-wise.
        assertThat(dateTyped).isEqualTo(new TypedValue(DATE, 1L));
        assertThat(dateTyped).isNotEqualTo(new TypedValue(DATE, 2L));
    }

    @Test
    void testGetObjectValueOnPrimitiveTypeThrows()
    {
        TypedValue v = new TypedValue(BIGINT, 1L);
        assertThatThrownBy(v::getObjectValue)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("call another method");
    }

    @Test
    void testJsonAlsoImplementsJsonItem()
    {
        Json json = JsonItemBuilder.encodeBigint(42);
        Json item = json;
        assertThat(item).isInstanceOf(Json.class);
    }

    @Test
    void testSealedSwitchExhaustive()
    {
        // The sealed-interface contract: pattern-match exhaustively on Json's permits.
        // Compiler will fail if a new permit is added without updating this switch.
        Json bigint = JsonItemBuilder.encodeBigint(42);
        Json typed = new TypedValue(BIGINT, 1L);

        assertThat(describe(bigint)).isEqualTo("EncodedJson");
        assertThat(describe(typed)).isEqualTo("TypedValue");
    }

    private static String describe(Json item)
    {
        return switch (item) {
            case EncodedJson _ -> "EncodedJson";
            case JsonObject _ -> "JsonObject";
            case JsonArray _ -> "JsonArray";
            case JsonNullValue _ -> "JsonNullValue";
            case JsonErrorValue _ -> "JsonErrorValue";
            case TypedValue _ -> "TypedValue";
        };
    }
}
