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

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.TrinoNumber;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/// Pins the SQL/JSON equality contract: scalars compare by value (a number is a number,
/// whatever SQL type carries it), arrays compare in order, object members compare as a
/// multiset, and equals/hashCode agree with all of it.
class TestJsonItemSemantics
{
    @Test
    void testScalarEqualityIsByValue()
    {
        Json integerOne = JsonItemBuilder.encode(w -> w.integerValue(1));
        Json bigintOne = JsonItemBuilder.encodeBigint(1L);
        Json doubleOne = JsonItemBuilder.encodeDouble(1.0);
        Json decimalOne = JsonItemBuilder.encodeNumber(TrinoNumber.from(new BigDecimal("1.00")));

        assertThat(JsonItemSemantics.equal(integerOne, JsonItemBuilder.encode(w -> w.integerValue(1)))).isTrue();

        // A JSON number is a number: the SQL type carrying it is not part of the value, so 1,
        // 1, 1.0 and 1.00 are one value however they arrived.
        assertEqualAndHashesAlike(integerOne, bigintOne);
        assertEqualAndHashesAlike(integerOne, doubleOne);
        assertEqualAndHashesAlike(integerOne, decimalOne);

        // Different numbers stay different.
        assertThat(JsonItemSemantics.equal(integerOne, JsonItemBuilder.encodeBigint(2L))).isFalse();
        assertThat(JsonItemSemantics.equal(integerOne, JsonItemBuilder.encodeDouble(1.5))).isFalse();

        // A number is not a string, whatever it looks like.
        assertThat(JsonItemSemantics.equal(integerOne, JsonItemBuilder.encodeVarchar(utf8Slice("1")))).isFalse();
    }

    @Test
    void testFloatingPointEqualityFollowsGrouping()
    {
        // equals/hashCode back GROUP BY and DISTINCT, so they use grouping semantics: NaN is
        // equal to itself and -0.0 is equal to 0.0, which is where grouping parts from `=`.
        assertEqualAndHashesAlike(JsonItemBuilder.encodeDouble(Double.NaN), JsonItemBuilder.encodeDouble(Double.NaN));
        assertEqualAndHashesAlike(JsonItemBuilder.encodeDouble(-0.0), JsonItemBuilder.encodeDouble(0.0));

        // Infinities keep their sign.
        assertEqualAndHashesAlike(JsonItemBuilder.encodeDouble(Double.POSITIVE_INFINITY), JsonItemBuilder.encodeDouble(Double.POSITIVE_INFINITY));
        assertThat(JsonItemSemantics.equal(
                JsonItemBuilder.encodeDouble(Double.POSITIVE_INFINITY),
                JsonItemBuilder.encodeDouble(Double.NEGATIVE_INFINITY)))
                .isFalse();
        assertThat(JsonItemSemantics.equal(
                JsonItemBuilder.encodeDouble(Double.NaN),
                JsonItemBuilder.encodeDouble(Double.POSITIVE_INFINITY)))
                .isFalse();

        // A very large integer keeps its identity rather than collapsing through double.
        Json huge = JsonItemBuilder.encodeNumber(TrinoNumber.from(new BigDecimal("12345678901234567890")));
        assertThat(JsonItemSemantics.equal(huge, JsonItemBuilder.encodeNumber(TrinoNumber.from(new BigDecimal("12345678901234567891"))))).isFalse();
    }

    @Test
    void testCharacterStringEqualityIsTransitive()
    {
        // A char and a varchar holding the same characters are one character-string value: a
        // char carries no significant trailing spaces. The wire encoding already conflates them,
        // so comparing (type, value) here instead would give an equality that isn't transitive:
        // char == encoded == varchar, yet char != varchar.
        TypedValue charValue = new TypedValue(createCharType(5), utf8Slice("ab"));
        TypedValue varcharValue = new TypedValue(VARCHAR, utf8Slice("ab"));
        Json encoded = Json.of(varcharValue.encoding());

        assertEqualAndHashesAlike(charValue, varcharValue);
        assertEqualAndHashesAlike(charValue, encoded);
        assertEqualAndHashesAlike(varcharValue, encoded);

        assertThat(JsonItemSemantics.equal(charValue, JsonItemBuilder.encodeVarchar(utf8Slice("abc")))).isFalse();
    }

    private static void assertEqualAndHashesAlike(Json left, Json right)
    {
        assertThat(JsonItemSemantics.equal(left, right)).isTrue();
        assertThat(JsonItemSemantics.equal(right, left)).isTrue();
        assertThat(JsonItemSemantics.hash(left)).isEqualTo(JsonItemSemantics.hash(right));
        // equals/hashCode must agree with the SQL rule, or a hash index would lose the value.
        assertThat(left).isEqualTo(right);
        assertThat(left.hashCode()).isEqualTo(right.hashCode());
    }

    @Test
    void testJsonNullEqual()
    {
        assertThat(JsonItemSemantics.equal(JsonItemBuilder.JSON_NULL, JsonItemBuilder.JSON_NULL)).isTrue();
    }

    @Test
    void testObjectMultisetEquality()
    {
        // Order-independent: {a:1, b:2} equals {b:2, a:1}.
        Json ab = JsonItemBuilder.encode(w -> w.startObject().fieldName("a").bigint(1).fieldName("b").bigint(2).endObject());
        Json ba = JsonItemBuilder.encode(w -> w.startObject().fieldName("b").bigint(2).fieldName("a").bigint(1).endObject());
        assertThat(JsonItemSemantics.equal(ab, ba)).isTrue();
        assertThat(JsonItemSemantics.hash(ab)).isEqualTo(JsonItemSemantics.hash(ba));

        // Duplicate-keyed bags compare as multisets per §9.42 WITHOUT UNIQUE KEYS.
        Json aa12 = JsonItemBuilder.encode(w -> w.startObject().fieldName("a").bigint(1).fieldName("a").bigint(2).endObject());
        Json aa21 = JsonItemBuilder.encode(w -> w.startObject().fieldName("a").bigint(2).fieldName("a").bigint(1).endObject());
        Json aa11 = JsonItemBuilder.encode(w -> w.startObject().fieldName("a").bigint(1).fieldName("a").bigint(1).endObject());
        assertThat(JsonItemSemantics.equal(aa12, aa21)).isTrue();
        assertThat(JsonItemSemantics.equal(aa12, aa11)).isFalse();
    }

    @Test
    void testArrayOrderedEquality()
    {
        // Arrays compare element-wise; order matters.
        Json ab = JsonItemBuilder.encode(w -> w.startArray().bigint(1).bigint(2).endArray());
        Json ba = JsonItemBuilder.encode(w -> w.startArray().bigint(2).bigint(1).endArray());
        assertThat(JsonItemSemantics.equal(ab, ab)).isTrue();
        assertThat(JsonItemSemantics.equal(ab, ba)).isFalse();
    }

    @Test
    void testCrossPermitEqualsSymmetry()
    {
        // A TypedValue scalar and an EncodedJson wrapping its bytes must compare equal in
        // both directions and hash the same, per Object.equals' symmetry contract. Same
        // requirement for tree-form vs encoded-form objects.
        Json typedScalar = JsonItemBuilder.encodeBigint(42L);
        Json encodedScalar = Json.of(typedScalar.encoding());
        assertThat(typedScalar.equals(encodedScalar)).isTrue();
        assertThat(encodedScalar.equals(typedScalar)).isTrue();
        assertThat(typedScalar.hashCode()).isEqualTo(encodedScalar.hashCode());

        Json treeObject = JsonItemBuilder.encode(w -> w.startObject().fieldName("a").bigint(1).endObject());
        Json encodedObject = Json.of(treeObject.encoding());
        assertThat(treeObject.equals(encodedObject)).isTrue();
        assertThat(encodedObject.equals(treeObject)).isTrue();
        assertThat(treeObject.hashCode()).isEqualTo(encodedObject.hashCode());

        // JsonItemSemantics.equal agrees on the byte-canonical fast path.
        assertThat(JsonItemSemantics.equal(typedScalar, encodedScalar)).isTrue();
        assertThat(JsonItemSemantics.equal(encodedScalar, typedScalar)).isTrue();
    }

    @Test
    void testObjectMemberMixedPermitEquality()
    {
        // An object whose member is a TypedValue and the same object materialized through
        // EncodedJson must compare equal under the multiset rule.
        Json treeObject = JsonItemBuilder.encode(w -> w.startObject()
                .fieldName("a").bigint(1)
                .fieldName("b").bigint(2)
                .endObject());
        Json encodedObject = Json.of(treeObject.encoding());

        assertThat(JsonItemSemantics.equal(treeObject, encodedObject)).isTrue();
        assertThat(JsonItemSemantics.hash(treeObject)).isEqualTo(JsonItemSemantics.hash(encodedObject));
    }

    @Test
    @Timeout(20)
    void testRepeatedKeyEqualityStaysNearLinear()
    {
        // WITHOUT UNIQUE KEYS lets a key repeat arbitrarily many times, and equality is on the
        // GROUP BY / DISTINCT / join path. Two objects with the same key repeated with distinct
        // values -- reversed on the right, so a per-value linear scan would match late -- must
        // still compare in near-linear time rather than O(n^2).
        int count = 50_000;
        ImmutableList.Builder<JsonObjectMember> left = ImmutableList.builder();
        ImmutableList.Builder<JsonObjectMember> right = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
            left.add(new JsonObjectMember(utf8Slice("k"), JsonItemBuilder.encodeBigint(i)));
            right.add(new JsonObjectMember(utf8Slice("k"), JsonItemBuilder.encodeBigint(count - 1 - i)));
        }
        Json leftObject = new JsonObject(left.build());
        Json rightObject = new JsonObject(right.build());
        assertThat(JsonItemSemantics.equal(leftObject, rightObject)).isTrue();
        assertThat(JsonItemSemantics.hash(leftObject)).isEqualTo(JsonItemSemantics.hash(rightObject));

        // One differing value makes them unequal, and that must stay near-linear too.
        ImmutableList.Builder<JsonObjectMember> altered = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
            altered.add(new JsonObjectMember(utf8Slice("k"), JsonItemBuilder.encodeBigint(i == 0 ? count : i)));
        }
        assertThat(JsonItemSemantics.equal(leftObject, new JsonObject(altered.build()))).isFalse();
    }
}
