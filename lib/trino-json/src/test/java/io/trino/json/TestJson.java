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
import io.trino.spi.type.Int128;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Verifies the encoded model: builder roundtrips, Json accessors, structural-misuse
/// detection, ARRAY_INDEXED / OBJECT_INDEXED behavior at the threshold boundary.
class TestJson
{
    @Test
    void testNullConstant()
    {
        assertThat(JsonItemBuilder.JSON_NULL.kind()).isEqualTo(Json.Kind.NULL);
        assertThat(JsonItemBuilder.JSON_NULL.isNull()).isTrue();
    }

    @Test
    void testErrorConstant()
    {
        assertThat(JsonItemBuilder.JSON_ERROR.kind()).isEqualTo(Json.Kind.ERROR);
        assertThat(JsonItemBuilder.JSON_ERROR.isError()).isTrue();
    }

    @Test
    void testScalarKindsAndAccessors()
    {
        assertThat(JsonItemBuilder.encodeBoolean(true).kind()).isEqualTo(Json.Kind.SCALAR);
        assertThat(JsonItemBuilder.encodeBoolean(true).scalarType()).isEqualTo(JsonItemEncoding.TypeTag.BOOLEAN);
        assertThat(JsonItemBuilder.encodeBigint(42).scalarType()).isEqualTo(JsonItemEncoding.TypeTag.BIGINT);
        assertThat(JsonItemBuilder.encodeDouble(3.14).scalarType()).isEqualTo(JsonItemEncoding.TypeTag.DOUBLE);
        assertThat(JsonItemBuilder.encodeVarchar(utf8Slice("hello")).scalarType()).isEqualTo(JsonItemEncoding.TypeTag.VARCHAR);
    }

    @Test
    void testArrayAccessors()
    {
        Json array = JsonItemBuilder.encodeArray(a -> a
                .bigint(1)
                .bigint(2)
                .bigint(3));

        assertThat(array.isArray()).isTrue();
        assertThat(array.arraySize()).isEqualTo(3);

        for (int i = 0; i < 3; i++) {
            Json element = array.arrayElement(i);
            assertThat(element.kind()).isEqualTo(Json.Kind.SCALAR);
            assertThat(element.scalarType()).isEqualTo(JsonItemEncoding.TypeTag.BIGINT);
        }

        List<Json> collected = new ArrayList<>();
        array.forEachArrayElement(collected::add);
        assertThat(collected).hasSize(3);
    }

    @Test
    void testObjectAccessors()
    {
        Json object = JsonItemBuilder.encodeObject(o -> o
                .bigint("a", 1)
                .booleanValue("b", true)
                .nullValue("c"));

        assertThat(object.isObject()).isTrue();
        assertThat(object.objectSize()).isEqualTo(3);

        assertThat(object.objectMember("a")).isPresent()
                .get()
                .satisfies(j -> assertThat(j.scalarType()).isEqualTo(JsonItemEncoding.TypeTag.BIGINT));
        assertThat(object.objectMember("b")).isPresent()
                .get()
                .satisfies(j -> assertThat(j.scalarType()).isEqualTo(JsonItemEncoding.TypeTag.BOOLEAN));
        assertThat(object.objectMember("c")).isPresent()
                .get()
                .satisfies(j -> assertThat(j.kind()).isEqualTo(Json.Kind.NULL));
        assertThat(object.objectMember("missing")).isEmpty();

        List<String> keys = new ArrayList<>();
        object.forEachObjectMember((k, _) -> keys.add(k));
        assertThat(keys).containsExactly("a", "b", "c");
    }

    @Test
    void testNestedSingleBuffer()
    {
        // Verifies that nested containers don't allocate intermediate slices: the inner
        // object writes directly into the outer array's buffer.
        Json outer = JsonItemBuilder.encodeArray(a -> a
                .object(o -> o.bigint("a", 1))
                .object(o -> o.bigint("b", 2)));

        assertThat(outer.arraySize()).isEqualTo(2);
        assertThat(outer.arrayElement(0).objectMember("a")).isPresent();
        assertThat(outer.arrayElement(1).objectMember("b")).isPresent();
    }

    @Test
    void testNestEmbedsExistingItem()
    {
        Json inner = JsonItemBuilder.encodeBigint(42);
        Json outer = JsonItemBuilder.encodeArray(a -> a.nest(inner));

        assertThat(outer.arraySize()).isEqualTo(1);
        assertThat(outer.arrayElement(0).scalarType()).isEqualTo(JsonItemEncoding.TypeTag.BIGINT);
    }

    @Test
    void testStatefulWriter()
    {
        Json item = JsonItemBuilder.encode(w -> w
                .startObject()
                .fieldName("k").bigint(1)
                .endObject());

        assertThat(item.objectSize()).isEqualTo(1);
        assertThat(item.objectMember("k")).isPresent();
    }

    @Test
    void testEqualityByEncodedBytes()
    {
        Json a = JsonItemBuilder.encodeObject(o -> o.bigint("k", 1));
        Json b = JsonItemBuilder.encodeObject(o -> o.bigint("k", 1));
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void testEncodingReturnsSelfContainedSlice()
    {
        Json outer = JsonItemBuilder.encodeArray(a -> a.bigint(42));
        Json inner = outer.arrayElement(0);

        // The inner view's encoding() copies bytes (with VERSION) so it can be re-wrapped
        // as a standalone Json.
        Json roundTripped = Json.of(inner.encoding());
        assertThat(roundTripped.scalarType()).isEqualTo(JsonItemEncoding.TypeTag.BIGINT);
        assertThat(roundTripped).isEqualTo(inner);
    }

    @Test
    void testUnclosedContainerThrows()
    {
        assertThatThrownBy(() -> JsonItemBuilder.encode(w -> w.startArray().bigint(1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("open container");
    }

    @Test
    void testEndArrayOutsideArrayThrows()
    {
        assertThatThrownBy(() -> JsonItemBuilder.encode(w -> w.startObject().endArray()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("endArray");
    }

    @Test
    void testFieldNameOutsideObjectThrows()
    {
        assertThatThrownBy(() -> JsonItemBuilder.encode(w -> w.startArray().fieldName("k")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("fieldName");
    }

    @Test
    void testValueWithoutFieldNameInObjectThrows()
    {
        assertThatThrownBy(() -> JsonItemBuilder.encode(w -> w.startObject().bigint(1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("field name");
    }

    @Test
    void testTwoRootItemsThrow()
    {
        assertThatThrownBy(() -> JsonItemBuilder.encode(w -> w.bigint(1).bigint(2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("more than one root item");
    }

    @Test
    void testNoRootItemThrows()
    {
        assertThatThrownBy(() -> JsonItemBuilder.encode(_ -> {}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no root item");
    }

    @Test
    void testDuplicateObjectKeysPreserved()
    {
        Json object = JsonItemBuilder.encodeObject(o -> o
                .bigint("key", 1)
                .bigint("key", 2));

        assertThat(object.objectSize()).isEqualTo(2);

        List<Long> values = new ArrayList<>();
        object.objectMembers("key", _ -> values.add((long) values.size()));
        assertThat(values).hasSize(2);
    }

    @Test
    @Timeout(30)
    void testManyDuplicatesOfOneKey()
    {
        // WITHOUT UNIQUE KEYS makes this a legal object, and raw connector JSON reaches the
        // member index on first structural access — so it has to stay linear. Indexing that
        // copied a growing array per duplicate would spend O(n^2) here.
        int count = 100_000;
        ImmutableList.Builder<JsonObjectMember> members = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
            members.add(new JsonObjectMember(utf8Slice("key"), JsonItemBuilder.encodeBigint(i)));
        }

        JsonObject object = new JsonObject(members.build());
        assertThat(object.objectSize()).isEqualTo(count);
        assertThat(object.hasDuplicateKeys()).isTrue();

        List<Json> seen = new ArrayList<>();
        object.objectMembers(utf8Slice("key"), seen::add);
        assertThat(seen).hasSize(count);
        // Members keep their original order.
        assertThat(seen.get(0)).isEqualTo(JsonItemBuilder.encodeBigint(0));
        assertThat(seen.get(count - 1)).isEqualTo(JsonItemBuilder.encodeBigint(count - 1));
    }

    @Test
    void testMaterializeScalar()
    {
        assertThat(JsonItemBuilder.encodeBoolean(true).materializeScalar())
                .isEqualTo(new TypedValue(BOOLEAN, true));
        assertThat(JsonItemBuilder.encodeBigint(42).materializeScalar())
                .isEqualTo(new TypedValue(BIGINT, 42L));
        assertThat(JsonItemBuilder.encodeDouble(3.5).materializeScalar())
                .isEqualTo(new TypedValue(DOUBLE, 3.5));
        assertThat(JsonItemBuilder.encodeVarchar(utf8Slice("abc")).materializeScalar())
                .isEqualTo(new TypedValue(VARCHAR, utf8Slice("abc")));

        // INTEGER, SMALLINT, TINYINT, REAL via the stateful writer
        Json integer = JsonItemBuilder.encode(w -> w.integerValue(7));
        assertThat(integer.materializeScalar())
                .isEqualTo(new TypedValue(INTEGER, 7L));

        Json smallint = JsonItemBuilder.encode(w -> w.smallintValue(7));
        assertThat(smallint.materializeScalar())
                .isEqualTo(new TypedValue(SMALLINT, 7L));

        Json tinyint = JsonItemBuilder.encode(w -> w.tinyintValue(7));
        assertThat(tinyint.materializeScalar())
                .isEqualTo(new TypedValue(TINYINT, 7L));

        Json real = JsonItemBuilder.encode(w -> w.realBits(Float.floatToRawIntBits(2.5f)));
        assertThat(real.materializeScalar())
                .isEqualTo(new TypedValue(REAL, (long) Float.floatToRawIntBits(2.5f)));

        Json shortDecimal = JsonItemBuilder.encodeShortDecimal(5, 2, 12345L);
        assertThat(shortDecimal.materializeScalar())
                .isEqualTo(new TypedValue(createDecimalType(5, 2), 12345L));

        Json longDecimal = JsonItemBuilder.encodeLongDecimal(30, 10, Int128.valueOf("12345678901234567890"));
        assertThat(longDecimal.materializeScalar())
                .isEqualTo(new TypedValue(createDecimalType(30, 10), Int128.valueOf("12345678901234567890")));
    }

    @Test
    void testDeeplyNestedArrayDoesNotStackOverflow()
    {
        // 200 levels — well above any typical input but below MAX_DEPTH (1000).
        Json item = JsonItemBuilder.encode(w -> {
            for (int i = 0; i < 200; i++) {
                w.startArray();
            }
            w.bigint(0);
            for (int i = 0; i < 200; i++) {
                w.endArray();
            }
        });

        Json cursor = item;
        for (int i = 0; i < 200; i++) {
            assertThat(cursor.isArray()).isTrue();
            assertThat(cursor.arraySize()).isEqualTo(1);
            cursor = cursor.arrayElement(0);
        }
        assertThat(cursor.scalarType()).isEqualTo(JsonItemEncoding.TypeTag.BIGINT);
    }
}
