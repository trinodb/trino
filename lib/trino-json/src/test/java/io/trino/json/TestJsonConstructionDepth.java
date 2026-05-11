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

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.json.JsonItemBuilder.JSON_NULL;
import static io.trino.json.JsonItemBuilder.checkNestingDepth;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestJsonConstructionDepth
{
    @Test
    void testStreamingConstruction()
    {
        for (boolean indexed : List.of(false, true)) {
            Json boundary = nestedArrays(1024, 1024, indexed);
            assertThatCode(() -> checkNestingDepth(boundary, 1024)).doesNotThrowAnyException();
            assertThatThrownBy(() -> nestedArrays(1025, 1024, indexed))
                    .isInstanceOf(JsonNestingDepthException.class)
                    .hasMessageContaining("1024");
            assertThatCode(() -> nestedArrays(1025, Integer.MAX_VALUE, indexed)).doesNotThrowAnyException();
        }
        assertThat(JsonItemBuilder.encodeWithDepthLimit(writer -> writer.nullValue(), 0).isNull()).isTrue();
        assertThatThrownBy(() -> JsonItemBuilder.encodeArray(_ -> {}, 0))
                .isInstanceOf(JsonNestingDepthException.class);
    }

    @Test
    void testNestedExistingValues()
    {
        Json tree = JSON_NULL;
        for (int i = 0; i < 1023; i++) {
            tree = new JsonArray(List.of(tree));
        }
        for (Json child : List.of(tree, nestedArrays(1023, Integer.MAX_VALUE, false), nestedArrays(1023, Integer.MAX_VALUE, true))) {
            Json wrapped = JsonItemBuilder.encodeObject(object -> object.nest("x", child), 1024);
            assertThatCode(() -> checkNestingDepth(wrapped, 1024)).doesNotThrowAnyException();
            assertThatThrownBy(() -> JsonItemBuilder.encodeArray(array -> array.nest(wrapped), 1024))
                    .isInstanceOf(JsonNestingDepthException.class)
                    .hasMessageContaining("1024");
        }
    }

    @Test
    void testObjectTraversal()
    {
        Json nested = JsonItemBuilder.encodeArray(array -> array.nullValue());
        Json tree = new JsonObject(List.of(
                new JsonObjectMember(utf8Slice("x"), JSON_NULL),
                new JsonObjectMember(utf8Slice("x"), nested)));
        for (Json object : List.of(tree, Json.of(tree.encoding()), Json.unchecked(utf8Slice("{\"x\":null,\"x\":[null]}")), JsonItemBuilder.encode(writer -> writer
                .startIndexedObject().fieldName("x").nullValue().fieldName("y").nest(nested).endIndexedObject()))) {
            assertThat(object.objectValueIterator()).toIterable().containsExactly(JSON_NULL, nested);
            assertThatCode(() -> checkNestingDepth(object, 2)).doesNotThrowAnyException();
            assertThatThrownBy(() -> checkNestingDepth(object, 1)).isInstanceOf(JsonNestingDepthException.class);
            assertThatThrownBy(() -> JsonItemBuilder.encodeObject(writer -> writer.nest("x", object), 2))
                    .isInstanceOf(JsonNestingDepthException.class);
        }
        assertThatCode(() -> JsonItemBuilder.encodeObject(_ -> {}, 1)).doesNotThrowAnyException();
    }

    @Test
    void testDeepTreeFailsWithoutStackOverflow()
    {
        Json tree = JSON_NULL;
        for (int i = 0; i < 10_000; i++) {
            tree = new JsonArray(List.of(tree));
        }
        Json input = tree;
        assertThatThrownBy(() -> JsonItemBuilder.encodeArray(array -> array.nest(input), 1024))
                .isInstanceOf(JsonNestingDepthException.class);
    }

    private static Json nestedArrays(int depth, int limit, boolean indexed)
    {
        return JsonItemBuilder.encodeWithDepthLimit(writer -> {
            for (int i = 0; i < depth; i++) {
                if (indexed) {
                    writer.startIndexedArray();
                }
                else {
                    writer.startArray();
                }
            }
            writer.nullValue();
            for (int i = 0; i < depth; i++) {
                if (indexed) {
                    writer.endIndexedArray();
                }
                else {
                    writer.endArray();
                }
            }
        }, limit);
    }
}
