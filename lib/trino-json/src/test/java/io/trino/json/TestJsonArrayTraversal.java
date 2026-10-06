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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestJsonArrayTraversal
{
    @Test
    void testIterator()
    {
        for (String text : List.of("[]", "[1,[2,null,\"variable width text\"],{},[]]")) {
            List<Json> expected = new ArrayList<>();
            JsonItems.parseToTree(utf8Slice(text)).forEachArrayElement(expected::add);
            for (Json array : representations(text)) {
                Iterator<Json> iterator = array.arrayIterator();
                List<Json> actual = new ArrayList<>();
                iterator.forEachRemaining(actual::add);
                assertThat(actual).containsExactlyElementsOf(expected);
                assertThat(iterator.hasNext()).isFalse();
                assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);

                Iterator<Json> first = array.arrayIterator();
                Iterator<Json> second = array.arrayIterator();
                for (Json element : expected) {
                    assertThat(first.hasNext()).isTrue();
                    assertThat(first.next()).isEqualTo(element);
                    assertThat(second.next()).isEqualTo(element);
                }
                assertThat(first.hasNext()).isFalse();
                assertThat(second.hasNext()).isFalse();
            }
        }
    }

    @Test
    void testNonArrayIterator()
    {
        for (String text : List.of("null", "1", "{}")) {
            assertThatThrownBy(() -> JsonItems.fromText(utf8Slice(text)).arrayIterator())
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> JsonItems.parseToTree(utf8Slice(text)).arrayIterator())
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void testEqualityAndHashAcrossEncodings()
    {
        for (Json left : representations("[1,[2,null,\"text\"],{},[]]")) {
            // Different numeric scalar types bypass the identical-byte equality fast path.
            for (Json right : representations("[1.0,[2.00,null,\"text\"],{},[]]")) {
                assertThat(JsonItemSemantics.equal(left, right)).isTrue();
                assertThat(JsonItemSemantics.equal(right, left)).isTrue();
                assertThat(JsonItemSemantics.hash(left)).isEqualTo(JsonItemSemantics.hash(right));
            }
            for (String different : List.of("[]", "[1,[2,null,\"text\"],{}]", "[2,[2,null,\"text\"],{},[]]", "[1,[2,null,\"text\"],{},[0]]")) {
                for (Json right : representations(different)) {
                    assertThat(JsonItemSemantics.equal(left, right)).isFalse();
                    assertThat(JsonItemSemantics.equal(right, left)).isFalse();
                }
            }
        }
        for (Json empty : representations("[]")) {
            assertThat(JsonItemSemantics.hash(empty)).isEqualTo(1);
        }
    }

    private static List<Json> representations(String text)
    {
        Json plain = JsonItems.fromText(utf8Slice(text));
        Json indexed = JsonItemBuilder.encode(writer -> {
            writer.startIndexedArray();
            plain.forEachArrayElement(writer::nest);
            writer.endIndexedArray();
        });
        Json nested = JsonItemBuilder.encode(writer -> writer.startArray().bigint(0).nest(indexed).endArray())
                .arrayElement(1);
        return List.of(plain, indexed, nested, JsonItems.parseToTree(utf8Slice(text)), Json.unchecked(utf8Slice(text)));
    }
}
