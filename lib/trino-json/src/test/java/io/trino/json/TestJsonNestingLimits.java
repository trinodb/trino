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

import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestJsonNestingLimits
{
    @Test
    void testReadWriteBoundary()
    {
        for (int depth : new int[] {1000, 1001, 1024}) {
            for (String text : nestedDocuments(depth)) {
                assertThat(JsonItems.toText(JsonItems.fromText(utf8Slice(text)))).isEqualTo(utf8Slice(text));
                assertThat(JsonItems.toText(JsonItems.parseToTree(utf8Slice(text)))).isEqualTo(utf8Slice(text));
            }
        }
    }

    @Test
    void testReadBeyondLimit()
    {
        for (String text : nestedDocuments(1025)) {
            assertThatThrownBy(() -> JsonItems.fromText(utf8Slice(text)))
                    .isInstanceOf(TrinoException.class)
                    .hasRootCauseInstanceOf(StreamConstraintsException.class);
            assertThatThrownBy(() -> JsonItems.parseToTree(utf8Slice(text)))
                    .isInstanceOf(TrinoException.class)
                    .hasRootCauseInstanceOf(StreamConstraintsException.class);
        }
    }

    @Test
    void testWriteBeyondLimit()
    {
        Json tree = JsonItemBuilder.JSON_NULL;
        for (int i = 0; i < 1025; i++) {
            tree = new JsonArray(List.of(tree));
        }
        Json json = tree;
        assertThatThrownBy(() -> JsonItems.toText(json))
                .isInstanceOf(IllegalStateException.class)
                .rootCause()
                .isInstanceOf(StreamConstraintsException.class)
                .hasMessageContaining("1024");
    }

    private static List<String> nestedDocuments(int depth)
    {
        return List.of(
                "[".repeat(depth) + "1" + "]".repeat(depth),
                "{\"x\":".repeat(depth) + "1" + "}".repeat(depth));
    }
}
