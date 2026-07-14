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
package io.trino.type;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import io.trino.json.JsonItems;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.util.JsonUtil.BlockBuilderAppender;
import io.trino.util.JsonUtil.StreamingBlockBuilderAppender;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.StructuralTestUtil.mapType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestJsonStreamingCasts
{
    @Test
    void testInvalidInput()
    {
        for (String input : List.of("[1e400]", "[0." + "0".repeat(20000) + "1]", "\uFEFF[1]", "\0[\0001\0]")) {
            assertThatThrownBy(() -> stream(new ArrayType(DOUBLE), input))
                    .isInstanceOf(JsonParseException.class);
        }
        assertThatThrownBy(() -> stream(new ArrayType(VARCHAR), "[\"\\ud800\"]"))
                .isInstanceOf(JsonParseException.class)
                .hasMessageContaining("unpaired surrogate");
        assertThatThrownBy(() -> stream(mapType(VARCHAR, DOUBLE), "{\"\\udc00\":1}"))
                .isInstanceOf(JsonParseException.class)
                .hasMessageContaining("unpaired surrogate");
    }

    @Test
    void testValueModelParity()
            throws IOException
    {
        assertEquivalent(new ArrayType(DOUBLE), "[0, -0, 0.0, -0.0, -0e0, 1e-400, 1.2345, 12345678901234567890123456789012345678901234567890]");
        assertEquivalent(new ArrayType(VARCHAR), "[\"\\ud83d\\ude00\", 123, 1.25]");
        assertEquivalent(mapType(VARCHAR, DOUBLE), "{\"\\ud83d\\ude00\":1.25}");
    }

    private static void assertEquivalent(Type type, String input)
            throws IOException
    {
        BlockBuilder expected = type.createBlockBuilder(null, 1);
        BlockBuilderAppender.createBlockBuilderAppender(type).append(JsonItems.fromText(utf8Slice(input)), expected);
        assertBlockEquals(type, stream(type, input), expected.build());
    }

    private static Block stream(Type type, String input)
            throws IOException
    {
        BlockBuilder result = type.createBlockBuilder(null, 1);
        try (JsonParser parser = JsonItems.createStreamingParser(utf8Slice(input))) {
            parser.nextToken();
            StreamingBlockBuilderAppender.create(type).append(parser, result);
            assertThat(parser.nextToken()).isNull();
        }
        return result.build();
    }
}
