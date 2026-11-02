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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.json.Json;
import io.trino.json.JsonBlock;
import io.trino.json.JsonBlockBuilder;
import io.trino.json.JsonItemBuilder;
import io.trino.json.JsonItems;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.type.JsonType.JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJsonType
        extends AbstractTestType
{
    public TestJsonType()
    {
        super(JSON, String.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = JSON.createBlockBuilder(null, 1);
        Slice slice = Slices.utf8Slice("{\"x\":1, \"y\":2}");
        JSON.writeSlice(blockBuilder, slice);
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return null;
    }

    @Test
    public void testFlatPayloads()
            throws Throwable
    {
        TypeOperators operators = new TypeOperators();
        MethodHandle writeStack = operators.getReadValueOperator(JSON, simpleConvention(FLAT_RETURN, NEVER_NULL));
        MethodHandle writeBlock = operators.getReadValueOperator(JSON, simpleConvention(FLAT_RETURN, VALUE_BLOCK_POSITION));
        MethodHandle read = operators.getReadValueOperator(JSON, simpleConvention(FAIL_ON_NULL, FLAT));
        List<Json> values = List.of(
                Json.unchecked(Slices.utf8Slice("1")),
                Json.unchecked(Slices.utf8Slice(" {\"a\":1,\"b\":[1,2,3,4,5,6,7,8]} ")),
                JsonItems.parseToTree(Slices.utf8Slice("[1,2,3,4,5,6,7,8]")),
                JsonItemBuilder.encodeDouble(1.0));
        for (Json value : values) {
            JsonBlockBuilder builder = JSON.createBlockBuilder(null, 1);
            builder.appendJson(value);
            JsonBlock block = builder.buildValueBlock();
            for (boolean fromBlock : new boolean[] {false, true}) {
                byte[] fixed = new byte[JSON.getFlatFixedSize() + 3];
                int variableLength = JSON.getFlatVariableWidthSize(block, 0);
                byte[] variable = new byte[variableLength + 7];
                if (fromBlock) {
                    writeBlock.invoke(block, 0, fixed, 3, variable, 7);
                }
                else {
                    writeStack.invoke(value, fixed, 3, variable, 7);
                }
                Json restored = (Json) read.invoke(fixed, 3, variable, 7);
                assertThat(restored).isEqualTo(value);
                assertThat(restored.isRawText()).isEqualTo(value.isRawText());
                if (value.isRawText()) {
                    assertThat(restored.rawText()).isEqualTo(value.rawText());
                }
                else {
                    assertThat(restored.encoding()).isEqualTo(value.encoding());
                }
                assertThat(JSON.getFlatVariableWidthLength(fixed, 3)).isEqualTo(variableLength);
            }
        }
    }

    @Test
    public void testRange()
    {
        assertThat(type.getRange())
                .isEmpty();
    }

    @Test
    public void testPreviousValue()
    {
        assertThatThrownBy(() -> type.getPreviousValue(getSampleValue()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Type is not orderable: " + type);
    }

    @Test
    public void testNextValue()
    {
        assertThatThrownBy(() -> type.getNextValue(getSampleValue()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Type is not orderable: " + type);
    }
}
