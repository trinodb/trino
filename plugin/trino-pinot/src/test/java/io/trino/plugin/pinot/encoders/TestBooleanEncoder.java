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
package io.trino.plugin.pinot.encoders;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBooleanEncoder
{
    @Test
    public void testBooleanEncodingReturnsIntegers()
    {
        BooleanEncoder encoder = new BooleanEncoder(BOOLEAN);
        BlockBuilder blockBuilder = BOOLEAN.createBlockBuilder(null, 3);

        // Add test values
        BOOLEAN.writeBoolean(blockBuilder, true);
        BOOLEAN.writeBoolean(blockBuilder, false);
        blockBuilder.appendNull();

        Block block = blockBuilder.build();

        // Test true value returns 1
        Object trueValue = encoder.encode(block, 0);
        assertThat(trueValue).isInstanceOf(Integer.class);
        assertThat(trueValue).isEqualTo(1);

        // Test false value returns 0
        Object falseValue = encoder.encode(block, 1);
        assertThat(falseValue).isInstanceOf(Integer.class);
        assertThat(falseValue).isEqualTo(0);

        // Test null value returns null
        Object nullValue = encoder.encode(block, 2);
        assertThat(nullValue).isNull();
    }
}
