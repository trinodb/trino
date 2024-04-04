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
package io.trino.spi.block;

import java.util.ArrayList;
import java.util.List;

public class TestByteArrayBlockBuilder
        extends AbstractTestBlockBuilder<Byte>
{
    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new ByteArrayBlockBuilder(null, 1);
    }

    @Override
    protected List<Byte> getTestValues()
    {
        return List.of((byte) 10, (byte) 11, (byte) 12, (byte) 13, (byte) 14);
    }

    @Override
    protected Byte getUnusedTestValue()
    {
        return (byte) -1;
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<Byte> values)
    {
        ByteArrayBlockBuilder blockBuilder = new ByteArrayBlockBuilder(null, 1);
        for (Byte value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeByte(value);
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<Byte> blockToValues(ValueBlock valueBlock)
    {
        ByteArrayBlock byteArrayBlock = (ByteArrayBlock) valueBlock;
        List<Byte> actualValues = new ArrayList<>(byteArrayBlock.getPositionCount());
        for (int i = 0; i < byteArrayBlock.getPositionCount(); i++) {
            if (byteArrayBlock.isNull(i)) {
                actualValues.add(null);
            }
            else {
                actualValues.add(byteArrayBlock.getByte(i));
            }
        }
        return actualValues;
    }
}
