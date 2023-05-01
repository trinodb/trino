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

import io.trino.spi.type.Type;

import java.util.Random;

import static io.trino.spi.type.TinyintType.TINYINT;

public class TestByteArrayBlockEncoding
        extends BaseBlockEncodingTest<Byte>
{
    @Override
    protected Type getType()
    {
        return TINYINT;
    }

    @Override
    protected void write(BlockBuilder blockBuilder, Byte value)
    {
        TINYINT.writeByte(blockBuilder, value);
    }

    @Override
    protected Byte randomValue(Random random)
    {
        return (byte) random.nextInt(0xFF);
    }
}
