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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

public class StridedVariableWidthBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "STRIDED_VARIABLE_WIDTH";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Class<? extends Block> getBlockClass()
    {
        return StridedVariableWidthBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        StridedVariableWidthBlock stridedBlock = (StridedVariableWidthBlock) block;
        sliceOutput.writeInt(stridedBlock.serializedSizeInBytes());
        stridedBlock.writeTo(sliceOutput);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        byte[] bytes = new byte[sliceInput.readInt()];
        sliceInput.readBytes(bytes);
        return StridedVariableWidthBlock.wrap(bytes, 0);
    }
}
