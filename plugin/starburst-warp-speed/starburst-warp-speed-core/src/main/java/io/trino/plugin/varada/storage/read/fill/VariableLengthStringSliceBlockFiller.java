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
package io.trino.plugin.varada.storage.read.fill;

import io.airlift.slice.Slice;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.dictionary.ReadDictionary;
import io.trino.plugin.varada.juffer.ByteBufferInputStream;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

public class VariableLengthStringSliceBlockFiller
        extends SliceBlockFiller
{
    public VariableLengthStringSliceBlockFiller(
            StorageEngineConstants storageEngineConstants,
            NativeConfiguration nativeConfiguration)
    {
        super(VarcharType.createVarcharType(20), storageEngineConstants, nativeConfiguration); // using is size 20 is arbitrary since this type is used only for creating a block in RLE
    }

    @Override
    protected int copyFromJufferOneRecord(ByteBuffer recordBuff,
            ShortBuffer lenBuff,
            int currPos,
            int recLength,
            int jufferOffset,
            Slice outputSlice,
            int outputSliceOffset)
            throws IOException
    {
        int len = Short.toUnsignedInt(lenBuff.get(currPos));
        outputSlice.setBytes(outputSliceOffset, new ByteBufferInputStream(recordBuff, jufferOffset), len);
        return len;
    }

    @Override
    protected void copyFromJufferMultipleRecords(ByteBuffer recordBuff,
            ShortBuffer lenBuff,
            int recLength,
            int numRecords,
            Slice outputSlice,
            int[] offsets)
            throws IOException
    {
        for (int currPos = 0; currPos < numRecords; currPos++) {
            offsets[currPos + 1] = offsets[currPos] + Short.toUnsignedInt(lenBuff.get(currPos));
        }
        outputSlice.setBytes(offsets[0], new ByteBufferInputStream(recordBuff, offsets[0]), offsets[numRecords] - offsets[0]);
    }

    @Override
    protected void copyFromDictionary(ShortBuffer buff,
            ReadDictionary readDictionary,
            int currPos,
            Slice outputSlice,
            int[] offsets)
    {
        Slice slice = (Slice) readDictionary.get(Short.toUnsignedInt(buff.get(currPos)));
        outputSlice.setBytes(offsets[currPos], slice);
        offsets[currPos + 1] = offsets[currPos] + slice.length();
    }
}
