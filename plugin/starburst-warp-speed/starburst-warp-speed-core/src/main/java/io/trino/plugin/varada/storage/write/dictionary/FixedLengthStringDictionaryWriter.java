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
package io.trino.plugin.varada.storage.write.dictionary;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.dictionary.DataValueDictionary;
import io.trino.plugin.varada.dictionary.DictionaryToWrite;
import io.trino.plugin.varada.util.SliceUtils;

import java.nio.ByteBuffer;
import java.util.Optional;

public class FixedLengthStringDictionaryWriter
        extends DictionaryWriter
{
    @Override
    public byte[] getDictionaryWriteData(DictionaryToWrite dictionaryToWrite)
    {
        ByteBuffer byteBuffer = createBuffer(dictionaryToWrite, Optional.empty());
        for (int i = 0; i < dictionaryToWrite.getSize(); i++) {
            byteBuffer.put(SliceUtils.slice2ByteArray((Slice) dictionaryToWrite.get(i), dictionaryToWrite.getRecTypeLength()));
        }
        return getBufferArray(byteBuffer); // no copy
    }

    @Override
    public void loadDataValueDictionary(DataValueDictionary dataValueDictionary, byte[] dataToRead)
    {
        ByteBuffer byteBuffer = ByteBuffer.wrap(dataToRead);
        int size = byteBuffer.getInt();
        int recTypeLength = byteBuffer.getInt();
        int offset = byteBuffer.position();
        for (int i = 0; i < size; i++) {
            byte[] valueInBytes = new byte[SliceUtils.trimSlice(byteBuffer, recTypeLength, offset)];
            byteBuffer.get(valueInBytes);
            dataValueDictionary.loadKey(Slices.wrappedBuffer(valueInBytes), i); // creating the Slice does not allocate new memory
            offset += recTypeLength;
            byteBuffer.position(offset);
        }
    }
}
