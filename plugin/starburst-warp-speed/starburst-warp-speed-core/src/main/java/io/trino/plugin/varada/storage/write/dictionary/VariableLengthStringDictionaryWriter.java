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

import java.nio.ByteBuffer;
import java.util.Optional;

public class VariableLengthStringDictionaryWriter
            extends DictionaryWriter
{
    @Override
    public byte[] getDictionaryWriteData(DictionaryToWrite dictionaryToWrite)
    {
        int totalLength = dictionaryToWrite.getWeight() + dictionaryToWrite.getSize() * Integer.BYTES;
        ByteBuffer byteBuffer = createBuffer(dictionaryToWrite, Optional.of(totalLength));
        for (int i = 0; i < dictionaryToWrite.getSize(); i++) {
            byte[] sliceInBytes = ((Slice) dictionaryToWrite.get(i)).getBytes();
            byteBuffer.putInt(sliceInBytes.length);
            byteBuffer.put(sliceInBytes);
        }
        return getBufferArray(byteBuffer);
    }

    @Override
    public void loadDataValueDictionary(DataValueDictionary dataValueDictionary, byte[] dataToRead)
    {
        ByteBuffer byteBuffer = ByteBuffer.wrap(dataToRead);
        int size = byteBuffer.getInt();
        byteBuffer.getInt(); // skip recTypeLength
        for (int i = 0; i < size; i++) {
            byte[] valueInBytes = new byte[byteBuffer.getInt()];
            byteBuffer.get(valueInBytes);
            dataValueDictionary.loadKey(Slices.wrappedBuffer(valueInBytes), i); // creating the Slice does not allocate new memory
        }
    }
}
