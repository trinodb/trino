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

import io.trino.plugin.varada.dictionary.DataValueDictionary;
import io.trino.plugin.varada.dictionary.DictionaryToWrite;

import java.nio.ByteBuffer;
import java.util.Optional;

public abstract class DictionaryWriter
{
    public abstract byte[] getDictionaryWriteData(DictionaryToWrite dictionaryToWrite);

    public abstract void loadDataValueDictionary(DataValueDictionary dataValueDictionary, byte[] dataToRead);

    protected ByteBuffer createBuffer(DictionaryToWrite dictionaryToWrite, Optional<Integer> precalculatedValuesLength)
    {
        int headerSize = 2 * Integer.BYTES;
        int valuesLength = precalculatedValuesLength.orElse(dictionaryToWrite.getSize() * dictionaryToWrite.getRecTypeLength());
        ByteBuffer byteBuffer = ByteBuffer.allocate(headerSize + valuesLength);
        byteBuffer.putInt(dictionaryToWrite.getSize());
        byteBuffer.putInt(dictionaryToWrite.getRecTypeLength());
        return byteBuffer;
    }

    @SuppressWarnings("ByteBufferBackingArray")
    protected byte[] getBufferArray(ByteBuffer byteBuffer)
    {
        return byteBuffer.array();
    }
}
