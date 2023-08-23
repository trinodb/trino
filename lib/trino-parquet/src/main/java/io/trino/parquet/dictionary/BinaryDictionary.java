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
package io.trino.parquet.dictionary;

import io.airlift.slice.Slice;
import io.trino.parquet.DictionaryPage;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class BinaryDictionary
        implements Dictionary
{
    private final Slice[] content;

    public BinaryDictionary(DictionaryPage dictionaryPage)
    {
        this(dictionaryPage, null);
    }

    public BinaryDictionary(DictionaryPage dictionaryPage, Integer length)
    {
        content = new Slice[dictionaryPage.getDictionarySize()];

        Slice dictionarySlice = dictionaryPage.getSlice();

        int currentInputOffset = 0;
        if (length == null) {
            for (int i = 0; i < content.length; i++) {
                int positionLength = dictionarySlice.getInt(currentInputOffset);
                currentInputOffset += Integer.BYTES;
                content[i] = dictionarySlice.slice(currentInputOffset, positionLength);
                currentInputOffset += positionLength;
            }
        }
        else {
            checkArgument(length > 0, "Invalid byte array length: %s", length);
            for (int i = 0; i < content.length; i++) {
                content[i] = dictionarySlice.slice(currentInputOffset, length);
                currentInputOffset += length;
            }
        }
    }

    @Override
    public Slice decodeToSlice(int id)
    {
        return content[id];
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("content", content)
                .toString();
    }
}
