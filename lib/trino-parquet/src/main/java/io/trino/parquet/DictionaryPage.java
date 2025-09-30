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
package io.trino.parquet;

import io.airlift.slice.Slice;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DictionaryPage
        extends Page
{
    private final Slice slice;
    private final int dictionarySize;
    private final ParquetEncoding encoding;

    public DictionaryPage(Slice slice, int dictionarySize, ParquetEncoding encoding)
    {
        this(slice, slice.length(), dictionarySize, encoding);
    }

    public DictionaryPage(Slice slice, int uncompressedSize, int dictionarySize, ParquetEncoding encoding)
    {
        super(uncompressedSize);
        this.slice = requireNonNull(slice, "slice is null");
        this.dictionarySize = dictionarySize;
        this.encoding = requireNonNull(encoding, "encoding is null");
        checkArgument(
                encoding == ParquetEncoding.PLAIN_DICTIONARY || encoding == ParquetEncoding.PLAIN,
                "Dictionary does not support encoding: %s",
                encoding);
    }

    @Override
    public Slice getSlice()
    {
        return slice;
    }

    public int getDictionarySize()
    {
        return dictionarySize;
    }

    public ParquetEncoding getEncoding()
    {
        return encoding;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("slice", slice)
                .add("dictionarySize", dictionarySize)
                .add("encoding", encoding)
                .add("uncompressedSize", uncompressedSize)
                .toString();
    }
}
