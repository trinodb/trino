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
package io.trino.parquet.crypto;

public enum ModuleType
{
    Footer((byte) 0),
    ColumnMetaData((byte) 1),
    DataPage((byte) 2),
    DictionaryPage((byte) 3),
    DataPageHeader((byte) 4),
    DictionaryPageHeader((byte) 5),
    ColumnIndex((byte) 6),
    OffsetIndex((byte) 7),
    BloomFilterHeader((byte) 8),
    BloomFilterBitset((byte) 9);

    private final byte value;

    ModuleType(byte value)
    {
        this.value = value;
    }

    public byte getValue()
    {
        return value;
    }
}
