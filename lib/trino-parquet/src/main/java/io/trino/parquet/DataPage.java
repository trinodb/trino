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

import java.util.OptionalLong;

public abstract sealed class DataPage
        extends Page
        permits DataPageV1, DataPageV2
{
    protected final int valueCount;
    private final OptionalLong firstRowIndex;
    private final int pageIndex;

    public DataPage(int uncompressedSize, int valueCount, OptionalLong firstRowIndex, int pageIndex)
    {
        super(uncompressedSize);
        this.valueCount = valueCount;
        this.firstRowIndex = firstRowIndex;
        this.pageIndex = pageIndex;
    }

    /**
     * @return the index of the first row index in this page or -1 if unset.
     */
    public OptionalLong getFirstRowIndex()
    {
        return firstRowIndex;
    }

    public int getValueCount()
    {
        return valueCount;
    }

    public int getPageIndex()
    {
        return pageIndex;
    }
}
