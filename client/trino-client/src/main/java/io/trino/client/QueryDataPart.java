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
package io.trino.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.DataSize;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@JsonInclude(JsonInclude.Include.NON_ABSENT)
public class QueryDataPart
{
    private final byte[] value;
    private final long rowCount;
    private final DataSize size;

    @JsonCreator
    public QueryDataPart(
            @JsonProperty("value") byte[] value,
            @JsonProperty("row_count") long rowCount,
            @JsonProperty("size") DataSize size)
    {
        this.value = requireNonNull(value, "value is null");
        this.rowCount = rowCount;
        this.size = requireNonNull(size, "size is null");
    }

    @JsonProperty("value")
    public byte[] getValue()
    {
        return value;
    }

    @JsonIgnore
    public boolean isEmpty()
    {
        return rowCount == 0;
    }

    @JsonProperty("size")
    public DataSize getSize()
    {
        return size;
    }

    @JsonProperty("row_count")
    public long getRowCount()
    {
        return rowCount;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("rowCount", rowCount)
                .add("size", size.succinct())
                .toString();
    }

    public static QueryDataPart inlineQueryDataPart(byte[] value, long rowCount)
    {
        return new QueryDataPart(value, rowCount, DataSize.ofBytes(value.length));
    }
}
