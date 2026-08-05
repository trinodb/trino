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
package io.trino.plugin.deltalake.transactionlog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.SizeOf;

import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.canonicalizePartitionValues;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CdcEntry
{
    private static final int INSTANCE_SIZE = instanceSize(CdcEntry.class);

    private final String path;
    private final Map<String, String> partitionValues;
    private final Map<String, Optional<String>> canonicalPartitionValues;
    private final long size;

    @JsonCreator
    public CdcEntry(
            @JsonProperty("path") String path,
            @JsonProperty("partitionValues") Map<String, String> partitionValues,
            @JsonProperty("size") long size)
    {
        this.path = requireNonNull(path, "path is null");
        // Avoid ImmutableMap.copyOf because the value may have nulls
        this.partitionValues = requireNonNull(partitionValues, "partitionValues is null");
        this.canonicalPartitionValues = canonicalizePartitionValues(partitionValues);
        this.size = size;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public Map<String, String> getPartitionValues()
    {
        return partitionValues;
    }

    @JsonIgnore // derived from partitionValues
    public Map<String, Optional<String>> getCanonicalPartitionValues()
    {
        return canonicalPartitionValues;
    }

    @JsonProperty
    public long getSize()
    {
        return size;
    }

    @JsonProperty("dataChange")
    public boolean isDataChange()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return format("CdcEntry{path=%s, partitionValues=%s, size=%d, dataChange=false}", path, partitionValues, size);
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + estimatedSizeOf(partitionValues, SizeOf::estimatedSizeOf, SizeOf::estimatedSizeOf)
                + estimatedSizeOf(canonicalPartitionValues, SizeOf::estimatedSizeOf, value -> sizeOf(value, SizeOf::estimatedSizeOf))
                + SIZE_OF_LONG;
    }
}
