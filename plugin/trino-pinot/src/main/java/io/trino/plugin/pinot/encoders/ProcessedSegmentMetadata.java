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
package io.trino.plugin.pinot.encoders;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class ProcessedSegmentMetadata
{
    private final String segmentName;
    private final boolean metadataPushEnabled;
    private final long segmentBuildTimeMillis;
    private final long segmentPushTimeMillis;
    private final long metadataPushTimeMillis;
    private final long segmentSizeBytes;
    private final long compressedSegmentSizeBytes;
    private final long segmentRows;

    @JsonCreator
    public ProcessedSegmentMetadata(
            @JsonProperty String segmentName,
            @JsonProperty boolean metadataPushEnabled,
            @JsonProperty long segmentBuildTimeMillis,
            @JsonProperty long segmentPushTimeMillis,
            @JsonProperty long metadataPushTimeMillis,
            @JsonProperty long segmentSizeBytes,
            @JsonProperty long compressedSegmentSizeBytes,
            @JsonProperty long segmentRows)
    {
        this.segmentName = requireNonNull(segmentName, "segmentName is null");
        this.metadataPushEnabled = metadataPushEnabled;
        this.segmentBuildTimeMillis = segmentBuildTimeMillis;
        this.segmentPushTimeMillis = segmentPushTimeMillis;
        this.metadataPushTimeMillis = metadataPushTimeMillis;
        this.segmentSizeBytes = segmentSizeBytes;
        this.compressedSegmentSizeBytes = compressedSegmentSizeBytes;
        this.segmentRows = segmentRows;
    }

    @JsonProperty
    public String getSegmentName()
    {
        return segmentName;
    }

    @JsonProperty
    public boolean isMetadataPushEnabled()
    {
        return metadataPushEnabled;
    }

    @JsonProperty
    public long getSegmentBuildTimeMillis()
    {
        return segmentBuildTimeMillis;
    }

    @JsonProperty
    public long getSegmentPushTimeMillis()
    {
        return segmentPushTimeMillis;
    }

    @JsonProperty
    public long getSegmentSizeBytes()
    {
        return segmentSizeBytes;
    }

    @JsonProperty
    public long getCompressedSegmentSizeBytes()
    {
        return compressedSegmentSizeBytes;
    }

    @JsonProperty
    public long getSegmentRows()
    {
        return segmentRows;
    }

    @JsonProperty
    public long getMetadataPushTimeMillis()
    {
        return metadataPushTimeMillis;
    }
}
