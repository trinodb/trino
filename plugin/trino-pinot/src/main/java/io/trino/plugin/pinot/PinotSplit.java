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
package io.trino.plugin.pinot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class PinotSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(PinotSplit.class);

    private final SplitType splitType;
    private final Optional<String> suffix;
    private final List<String> segments;
    private final Optional<String> segmentHost;
    private final Optional<String> timePredicate;
    private final int bucket;

    @JsonCreator
    public PinotSplit(
            @JsonProperty("splitType") SplitType splitType,
            @JsonProperty("suffix") Optional<String> suffix,
            @JsonProperty("segments") List<String> segments,
            @JsonProperty("segmentHost") Optional<String> segmentHost,
            @JsonProperty("timePredicate") Optional<String> timePredicate,
            @JsonProperty("bucket") int bucket)
    {
        this.splitType = requireNonNull(splitType, "splitType id is null");
        this.suffix = requireNonNull(suffix, "suffix is null");
        this.segments = ImmutableList.copyOf(requireNonNull(segments, "segments is null"));
        this.segmentHost = requireNonNull(segmentHost, "segmentHost is null");
        this.timePredicate = requireNonNull(timePredicate, "timePredicate is null");
        this.bucket = bucket;

        // make sure the segment properties are present when the split type is segment
        if (splitType == SplitType.SEGMENT) {
            checkArgument(suffix.isPresent(), "Suffix is missing from this split");
            checkArgument(!segments.isEmpty(), "Segments are missing from the split");
            checkArgument(segmentHost.isPresent(), "Segment host address is missing from the split");
        }
    }

    @JsonProperty
    public SplitType getSplitType()
    {
        return splitType;
    }

    @JsonProperty
    public Optional<String> getSuffix()
    {
        return suffix;
    }

    @JsonProperty
    public Optional<String> getSegmentHost()
    {
        return segmentHost;
    }

    @JsonProperty
    public List<String> getSegments()
    {
        return segments;
    }

    @JsonProperty
    public Optional<String> getTimePredicate()
    {
        return timePredicate;
    }

    @JsonProperty
    public int getBucket()
    {
        return bucket;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("splitType", splitType)
                .add("suffix", suffix)
                .add("segments", segments)
                .add("segmentHost", segmentHost)
                .add("bucket", bucket)
                .toString();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + sizeOf(suffix, SizeOf::estimatedSizeOf)
                + estimatedSizeOf(segments, SizeOf::estimatedSizeOf)
                + sizeOf(segmentHost, SizeOf::estimatedSizeOf)
                + sizeOf(timePredicate, SizeOf::estimatedSizeOf)
                + sizeOf(bucket);
    }

    public enum SplitType
    {
        SEGMENT,
        BROKER,
    }
}
