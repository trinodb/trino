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
package io.trino.client.spooling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.client.QueryData;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class EncodedQueryData
        implements QueryData
{
    private final String encoding;
    private final DataAttributes metadata;
    private final List<Segment> segments;

    @JsonCreator
    public EncodedQueryData(
            @JsonProperty("encoding") String encoding,
            @JsonProperty("metadata") Map<String, Object> metadata,
            @JsonProperty("segments") List<Segment> segments)
    {
        this(encoding, new DataAttributes(metadata), segments);
    }

    public EncodedQueryData(String encoding, DataAttributes metadata, List<Segment> segments)
    {
        this.encoding = requireNonNull(encoding, "encoding is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.segments = ImmutableList.copyOf(requireNonNull(segments, "segments is null"));
    }

    @JsonProperty("segments")
    public List<Segment> getSegments()
    {
        return segments;
    }

    @JsonProperty("encoding")
    public String getEncoding()
    {
        return encoding;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty("metadata")
    public Map<String, Object> getJsonMetadata()
    {
        return metadata.attributes;
    }

    @JsonIgnore
    public DataAttributes getMetadata()
    {
        return metadata;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("encoding", encoding)
                .add("segments", segments)
                .add("metadata", metadata.attributes.keySet())
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EncodedQueryData that = (EncodedQueryData) o;
        return Objects.equals(encoding, that.encoding)
                && Objects.equals(metadata, that.metadata)
                && Objects.equals(segments, that.segments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(encoding, metadata, segments);
    }

    public static Builder builder(String encoding)
    {
        return new Builder(encoding);
    }

    @Override
    public boolean isNull()
    {
        return segments.isEmpty();
    }

    @Override
    public long getRowsCount()
    {
        return segments.stream()
                .mapToLong(Segment::getRowsCount)
                .sum();
    }

    public static class Builder
    {
        private final String encoding;
        private final ImmutableList.Builder<Segment> segments = ImmutableList.builder();
        private DataAttributes metadata = DataAttributes.empty();

        private Builder(String encoding)
        {
            this.encoding = requireNonNull(encoding, "encoding is null");
        }

        public Builder withSegment(Segment segment)
        {
            this.segments.add(segment);
            return this;
        }

        public Builder withSegments(List<Segment> segments)
        {
            this.segments.addAll(segments);
            return this;
        }

        public Builder withAttributes(DataAttributes attributes)
        {
            this.metadata = requireNonNull(attributes, "attributes is null");
            return this;
        }

        public EncodedQueryData build()
        {
            return new EncodedQueryData(encoding, metadata, segments.build());
        }
    }
}
